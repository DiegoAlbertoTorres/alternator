package alternator

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"log"
	"net/rpc"
	"os"
	"sync"
	"time"

	"github.com/boltdb/bolt"
)

var dataBucket = []byte("data")
var metaDataBucket = []byte("metadata")
var pendingDataBucket = []byte("pendingdata")
var pendingMDBucket = []byte("pendingmetadata")

// Metadata represents the metadata of a (key, value) pair
type Metadata struct {
	Name       string
	Replicants []Key
}

/* Database struct and methods */

// DB is a wrapper around bolt's DB struct, so that methods can be added
type dB struct {
	*bolt.DB
}

// getMDRange returns all key,value pairs in a given key range
func (db *dB) getMDRange(min, max Key) ([]Key, [][]byte) {
	var keys []Key
	var vals [][]byte

	err := db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(metaDataBucket)
		c := b.Cursor()

		// Start seeking at min
		c.Seek(min[:])
		// Stop at max
		for kslice, v := c.First(); kslice != nil && bytes.Compare(kslice, max[:]) < 0; kslice, v = c.Next() {
			// TODO: Set a proper capacity for keys and vals?
			keys = append(keys, SliceToKey(kslice))
			vals = append(vals, v)
		}
		return nil
	})
	checkErr("Get range error error: ", err)

	return keys, vals
}

// close closes the db
func (db *dB) close() {
	db.Close()
}

// Initialize a node's database in the filesystem, create buckets
func (altNode *Node) initDB() {
	checkBucketErr := func(err error) {
		if (err != nil) && (err != bolt.ErrBucketExists) {
			log.Fatal(err)
		}
	}

	err := os.MkdirAll(altNode.Config.DotPath, 0777)
	checkFatal("failed to make db path", err)
	boltdb, err := bolt.Open(altNode.Config.DotPath+altNode.Port+".db", 0600, nil)
	altNode.DB = &dB{boltdb}
	checkFatal("failed to open .db file", err)

	// Create a bucket for all entries
	altNode.DB.Batch(func(tx *bolt.Tx) error {
		// Create buckets
		_, err = tx.CreateBucketIfNotExists(metaDataBucket)
		checkBucketErr(err)
		_, err := tx.CreateBucket(dataBucket)
		checkBucketErr(err)
		_, err = tx.CreateBucket(pendingMDBucket)
		checkBucketErr(err)
		_, err = tx.CreateBucket(pendingDataBucket)
		checkBucketErr(err)
		return nil
	})

	gob.Register(PutMDArgs{})
	gob.Register(PutArgs{})

}

/* kv-store methods for PUTTING (inserting) into the database */

const (
	pendingMD = iota
	pendingData
)

// PendingPut is used to store in database information on some entry that is still pending in the
// kv store
type PendingPut struct {
	Type  int
	Peers []Key
	Args  interface{}
}

// putPending puts the pending put in all of its pending peers
func (altNode *Node) putPending(k Key, pending PendingPut) {
	successIndeces := make(map[int]struct{})

	// TODO: make remote calls here async
	for i, ownerID := range pending.Peers {

		altNode.membersMutex.RLock()
		owner := getPeer(altNode.Members.Map[ownerID])
		altNode.membersMutex.RUnlock()

		if owner == nil {
			// Remove from Peers, node no longer in members list
			successIndeces[i] = struct{}{}
			continue
		}
		if pending.Type == pendingMD {
			putMDArgs := pending.Args.(PutMDArgs)
			err := altNode.rpcServ.MakeRemoteCall(owner, "PutMetadata", &putMDArgs, &struct{}{})
			k := StringToKey(putMDArgs.Name)
			if err == nil {
				fmt.Printf("Good: md resolve, %v:%v\n", ownerID, k)
				successIndeces[i] = struct{}{}
			} else {
				fmt.Printf("Bad: md resolve, %v:%v\n", ownerID, k)
			}

		} else if pending.Type == pendingData {
			putArgs := pending.Args.(PutArgs)
			err := altNode.rpcServ.MakeRemoteCall(owner, "PutData", &putArgs, &struct{}{})
			k := StringToKey(putArgs.Name)
			if err == nil {
				fmt.Printf("Good: data resolve, %v:%v\n", ownerID, k)
				successIndeces[i] = struct{}{}
			} else {
				fmt.Printf("Bad: data resolve %v:%v\n", ownerID, k)
			}
		}
	}
	// Remove successfully resolved entries from the pending Peers list
	var newPending []Key
	for i := range pending.Peers {
		if _, ok := successIndeces[i]; !ok {
			newPending = append(newPending, pending.Peers[i])
		}
	}
	pending.Peers = newPending

	altNode.DB.Batch(func(tx *bolt.Tx) error {
		var b *bolt.Bucket
		if pending.Type == pendingMD {
			b = tx.Bucket(pendingMDBucket)
		} else if pending.Type == pendingData {
			b = tx.Bucket(pendingDataBucket)
		} else {
			return nil
		}
		if len(pending.Peers) == 0 {
			b.Delete(k[:])
		} else {
			b.Put(k[:], serialize(pending))
		}
		return nil
	})
}

func (altNode *Node) resolvePending() {
	altNode.DB.Batch(func(tx *bolt.Tx) error {
		pendMDBucket := tx.Bucket(pendingMDBucket)
		c := pendMDBucket.Cursor()
		for kslice, v := c.First(); kslice != nil; kslice, v = c.Next() {
			k := SliceToKey(kslice)
			PendingPut := bytesToPendingPut(v)
			// Delete it while working on it
			pendMDBucket.Delete(kslice)
			go altNode.putPending(k, PendingPut)
		}

		pendDataBucket := tx.Bucket(pendingDataBucket)
		c = pendDataBucket.Cursor()
		for kslice, v := c.First(); kslice != nil; kslice, v = c.Next() {
			k := SliceToKey(kslice)
			PendingPut := bytesToPendingPut(v)
			// Delete it while working on it
			pendDataBucket.Delete(kslice)
			go altNode.putPending(k, PendingPut)
		}
		return nil
	})
	return
}

func (altNode *Node) autoResolvePending() {
	for {
		time.Sleep(time.Duration(altNode.Config.ResolvePendingTime) * time.Millisecond)
		altNode.resolvePending()
	}
}

// PutArgs is a struct to represent the arguments of Put or DBPut
type PutArgs struct {
	Name       string
	V          []byte
	Replicants []Key
	Success    int
}

// Put a (key, value) pair in the system
func (altNode *Node) Put(putArgs *PutArgs, _ *struct{}) error {
	k := StringToKey(putArgs.Name)
	var i int

	// Attempt to pass the call to member of replication chain
	chainLink := altNode.findListSuccessor(k)
	for i = 0; i < altNode.Config.N; i++ {
		// Wrap around if reach rings end
		if chainLink == nil {
			chainLink = altNode.Members.List.Front()
		}
		chainPeer := getPeer(chainLink)

		// If this node is the chainLink
		if chainPeer.ID == altNode.ID {
			// Handle here
			break
		}
		// Else try to pass the Put down the chain
		fmt.Printf("Forwarding to %v\n", chainPeer.ID)
		err := altNode.rpcServ.MakeRemoteCall(chainPeer, "Put", putArgs, &struct{}{})
		if err == nil {
			// Done, chain took responsibility
			return nil
		}
		chainLink = chainLink.Next()
	}

	var wg sync.WaitGroup
	putMDArgs := PutMDArgs{Name: putArgs.Name,
		MD: Metadata{Name: putArgs.Name, Replicants: putArgs.Replicants}}
	mdPeers := make([]*Peer, 0, altNode.Config.N)
	mdPendingIDs := make([]Key, 0, altNode.Config.N)

	dataPeers := make([]*Peer, 0, altNode.Config.N)
	dataPendingIDs := make([]Key, 0, altNode.Config.N)

	wg.Add(2)
	go altNode.chainPutMetadata(&wg, &putMDArgs, &mdPeers, &mdPendingIDs)
	go altNode.putDataInReps(&wg, putArgs, &dataPeers, &dataPendingIDs)
	wg.Wait()

	// Cancel puts if less than half the chain has metadata
	// if len(mdPeers) < ((altNode.Config.N - 1) / 2) {
	// 	altNode.undoPutMD(k, mdPeers)
	// 	altNode.undoPutData(k, dataPeers)
	// 	return ErrPutMDFail
	// }

	// Undo if put does not meet success criteria
	// if ((putArgs.Success == 0) && (len(dataPeers) != len(putArgs.Replicants))) ||
	// 	(len(dataPeers) < putArgs.Success) {
	//
	// 	fmt.Printf("Success is %v, len(dataPeers) is %v, len(reps) is %v, len(fails) is %v\n", putArgs.Success, len(dataPeers), len(putArgs.Replicants), len(dataPendingIDs))
	//
	// 	altNode.undoPutMD(k, mdPeers)
	// 	altNode.undoPutData(k, dataPeers)
	// 	return ErrPutFail
	// }

	go func() {
		pendingMDPut := PendingPut{Type: pendingMD, Peers: mdPendingIDs, Args: putMDArgs}
		pendingDataPut := PendingPut{Type: pendingData, Peers: dataPendingIDs, Args: putArgs}

		altNode.DB.Batch(func(tx *bolt.Tx) error {
			if len(pendingMDPut.Peers) > 0 {
				pendMDBucket := tx.Bucket(pendingMDBucket)
				pendMDBucket.Put(k[:], serialize(pendingMDPut))
			}
			if len(pendingDataPut.Peers) > 0 {
				pendDataBucket := tx.Bucket(pendingDataBucket)
				pendDataBucket.Put(k[:], serialize(pendingDataPut))
			}
			return nil
		})
	}()

	return nil
}

func (altNode *Node) putDataInReps(wg *sync.WaitGroup, args *PutArgs, successPeers *[]*Peer,
	failPeerIDs *[]Key) {

	successCh := make(chan *Peer, len(args.Replicants))
	failCh := make(chan Key, altNode.Config.N)

	for _, repID := range args.Replicants {
		altNode.membersMutex.RLock()
		currentLNode, ok := altNode.Members.Map[repID]
		altNode.membersMutex.RUnlock()
		current := getPeer(currentLNode)

		if !ok {
			log.Print("Members map wrong error")
			continue
		}

		call := altNode.rpcServ.MakeAsyncCall(current, "PutData", args, &struct{}{})
		go func(call *rpc.Call, current *Peer) {
			select {
			case reply := <-call.Done:
				if !checkErr("failed to put data in replicant", reply.Error) {
					successCh <- current
				} else {
					altNode.rpcServ.CloseIfBad(reply.Error, current)
					fmt.Println("Error returned!")
					failCh <- current.ID
				}
			case <-time.After(time.Duration(altNode.Config.PutDataTimeout) * time.Millisecond):
				fmt.Println("Timeout!")
				failCh <- current.ID
			}
		}(call, current)
	}

	for i := 0; i < len(args.Replicants); i++ {
		select {
		case success := <-successCh:
			*successPeers = append(*successPeers, success)
		case fail := <-failCh:
			*failPeerIDs = append(*failPeerIDs, fail)
		}
	}

	wg.Done()
}

func (altNode *Node) chainPutMetadata(wg *sync.WaitGroup, putMDArgs *PutMDArgs,
	successPeers *[]*Peer, failPeerIDs *[]Key) {

	k := StringToKey(putMDArgs.Name)
	successCh := make(chan *Peer, altNode.Config.N)
	failCh := make(chan Key, altNode.Config.N)

	i := 0
	altNode.membersMutex.RLock()
	for current := altNode.Members.FindSuccessor(k); i < altNode.Config.N; current = current.Next() {
		if current == nil {
			current = altNode.Members.List.Front()
		}
		currentPeer := getPeer(current)
		call := altNode.rpcServ.MakeAsyncCall(currentPeer, "PutMetadata", &putMDArgs, &struct{}{})
		go func(call *rpc.Call, current *Peer) {
			select {
			case reply := <-call.Done:
				if !checkErr("metadata put fail", reply.Error) {
					// successPeers = append(successPeers, currentPeer)
					successCh <- currentPeer
				} else {
					// failedPeerIDs = append(failedPeerIDs, currentPeer.ID)
					failCh <- currentPeer.ID
					altNode.rpcServ.CloseIfBad(reply.Error, current)
				}
			case <-time.After(time.Duration(altNode.Config.PutMDTimeout) * time.Millisecond):
				failCh <- currentPeer.ID
			}
		}(call, currentPeer)
		i++
	}
	altNode.membersMutex.RUnlock()

	for i := 0; i < altNode.Config.N; i++ {
		select {
		case success := <-successCh:
			*successPeers = append(*successPeers, success)
		case fail := <-failCh:
			*failPeerIDs = append(*failPeerIDs, fail)
		}
	}

	wg.Done()
	return
}

// PutData puts the (hash(name), val) pair in DB
func (altNode *Node) PutData(args *PutArgs, _ *struct{}) error {
	k := StringToKey(args.Name)
	err := altNode.DB.Batch(func(tx *bolt.Tx) error {
		b := tx.Bucket(dataBucket)
		err := b.Put(k[:], args.V)
		return err
	})
	if err == nil {
		// log.Print("Stored pair " + args.Name + "," + string(args.V) + " key is " + k.String())
		fmt.Println("Pair: " + k.String())
	}
	return err
}

// PutMDArgs represents the arguments of a call to PutMetadata
type PutMDArgs struct {
	Name string
	MD   Metadata
}

// PutMetadata puts the (key, val) pair in DB
func (altNode *Node) PutMetadata(args *PutMDArgs, _ *struct{}) error {
	k := StringToKey(args.Name)
	// Serialize replicants
	md := serialize(args.MD)
	err := altNode.DB.Batch(func(tx *bolt.Tx) error {
		b := tx.Bucket(metaDataBucket)
		err := b.Put(k[:], md)
		return err
	})
	if err == nil {
		// log.Print("Stored metadata for " + args.Name + ", key is " + k.String())
		fmt.Println("Meta: " + k.String())
	}
	return err
}

// RePutArgs are arguments for a call to RePut
type RePutArgs struct {
	LeaverID Key
	K        Key
	V        []byte
}

// RePut redoes a Put
func (altNode *Node) RePut(args *RePutArgs, _ *struct{}) error {
	// var successor Peer
	successorLNode := altNode.getListSuccessor()
	successor := getPeer(successorLNode)
	if successor.ID == args.LeaverID {
		successor = getPeer(successorLNode.Next())
		if successor.ID == args.LeaverID {
			log.Print("Cannot do RePut, successor leaving")
			return ErrRePutFail
		}
	}
	// Get metadata from chain
	md, err := altNode.chainGetMetadata(args.K)
	if checkErr("RePut failed", err) {
		return err
	}

	// Find leaver
	var i int
	for i = range md.Replicants {
		if md.Replicants[i] == args.LeaverID {
			var randomID Key
			// Get some random, different ID
			for {
				altNode.membersMutex.RLock()
				randomID = altNode.Members.GetRandom().ID
				altNode.membersMutex.RUnlock()
				if randomID != md.Replicants[i] {
					break
				}
			}
			// Replace
			md.Replicants[i] = randomID
			break
		}
	}
	// Do the put again
	putArgs := PutArgs{Name: md.Name, V: args.V, Replicants: md.Replicants, Success: 0}
	err = altNode.rpcServ.MakeRemoteCall(successor, "Put", putArgs, &struct{}{})
	checkErr("Put failed", err)

	return err
}

// undoPutData undoes all data puts for a key in a set of nodes
func (altNode *Node) undoPutData(k Key, nodes []*Peer) error {
	for _, node := range nodes {
		altNode.rpcServ.MakeRemoteCall(node, "DropKeyData", k, &struct{}{})
	}
	// Wrong
	return nil
}

// undoPutMD undoes all metadata puts for a key in a set of nodes
func (altNode *Node) undoPutMD(k Key, nodes []*Peer) error {
	for _, node := range nodes {
		altNode.rpcServ.MakeRemoteCall(node, "DropKeyMD", k, &struct{}{})
	}
	// Wrong
	return nil
}

func (altNode *Node) rePutAllData() {
	altNode.DB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(dataBucket)
		c := b.Cursor()

		var wg sync.WaitGroup
		for kslice, v := c.First(); kslice != nil; kslice, v = c.Next() {
			var successor Peer
			k := SliceToKey(kslice)
			altNode.FindSuccessor(k, &successor)
			rpArgs := RePutArgs{LeaverID: altNode.ID, K: k, V: v}
			call := altNode.rpcServ.MakeAsyncCall(&successor, "RePut", rpArgs, &struct{}{})
			go func(call *rpc.Call, peer *Peer) {
				reply := <-call.Done
				checkErr("Reput failed", reply.Error)
				altNode.rpcServ.CloseIfBad(reply.Error, peer)
				wg.Done()
			}(call, &successor)
		}
		wg.Wait()
		return nil
	})
}

/* kv-store methods for GETTING from the database */

// GetMetadata returns the metadata (serialized) of a specific variable
func (altNode *Node) GetMetadata(k Key, md *[]byte) error {
	// fmt.Println("About to get metadata")
	return altNode.DB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(metaDataBucket)
		*md = b.Get(k[:])
		// fmt.Println("Getting meta", k)
		if md == nil {
			return ErrKeyNotFound
		}
		return nil
	})
}

// GetData returns the metadata (serialized) of a specific variable
func (altNode *Node) GetData(k Key, data *[]byte) error {
	// fmt.Println("About to get data")
	return altNode.DB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(dataBucket)
		*data = b.Get(k[:])
		// fmt.Println("Getting data", k)
		if data == nil {
			return ErrKeyNotFound
		}
		// fmt.Printf("The answer is: %s\n", v)
		return nil
	})
}

// Get gets from the system the value corresponding to the key
func (altNode *Node) Get(name string, ret *[]byte) error {
	k := StringToKey(name)

	// Get replicants from metadata chain
	md, err := altNode.chainGetMetadata(k)
	if checkErr("Get failed", err) {
		return err
	}

	// Get data from some replicant
	for _, repID := range md.Replicants {
		altNode.membersMutex.RLock()
		rep := getPeer(altNode.Members.Map[repID])
		altNode.membersMutex.RUnlock()
		// In case rep left
		// TODO: this occurs because when replicants leave, nobody re-replicates their data
		if rep == nil {
			continue
		}
		var result []byte
		err := altNode.rpcServ.MakeRemoteCall(rep, "GetData", k, &result)
		if err == nil {
			*ret = result
			return nil
		}
	}
	// None of the replicants had the data?
	return ErrDataLost
}

// TODO: write chainGetData

func (altNode *Node) chainGetMetadata(k Key) (Metadata, error) {
	// altNode.FindSuccessor(k, &successor)
	i := 0
	current := altNode.Members.FindSuccessor(k)
	for ; i < altNode.Config.N; current = current.Next() {
		var rawMD []byte
		if current == nil {
			current = altNode.Members.List.Front()
		}
		currentPeer := getPeer(current)
		call := altNode.rpcServ.MakeAsyncCall(currentPeer, "GetMetadata", k, &rawMD)
		select {
		case reply := <-call.Done:
			if reply.Error == nil {
				return bytesToMetadata(rawMD), nil
			}
			altNode.rpcServ.CloseIfBad(reply.Error, currentPeer)
		case <-time.After(500 * time.Millisecond):
		}
		i++
	}
	return Metadata{}, ErrKeyNotFound
}

func (db *dB) batchPut(bucket []byte, keys *[]Key, vals *[][]byte) error {
	anyError := false
	err := db.Batch(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucket)
		for i := range *keys {
			err := b.Put((*keys)[i][:], (*vals)[i])
			if err != nil {
				anyError = true
			}
		}
		if anyError {
			return ErrBatchPutIncomplete
		}
		return nil
	})
	return err
}

// BatchPutArgs are the arguments for a batch put
type BatchPutArgs struct {
	Bucket []byte
	Keys   []Key
	Vals   [][]byte
}

// BatchPut puts a set of key-value pairs in the specified bucket
func (altNode *Node) BatchPut(args BatchPutArgs, _ *struct{}) error {
	err := altNode.DB.Batch(func(tx *bolt.Tx) error {
		b := tx.Bucket(args.Bucket)
		anyError := false
		for i := range args.Keys {
			// TODO: handle error
			err := b.Put(args.Keys[i][:], args.Vals[i])
			if err != nil {
				anyError = true
			}
			log.Printf("Stored metadata for%s\n", args.Keys[i])
		}
		if anyError {
			return ErrBatchPutIncomplete
		}
		return nil
	})
	return err
}

// DropKeyMD Drops all metadata associated with a key
func (altNode *Node) DropKeyMD(k *Key, _ *struct{}) error {
	err := altNode.DB.Batch(func(tx *bolt.Tx) error {
		b := tx.Bucket(metaDataBucket)
		err := b.Delete(k[:])
		return err
	})
	return err
}

// DropKeyData drops all data associated with a key
func (altNode *Node) DropKeyData(k *Key, _ *struct{}) error {
	err := altNode.DB.Batch(func(tx *bolt.Tx) error {
		b := tx.Bucket(dataBucket)
		err := b.Delete(k[:])
		return err
	})
	return err
}

// DumpMetadata dumps all keys in the metadata store to stdout
func (altNode *Node) DumpMetadata(_ struct{}, _ *struct{}) error {
	i := 0
	fmt.Println("***Metadata dump***")
	err := altNode.DB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(metaDataBucket)
		c := b.Cursor()

		// Start seeking at min
		c.Seek(minSlice)
		// Stop at max
		for kslice, _ := c.First(); kslice != nil; kslice, _ = c.Next() {
			fmt.Printf("Meta: %v\n", SliceToKey(kslice))
			i++
		}
		return nil
	})
	fmt.Printf("TOTAL: %d\n", i)
	fmt.Println("***End of metadata dump***")
	return err
}

// DumpData dumps all keys in the metadata store to stdout
func (altNode *Node) DumpData(_ struct{}, _ *struct{}) error {
	fmt.Println("***Data dump***")
	i := 0
	err := altNode.DB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(dataBucket)
		c := b.Cursor()

		// Start seeking at min
		c.Seek(minSlice)
		// Stop at max
		for kslice, _ := c.First(); kslice != nil; kslice, _ = c.Next() {
			fmt.Printf("Data: %v\n", SliceToKey(kslice))
			i++
		}
		return nil
	})
	fmt.Printf("TOTAL: %d\n", i)
	fmt.Println("***End of data dump***")
	return err
}
