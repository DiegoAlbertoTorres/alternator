package alternator

import (
	"bytes"
	"encoding/gob"
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
	err := os.MkdirAll(altNode.Config.DotPath, 0777)
	checkFatal("failed to make db path", err)
	boltdb, err := bolt.Open(altNode.Config.DotPath+altNode.Port+".db", 0600, nil)
	altNode.DB = &dB{boltdb}
	checkFatal("failed to open .db file", err)

	checkBucketErr := func(err error) {
		if (err != nil) && (err != bolt.ErrBucketExists) {
			log.Fatal(err)
		}
	}
	// Create a bucket for all entries
	altNode.DB.Update(func(tx *bolt.Tx) error {
		// Create buckets
		_, err = tx.CreateBucket(metaDataBucket)
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
	var successIndeces []int

	// TODO: make remote calls here async
	for i, ownerID := range pending.Peers {

		altNode.membersMutex.RLock()
		owner := getPeer(altNode.Members.Map[ownerID])
		altNode.membersMutex.RUnlock()

		if owner == nil {
			// Remove from Peers, node left
			successIndeces = append(successIndeces, i)
			continue
		}
		if pending.Type == pendingMD {
			putMDArgs := pending.Args.(PutMDArgs)
			err := altNode.rpcServ.MakeRemoteCall(owner, "PutMetadata", &putMDArgs, &struct{}{})
			if err == nil {
				successIndeces = append(successIndeces, i)
			}

		} else if pending.Type == pendingData {
			putArgs := pending.Args.(PutArgs)
			err := altNode.rpcServ.MakeRemoteCall(owner, "PutData", &putArgs, &struct{}{})
			if err == nil {
				successIndeces = append(successIndeces, i)
			}
		}
	}
	// Remove successfully resolved entries from the pending Peers list
	for _, idx := range successIndeces {
		pending.Peers = append(pending.Peers[:idx], pending.Peers[idx+1:]...)
	}

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
	altNode.DB.View(func(tx *bolt.Tx) error {
		pendMDBucket := tx.Bucket(pendingMDBucket)
		c := pendMDBucket.Cursor()
		for kslice, v := c.First(); kslice != nil; kslice, v = c.Next() {
			k := SliceToKey(kslice)
			PendingPut := bytesToPendingPut(v)
			go altNode.putPending(k, PendingPut)
		}

		pendDataBucket := tx.Bucket(pendingDataBucket)
		c = pendDataBucket.Cursor()
		for kslice, v := c.First(); kslice != nil; kslice, v = c.Next() {
			k := SliceToKey(kslice)
			PendingPut := bytesToPendingPut(v)
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
	var successor Peer
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
		err := altNode.rpcServ.MakeRemoteCall(&successor, "Put", putArgs, &struct{}{})
		if err == nil {
			// Done, chain took responsibility
			return nil
		}
		chainLink = chainLink.Next()
	}

	// // Handle the put here
	// mdWg, mdPeers, mdPendingIDs := altNode.chainPutMetadata(&putMDArgs)
	//
	// // Put data in replicants
	// dataWg, dataPeers, dataPendingIDs := altNode.putDataInReps(putArgs)
	// // Handle the put here
	// putMDArgs := PutMDArgs{Name: putArgs.Name,
	// 	MD: Metadata{Name: putArgs.Name, Replicants: putArgs.Replicants}}

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
	if len(mdPeers) < ((altNode.Config.N - 1) / 2) {
		altNode.undoPutMD(k, mdPeers)
		altNode.undoPutData(k, dataPeers)
		return ErrPutMDFail
	}

	// Undo if put does not meet success criteria
	if ((putArgs.Success == 0) && (len(dataPeers) != len(putArgs.Replicants))) ||
		(len(dataPeers) < putArgs.Success) {

		altNode.undoPutMD(k, mdPeers)
		altNode.undoPutData(k, dataPeers)
		return ErrPutFail
	}

	pendingMDPut := PendingPut{Type: pendingMD, Peers: mdPendingIDs, Args: putMDArgs}
	pendingDataPut := PendingPut{Type: pendingData, Peers: dataPendingIDs, Args: putArgs}

	altNode.DB.Batch(func(tx *bolt.Tx) error {
		pendMDBucket := tx.Bucket(pendingMDBucket)
		pendDataBucket := tx.Bucket(pendingDataBucket)

		pendMDBucket.Put(k[:], serialize(pendingMDPut))
		pendDataBucket.Put(k[:], serialize(pendingDataPut))
		return nil
	})

	return nil
}

func (altNode *Node) putDataInReps(wg *sync.WaitGroup, args *PutArgs, successPeers *[]*Peer,
	failPeerIDs *[]Key) {

	successCh := make(chan *Peer, altNode.Config.N)
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
					// successPeers = append(successPeers, rep)
					successCh <- current
				} else {
					// failedPeerIDs = append(failedPeerIDs, rep.ID)
					failCh <- current.ID
				}
			case <-time.After(time.Duration(altNode.Config.PutDataTimeout) * time.Millisecond):
				failCh <- current.ID
			}
		}(call, current)
	}

	for i := 0; i < altNode.Config.N; i++ {
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
	var mdWg sync.WaitGroup
	altNode.membersMutex.RLock()
	for current := altNode.Members.FindSuccessor(k); i < altNode.Config.N; current = current.Next() {
		if current == nil {
			current = altNode.Members.List.Front()
		}
		currentPeer := getPeer(current)
		call := altNode.rpcServ.MakeAsyncCall(currentPeer, "PutMetadata", &putMDArgs, &struct{}{})
		mdWg.Add(1)
		go func(call *rpc.Call, current *Peer) {
			select {
			case reply := <-call.Done:
				if !checkErr("metadata put fail", reply.Error) {
					// successPeers = append(successPeers, currentPeer)
					successCh <- currentPeer
				} else {
					// failedPeerIDs = append(failedPeerIDs, currentPeer.ID)
					failCh <- currentPeer.ID
				}
				mdWg.Done()
			case <-time.After(time.Duration(altNode.Config.PutMDTimeout) * time.Millisecond):
				failCh <- currentPeer.ID
				mdWg.Done()
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
	// if err == nil {
	// 	log.Print("Stored pair " + args.Name + "," + string(args.V) + " key is " + k.String())
	// }
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
		log.Print("Stored metadata for " + args.Name + ", key is " + k.String())
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
	md := altNode.chainGetMetadata(args.K)

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
	err := altNode.rpcServ.MakeRemoteCall(successor, "Put", putArgs, &struct{}{})
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
			go func(call *rpc.Call) {
				reply := <-call.Done
				checkErr("Reput failed", reply.Error)
				wg.Done()
			}(call)
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
	md := altNode.chainGetMetadata(k)

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

func (altNode *Node) chainGetMetadata(k Key) Metadata {
	// altNode.FindSuccessor(k, &successor)
	i := 0
	current := altNode.Members.FindSuccessor(k)
	for ; i < altNode.Config.N; current = current.Next() {
		var rawMD []byte
		if current == nil {
			current = altNode.Members.List.Front()
		}
		call := altNode.rpcServ.MakeAsyncCall(getPeer(current), "GetMetadata", k, &rawMD)
		select {
		case reply := <-call.Done:
			if reply.Error == nil {
				return bytesToMetadata(rawMD)
			}
		case <-time.After(500 * time.Millisecond):
		}
		i++
	}
	return Metadata{}
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
