package alternator

import (
	"bytes"
	"log"
	"net/rpc"
	"os"
	"sync"
	"time"

	"github.com/boltdb/bolt"
)

var dataBucket = []byte("data")
var metaDataBucket = []byte("metadata")

// Metadata represents the metadata of a (key, value) pair
type Metadata struct {
	Name       string
	Replicants []Key
}

/* Database struct and methods */

// DB is a wrapper around bolt's DB struct, so that methods can be added
type DB struct {
	*bolt.DB
}

// getMDRange returns all key,value pairs in a given key range
func (db *DB) getMDRange(min, max Key) ([]Key, [][]byte) {
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
func (db *DB) close() {
	db.Close()
}

// Initialize a node's database in the filesystem, create buckets
func (altNode *Node) initDB() {
	err := os.MkdirAll(altNode.Config.DotPath, 0777)
	checkFatal("failed to make db path", err)
	boltdb, err := bolt.Open(altNode.Config.DotPath+altNode.Port+".db", 0600, nil)
	altNode.DB = &DB{boltdb}
	if !checkFatal("failed to open .db file", err) {
		// Create a bucket for all entries
		altNode.DB.Update(func(tx *bolt.Tx) error {
			// Create buckets
			_, err := tx.CreateBucket(dataBucket)
			if err != nil {
				if err != bolt.ErrBucketExists {
					log.Fatal(err)
				}
			}
			_, err = tx.CreateBucket(metaDataBucket)
			if err != nil {
				if err != bolt.ErrBucketExists {
					log.Fatal(err)
				}
			}
			return nil
		})
	}
}

/* kv-store methods for PUTTING (inserting) into the database */

// PutArgs is a struct to represent the arguments of Put or DBPut
type PutArgs struct {
	Name       string
	V          []byte
	Replicants []Key
	Success    int
}

// Put a (key, value) pair in the system
func (altNode *Node) Put(args *PutArgs, _ *struct{}) error {
	k := StringToKey(args.Name)
	var successor Peer
	err := altNode.FindSuccessor(k, &successor)

	// Pass the call to successor
	if (err == nil) && (successor.ID != altNode.ID) {
		err := MakeRemoteCall(&successor, "Put", args, &struct{}{})
		return err
	}
	// Else resolve in this node
	putMDArgs := PutMetaArgs{Name: args.Name, MD: Metadata{Name: args.Name, Replicants: args.Replicants}}

	// Store in chain
	i := 0
	mdSuccess := 0
	mdReplicants := make([]*Peer, 0, altNode.Config.N)
	var mdWg sync.WaitGroup
	for current := altNode.Members.Map[altNode.ID]; i < altNode.Config.N; current = current.Next() {
		if current == nil {
			current = altNode.Members.List.Front()
		}
		call := MakeAsyncCall(getPeer(current), "PutMetadata", putMDArgs, &struct{}{})
		mdWg.Add(1)
		go func(call *rpc.Call) {
			// defer mdWg.Done()
			select {
			case reply := <-call.Done:
				if !checkErr("metadata put fail", reply.Error) {
					mdSuccess++
				}
				mdWg.Done()
			case <-time.After(time.Duration(altNode.Config.PutMDTimeout) * time.Millisecond):
				mdWg.Done()
			}
		}(call)
		i++
	}

	// Put data in replicants
	var dataWg sync.WaitGroup
	dataReplicants := make([]*Peer, 0, len(args.Replicants))
	dataSuccess := 0
	for _, repID := range args.Replicants {
		repLNode, ok := altNode.Members.Map[repID]
		if !ok {
			log.Print("Members map wrong error")
			continue
		}
		rep := getPeer(repLNode)
		call := MakeAsyncCall(rep, "PutData", args, &struct{}{})
		dataWg.Add(1)
		go func(call *rpc.Call) {
			select {
			case reply := <-call.Done:
				if !checkErr("failed to put data in replicant", reply.Error) {
					dataReplicants = append(dataReplicants, rep)
					dataSuccess++
				}
				dataWg.Done()
			case <-time.After(time.Duration(altNode.Config.PutDataTimeout) * time.Millisecond):
				dataWg.Done()
			}
		}(call)
	}

	dataWg.Wait()
	mdWg.Wait()

	if !(mdSuccess > ((altNode.Config.N - 1) / 2)) {
		// Cancel puts
		altNode.undoPutMD(k, mdReplicants)
		altNode.undoPutData(k, dataReplicants)
		// Return error
		return ErrPutMDFail
	}

	// Undo if put does not meet success criteria
	if ((args.Success == 0) && (dataSuccess != len(args.Replicants))) || (dataSuccess < args.Success) {
		altNode.undoPutMD(k, mdReplicants)
		altNode.undoPutData(k, dataReplicants)
		return ErrPutFail
	}
	return nil
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

// PutMetaArgs represents the arguments of a call to PutMetadata
type PutMetaArgs struct {
	Name string
	MD   Metadata
}

// PutMetadata puts the (key, val) pair in DB
func (altNode *Node) PutMetadata(args *PutMetaArgs, _ *struct{}) error {
	k := StringToKey(args.Name)
	// Serialize replicants
	md := serialize(args.MD)
	err := altNode.DB.Batch(func(tx *bolt.Tx) error {
		b := tx.Bucket(metaDataBucket)
		err := b.Put(k[:], md)
		return err
	})
	// if err == nil {
	// 	log.Print("Stored metadata for " + args.Name + ", key is " + k.String())
	// }
	return err
}

// RePutArgs are areguments for a call to RePut
type RePutArgs struct {
	LeaverID Key
	K        Key
	V        []byte
}

// RePut redoes a Put
func (altNode *Node) RePut(args *RePutArgs, _ *struct{}) error {
	// var successor Peer
	successor := altNode.getNthSuccessor(1)
	if successor.ID == args.LeaverID {
		successor = altNode.getNthSuccessor(2)
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
				randomID = altNode.Members.GetRandom().ID
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
	err := MakeRemoteCall(successor, "Put", putArgs, &struct{}{})
	checkErr("Put failed", err)

	return err
}

// undoPutData undoes all data puts for a key in a set of nodes
func (altNode *Node) undoPutData(k Key, nodes []*Peer) error {
	for _, node := range nodes {
		MakeRemoteCall(node, "DropKeyData", k, &struct{}{})
	}
	// Wrong
	return nil
}

// undoPutMD undoes all metadata puts for a key in a set of nodes
func (altNode *Node) undoPutMD(k Key, nodes []*Peer) error {
	for _, node := range nodes {
		MakeRemoteCall(node, "DropKeyMD", k, &struct{}{})
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
			call := MakeAsyncCall(&successor, "RePut", rpArgs, &struct{}{})
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
		rep := getPeer(altNode.Members.Map[repID])
		// In case rep left
		// TODO: this occurs because when replicants leave, nobody re-replicates their data
		if rep == nil {
			continue
		}
		var result []byte
		err := MakeRemoteCall(rep, "GetData", k, &result)
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
	var successor Peer
	var rawMD []byte
	altNode.FindSuccessor(k, &successor)
	i := 0
	for current := altNode.Members.Map[successor.ID]; i < altNode.Config.N; current = current.Next() {
		if current == nil {
			current = altNode.Members.List.Front()
		}
		err := MakeRemoteCall(getPeer(current), "GetMetadata", k, &rawMD)
		if err == nil {
			break
		}
		i++
	}
	md := bytesToMetadata(rawMD)
	return md
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
