package main

import (
	"bytes"
	"fmt"
	"log"
	"os"

	"github.com/boltdb/bolt"
)

// N is the number of nodes in which metadata is replicated
const N = 2

var dataBucket = []byte("data")
var metaDataBucket = []byte("metadata")

var dotPath = os.Getenv("HOME") + "/.alternator/"

// PutArgs is a struct to represent the arguments of Put or DBPut
type PutArgs struct {
	Name       string
	V          []byte
	Replicants []Key
	Success    int
}

// BatchInsertArgs is a struct to represent the arguments of BatchInsert
type BatchInsertArgs struct {
	Keys []Key
	Vals [][]byte
}

func (altNode *Alternator) initDB() {
	err := os.MkdirAll(dotPath, 0777)
	checkFatal(err)
	altNode.DB, err = bolt.Open(dotPath+altNode.Port+".db", 0600, nil)
	if !checkFatal(err) {
		// Create a bucket for all entries
		altNode.DB.Update(func(tx *bolt.Tx) error {
			// Create buckets
			_, err := tx.CreateBucket(dataBucket)
			if err != nil {
				if err != bolt.ErrBucketExists {
					log.Fatal(err)
					return fmt.Errorf("create bucket: %s", err)
				}
			}
			_, err = tx.CreateBucket(metaDataBucket)
			if err != nil {
				if err != bolt.ErrBucketExists {
					log.Fatal(err)
					return fmt.Errorf("create bucket: %s", err)
				}
			}
			return nil
		})
	}
}

// dbGetRange returns all keys and values in a given range
func (altNode *Alternator) dbGetRange(min, max Key) ([]Key, [][]byte) {
	var keys []Key
	var vals [][]byte

	err := altNode.DB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(metaDataBucket)
		c := b.Cursor()

		// Start seeking at min
		c.Seek(min[:])
		// Stop at max
		for kslice, v := c.First(); kslice != nil && bytes.Compare(kslice, max[:]) < 0; kslice, v = c.Next() {
			// TODO: Set a proper capacity for keys and vals?
			keys = append(keys, sliceToKey(kslice))
			vals = append(vals, v)
		}
		return nil
	})
	checkErr("Get range error error: ", err)

	return keys, vals
}

func (altNode *Alternator) closeDB() {
	altNode.DB.Close()
}

// PutData puts the (hash(name), val) pair in DB
func (altNode *Alternator) PutData(args *PutArgs, _ *struct{}) error {
	k := stringToKey(args.Name)
	err := altNode.DB.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(dataBucket)
		err := b.Put(k[:], args.V)
		return err
	})
	if err == nil {
		log.Print("Stored pair " + args.Name + "," + string(args.V) + " key is " + keyToString(k))
	}
	return err
}

// PutMetaArgs represents the arguments of a call to PutMetadata
type PutMetaArgs struct {
	Name string
	MD   Metadata
}

// PutMetadata puts the (key, val) pair in DB
func (altNode *Alternator) PutMetadata(args *PutMetaArgs, _ *struct{}) error {
	k := stringToKey(args.Name)
	// Serialize replicants
	md := serialize(args.MD)
	err := altNode.DB.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(metaDataBucket)
		err := b.Put(k[:], md)
		return err
	})
	if err == nil {
		log.Print("Stored metadata for " + args.Name + ", key is " + keyToString(k))
	}
	return err
}

// DropKeyMD Drops all metadata associated with a key
func (altNode *Alternator) DropKeyMD(k *Key, _ *struct{}) error {
	err := altNode.DB.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(metaDataBucket)
		err := b.Delete(k[:])
		return err
	})
	return err
}

// DropKeyData drops all data associated with a key
func (altNode *Alternator) DropKeyData(k *Key, _ *struct{}) error {
	err := altNode.DB.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(dataBucket)
		err := b.Delete(k[:])
		return err
	})
	return err
}

// Metadata represents the metadata of a (key, value) pair
type Metadata struct {
	Replicants []Key
}

// Put a (key, value) pair in the system
func (altNode *Alternator) Put(args *PutArgs, _ *struct{}) error {
	k := stringToKey(args.Name)
	var successor Peer
	err := altNode.FindSuccessor(k, &successor)

	// Pass the call to successor
	if (err == nil) && (successor.ID != altNode.ID) {
		err := makeRemoteCall(&successor, "Put", args, &struct{}{})
		return err
	}
	// Else resolve in this node
	putMDArgs := PutMetaArgs{args.Name, Metadata{args.Replicants}}
	// // Store metadata here
	// altNode.PutMetadata(&putMDArgs, &struct{}{})

	// Store in chain
	i := 0
	success := 0
	mdReplicants := make([]*Peer, 0, N)
	for current := altNode.Members.Map[altNode.ID]; i < N; current = current.Next() {
		if current == nil {
			current = altNode.Members.List.Front()
		}
		err := makeRemoteCall(getExt(current), "PutMetadata", putMDArgs, &struct{}{})
		if err != nil {
			checkErr("metadata put fail", err)
		} else {
			success++
			mdReplicants = append(mdReplicants, getExt(current))
		}
		i++
	}

	if !(success > ((N - 1) / 2)) {
		// Cancel puts
		altNode.undoPutMD(k, mdReplicants)
		// Return error
		return ErrPutMDFail
	}

	// Put data in replicants
	dataReplicants := make([]*Peer, 0, len(args.Replicants))
	success = 0
	for _, repID := range args.Replicants {
		repLNode, ok := altNode.Members.Map[repID]
		if !ok {
			log.Print("Members map wrong error")
			continue
		}
		rep := getExt(repLNode)
		err = makeRemoteCall(rep, "PutData", args, &struct{}{})
		if err == nil {
			dataReplicants = append(dataReplicants, rep)
			success++
		} else {
			log.Print(err)
		}
	}

	// Undo if put does not meet success criteria
	if ((args.Success == 0) && (success != len(args.Replicants))) || (success < args.Success) {
		altNode.undoPutMD(k, mdReplicants)
		altNode.undoPutData(k, dataReplicants)
		return ErrPutFail
	}
	return nil
}

// undoPutData undoes all data puts for a key in a set of nodes
func (altNode *Alternator) undoPutData(k Key, nodes []*Peer) error {
	for _, node := range nodes {
		makeRemoteCall(node, "DropKeyData", k, &struct{}{})
	}
	// Wrong
	return nil
}

// undoPutMD undoes all metadata puts for a key in a set of nodes
func (altNode *Alternator) undoPutMD(k Key, nodes []*Peer) error {
	for _, node := range nodes {
		makeRemoteCall(node, "DropKeyMD", k, &struct{}{})
	}
	// Wrong
	return nil
}

// GetMetadata returns the metadata (serialized) of a specific variable
func (altNode *Alternator) GetMetadata(k Key, md *[]byte) error {
	// fmt.Println("About to get metadata")
	return altNode.DB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(metaDataBucket)
		*md = b.Get(k[:])
		fmt.Println("Getting metadata for " + keyToString(k))
		if md == nil {
			return ErrKeyNotFound
		}
		// fmt.Printf("The answer is: %s\n", v)
		return nil
	})
}

// GetData returns the metadata (serialized) of a specific variable
func (altNode *Alternator) GetData(k Key, data *[]byte) error {
	// fmt.Println("About to get data")
	return altNode.DB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(dataBucket)
		*data = b.Get(k[:])
		fmt.Println("Getting data for " + keyToString(k))
		if data == nil {
			return ErrKeyNotFound
		}
		// fmt.Printf("The answer is: %s\n", v)
		return nil
	})
}

// Get gets from the system the value corresponding to the key
func (altNode *Alternator) Get(name string, ret *[]byte) error {
	k := stringToKey(name)
	var rawMD []byte

	// Get replicants from metadata chain
	var successor Peer
	altNode.FindSuccessor(k, &successor)
	i := 0
	for current := altNode.Members.Map[successor.ID]; i < N; current = current.Next() {
		if current == nil {
			current = altNode.Members.List.Front()
		}
		fmt.Printf("asking %s\n", keyToString(getExt(current).ID))
		i++
		err := makeRemoteCall(getExt(current), "GetMetadata", k, &rawMD)
		if err == nil {
			break
		}
	}
	md := bytesToMetadata(rawMD)

	// Get data from some replicant
	for _, repID := range md.Replicants {
		rep := getExt(altNode.Members.Map[repID])
		// In case rep left
		// TODO: this occurs because when replicants leave, nobody re-replicates their data
		if rep == nil {
			continue
		}
		var result []byte
		fmt.Println("trying to get data from", rep)
		err := makeRemoteCall(rep, "GetData", k, &result)
		if err == nil {
			*ret = result
			return nil
		}
	}
	// None of the replicants had the data?
	fmt.Println("***************************hit here")
	return ErrDataLost
}

// BatchPutArgs are the arguments for a batch put
type BatchPutArgs struct {
	Bucket []byte
	Keys   []Key
	Vals   [][]byte
}

// BatchPut puts a set of key-value pairs in the specified bucket
func (altNode *Alternator) BatchPut(args BatchPutArgs, _ *struct{}) error {
	err := altNode.DB.Batch(func(tx *bolt.Tx) error {
		b := tx.Bucket(args.Bucket)
		anyError := false
		for i := range args.Keys {
			// TODO: handle error
			err := b.Put(args.Keys[i][:], args.Vals[i])
			if err != nil {
				anyError = true
			}
			log.Printf("Stored metadata for%s\n", keyToString(args.Keys[i]))
		}
		if anyError {
			return ErrBatchPutIncomplete
		}
		return nil
	})
	return err
}
