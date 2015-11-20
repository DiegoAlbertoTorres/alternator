package main

import (
	"fmt"
	"log"
	"os"

	"github.com/boltdb/bolt"
)

var dataBucket = []byte("data")
var metaDataBucket = []byte("metadata")

var dotPath = os.Getenv("HOME") + "/.alternator/"

// PutArgs is a struct to represent the arguments of Put or DBPut
type PutArgs struct {
	Name       string
	V          []byte
	Replicants []Key
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

func (altNode *Alternator) closeDB() {
	altNode.DB.Close()
}

func (altNode *Alternator) deleteRange() {

}

// dbGetRange returns all keys and values in a given range
// func (altNode *Alternator) dbGetRange(min, max Key) ([]Key, [][]byte) {
// 	var keys []Key
// 	var vals [][]byte
//
// 	err := altNode.DB.View(func(tx *bolt.Tx) error {
// 		b := tx.Bucket(dataBucket)
// 		c := b.Cursor()
//
// 		// Start seeking at mind
// 		c.Seek(min[:])
// 		// Stop at max
// 		for kslice, v := c.First(); kslice != nil && bytes.Compare(kslice, max[:]) < 0; kslice, v = c.Next() {
// 			// TODO: Set a proper capacity for keys and vals?
// 			keys = append(keys, sliceToKey(kslice))
// 			vals = append(vals, v)
// 		}
// 		return nil
// 	})
//
// 	checkErr("Get foreign error error:", err)
//
// 	return keys, vals
// }

// dbDeleteRange deletes all keys in a given range
// func (altNode *Alternator) dbDeleteRange(min, max Key) error {
// 	err := altNode.DB.View(func(tx *bolt.Tx) error {
// 		b := tx.Bucket([]byte(bucketName))
// 		c := b.Cursor()
//
// 		// Start seeking at mind
// 		c.Seek(min[:])
// 		// Stop at max
// 		for k, _ := c.First(); k != nil && bytes.Compare(k, max[:]) < 0; k, _ = c.Next() {
// 			b.Delete(k)
// 		}
// 		return nil
// 	})
// 	return err
// }

// BatchInsert inserts a set of keys/vals into the database in a single batch transaction
// func (altNode *Alternator) BatchInsert(args BatchInsertArgs, _ *struct{}) error {
// 	err := altNode.DB.Batch(func(tx *bolt.Tx) error {
// 		b := tx.Bucket([]byte(bucketName))
// 		for i := range args.Keys {
// 			// TODO: catch put error, do something?
// 			err := b.Put(args.Keys[i][:], args.Vals[i])
// 			log.Print(err)
// 		}
// 		return nil
// 	})
// 	return err
// }

// DBDelete deletes a key from the database
// func (altNode *Alternator) DBDelete(k Key, _ *struct{}) error {
// 	return altNode.DB.View(func(tx *bolt.Tx) error {
// 		b := tx.Bucket([]byte(bucketName))
// 		err := b.Delete(k[:])
// 		return err
// 	})
// }

// DBGet gets gets the value corresponding to key, sets ret to this value
// func (altNode *Alternator) DBGet(k Key, ret *[]byte) error {
// 	fmt.Println("About to get key")
// 	return altNode.DB.View(func(tx *bolt.Tx) error {
// 		b := tx.Bucket([]byte(bucketName))
// 		result := b.Get(k[:])
// 		if result == nil {
// 			return ErrKeyNotFound
// 		}
// 		*ret = result
// 		// fmt.Printf("The answer is: %s\n", v)
// 		return nil
// 	})
// }

// PutData puts the (hash(name), val) pair in DB
func (altNode *Alternator) PutData(args *PutArgs, _ *struct{}) error {
	fmt.Println("About to put pair " + args.Name + "," + string(args.V))
	altNode.DB.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(dataBucket)
		kslice := stringToKey(args.Name)
		err := b.Put(kslice[:], args.V)
		return err
	})
	return nil
}

// PutMetaArgs represents the arguments of a call to PutMetadata
type PutMetaArgs struct {
	Name string
	MD   Metadata
}

// PutMetadata puts the (key, val) pair in DB
func (altNode *Alternator) PutMetadata(args *PutMetaArgs, _ *struct{}) error {
	fmt.Println("About to put meta " + args.Name)
	// Serialize replicants
	md := serialize(args.MD)
	altNode.DB.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(metaDataBucket)
		kslice := stringToKey(args.Name)
		err := b.Put(kslice[:], md)
		return err
	})
	return nil
}

// Get gets an entry from the DHT
// func (altNode *Alternator) Get(k Key, ret *[]byte) error {
// 	var ext ExtNode
// 	var result []byte
// 	altNode.FindSuccessor(k, &ext)
// 	// Resolve in this node
// 	if keyCompare(ext.ID, altNode.ID) == 0 {
// 		return altNode.DBGet(k, ret)
// 	}
// 	// Redirect
// 	fmt.Println("Redirecting get " + keyToString(k))
// 	err := makeRemoteCall(&ext, "DBGet", k, &result)
// 	*ret = result
// 	return err
// }

// Put adds an entry to the kv store
// func (altNode *Alternator) Put(args *PutArgs, _ *struct{}) error {
// 	// Find successor
// 	var ext ExtNode
// 	altNode.FindSuccessor(args.K, &ext)
// 	// This node is successor
// 	if keyCompare(altNode.ID, ext.ID) == 0 {
// 		altNode.DBPut(args, &struct{}{})
// 	} else {
// 		fmt.Println("Redirecting put pair " + keyToString(args.K) + "," + string(args.V))
// 		// Put it in the right successor
// 		return makeRemoteCall(&ext, "DBPut", args, &struct{}{})
// 	}
// 	return nil
// }

// Metadata represents the metadata of a (key, value) pair
type Metadata struct {
	Replicants []Key
}

// Put puts a (key, value) pair in the system
func (altNode *Alternator) Put(args *PutArgs, _ *struct{}) error {
	// Hash name
	k := stringToKey(args.Name)
	var successor ExtNode
	altNode.FindSuccessor(k, &successor)
	// Put metadata in name's successor
	putMDArgs := PutMetaArgs{args.Name, Metadata{args.Replicants}}
	err := makeRemoteCall(&successor, "PutMetadata", putMDArgs, &struct{}{})
	if checkErr("PutMetadata error", err) {
		return err
	}
	// Put data in replicants
	for _, rep := range args.Replicants {
		ext, ok := altNode.Fingers.Map[rep]
		if !ok {
			log.Print("Fingers map wrong error")
			continue
		}
		err = makeRemoteCall(ext, "PutData", args, &struct{}{})
		checkLogErr(err)
	}

	return nil
}

// GetMetadata returns the metadata (serialized) of a specific variable
func (altNode *Alternator) GetMetadata(k Key, md *[]byte) error {
	fmt.Println("About to get metadata")
	return altNode.DB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(metaDataBucket)
		*md = b.Get(k[:])
		if md == nil {
			return ErrKeyNotFound
		}
		// fmt.Printf("The answer is: %s\n", v)
		return nil
	})
}

// GetData returns the metadata (serialized) of a specific variable
func (altNode *Alternator) GetData(k Key, data *[]byte) error {
	fmt.Println("About to get data")
	return altNode.DB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(dataBucket)
		*data = b.Get(k[:])
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
	var successor ExtNode
	var rawMD []byte

	altNode.FindSuccessor(k, &successor)
	// Get replicants from coordinator
	err := makeRemoteCall(&successor, "GetMetadata", k, &rawMD)
	md := bytesToMetadata(rawMD)

	if checkLogErr(err) {
		return err
	}
	// Get data from some replicant
	for _, repID := range md.Replicants {
		rep := altNode.Fingers.Map[repID]
		err := makeRemoteCall(rep, "GetData", k, ret)
		if err == nil {
			return nil
		}
	}
	// None of the replicants had the data?
	return ErrDataLost
}
