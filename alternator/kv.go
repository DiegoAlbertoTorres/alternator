package main

import (
	"bytes"
	"fmt"
	"log"
	"os"

	"github.com/boltdb/bolt"
)

const bucketName = "Alternator"

var dotPath = os.Getenv("HOME") + "/.alternator/"

// PutArgs is a struct to represent the arguments of Put or DBPut
type PutArgs struct {
	K Key
	V []byte
}

// BatchInsertArgs is a struct to represent the arguments of BatchInsert
type BatchInsertArgs struct {
	Keys []Key
	Vals [][]byte
}

func (altNode *Alernator) initDB() {
	err := os.MkdirAll(dotPath, 0777)
	checkFatal(err)
	altNode.DB, err = bolt.Open(dotPath+altNode.Port+".db", 0600, nil)
	if !checkFatal(err) {
		// Create a bucket for all entries
		altNode.DB.Update(func(tx *bolt.Tx) error {
			// Single bucket system
			_, err := tx.CreateBucket([]byte(bucketName))
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

func (altNode *Alernator) closeDB() {
	altNode.DB.Close()
}

func (altNode *Alernator) deleteRange() {

}

// dbGetRange returns all keys and values in a given range
func (altNode *Alernator) dbGetRange(min, max Key) ([]Key, [][]byte) {
	var keys []Key
	var vals [][]byte

	err := altNode.DB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketName))
		c := b.Cursor()

		// Start seeking at mind
		c.Seek(min[:])
		// Stop at max
		for kslice, v := c.First(); kslice != nil && bytes.Compare(kslice, max[:]) < 0; kslice, v = c.Next() {
			// TODO: Set a proper capacity for keys and vals?
			keys = append(keys, sliceToKey(kslice))
			vals = append(vals, v)
		}
		return nil
	})

	checkErr("Get foreign error error:", err)

	return keys, vals
}

// dbDeleteRange deletes all keys in a given range
func (altNode *Alernator) dbDeleteRange(min, max Key) error {
	err := altNode.DB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketName))
		c := b.Cursor()

		// Start seeking at mind
		c.Seek(min[:])
		// Stop at max
		for k, _ := c.First(); k != nil && bytes.Compare(k, max[:]) < 0; k, _ = c.Next() {
			b.Delete(k)
		}
		return nil
	})
	return err
}

// BatchInsert inserts a set of keys/vals into the database in a single batch transaction
func (altNode *Alernator) BatchInsert(args BatchInsertArgs, _ *struct{}) error {
	err := altNode.DB.Batch(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketName))
		for i := range args.Keys {
			// TODO: catch put error, do something?
			err := b.Put(args.Keys[i][:], args.Vals[i])
			log.Print(err)
		}
		return nil
	})
	return err
}

// DBDelete deletes a key from the database
func (altNode *Alernator) DBDelete(k Key, _ *struct{}) error {
	return altNode.DB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketName))
		err := b.Delete(k[:])
		return err
	})
}

// DBGet gets gets the value corresponding to key, sets ret to this value
func (altNode *Alernator) DBGet(k Key, ret *[]byte) error {
	fmt.Println("About to get key")
	return altNode.DB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketName))
		result := b.Get(k[:])
		if result == nil {
			return ErrKeyNotFound
		}
		*ret = result
		// fmt.Printf("The answer is: %s\n", v)
		return nil
	})
}

// DBPut puts the (key, val) pair in DB
func (altNode *Alernator) DBPut(args *PutArgs, _ *struct{}) error {
	fmt.Println("About to put pair " + keyToString(args.K) + "," + string(args.V))
	altNode.DB.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketName))
		err := b.Put(args.K[:], args.V)
		return err
	})
	return nil
}

// Get gets an entry from the DHT
func (altNode *Alernator) Get(k Key, ret *[]byte) error {
	var ext ExtNode
	var result []byte
	altNode.FindSuccessor(k, &ext)
	// Resolve in this node
	if keyCompare(ext.ID, altNode.ID) == 0 {
		return altNode.DBGet(k, ret)
	}
	// Redirect
	fmt.Println("Redirecting get " + keyToString(k))
	err := makeRemoteCall(&ext, "DBGet", k, &result)
	*ret = result
	return err
}

// Put adds an entry to the kv store
func (altNode *Alernator) Put(args *PutArgs, _ *struct{}) error {
	// Find successor
	var ext ExtNode
	altNode.FindSuccessor(args.K, &ext)
	// This node is successor
	if keyCompare(altNode.ID, ext.ID) == 0 {
		altNode.DBPut(args, &struct{}{})
	} else {
		fmt.Println("Redirecting put pair " + keyToString(args.K) + "," + string(args.V))
		// Put it in the right successor
		return makeRemoteCall(&ext, "DBPut", args, &struct{}{})
	}
	return nil
}
