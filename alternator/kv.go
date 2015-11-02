package main

import (
	"fmt"
	"log"

	"github.com/boltdb/bolt"
)

const bucketName = "Alternator"

// PutArgs is a struct to represent the arguments of a call to Put or DBPut
type PutArgs struct {
	Key string
	Val []byte
}

func (altNode *AlterNode) initDB() {
	var err error
	altNode.DB, err = bolt.Open("alternator"+altNode.Port+".db", 0600, nil)
	if err != nil {
		log.Fatal(err)
	} else {
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

func (altNode *AlterNode) closeDB() {
	altNode.DB.Close()
}

// DBGet gets gets the value corresponding to key, sets ret to this value
func (altNode *AlterNode) DBGet(key string, ret *[]byte) error {
	fmt.Println("About to get key")
	return altNode.DB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketName))
		result := b.Get([]byte(key))
		if result == nil {
			return ErrKeyNotFound
		}
		// fmt.Printf("The answer is: %s\n", v)
		return nil
	})
}

// DBPut puts the (key, val) pair in DB
func (altNode *AlterNode) DBPut(args *PutArgs, _ *struct{}) error {
	fmt.Println("About to put pair " + args.Key + "," + string(args.Val))
	altNode.DB.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketName))
		err := b.Put([]byte(args.Key), args.Val)
		return err
	})
	return nil
}

// Get gets an entry from the DHT
func (altNode *AlterNode) Get(key string, ret *[]byte) error {
	var ext ExtNode
	var result []byte
	altNode.FindSuccessor(key, &ext)
	// Resolve in this node
	if ext.ID == altNode.ID {
		return altNode.DBGet(key, ret)
	}
	// Redirect
	fmt.Println("Redirecting get " + key)
	err := makeRemoteCall(&ext, "DBGet", key, &result)
	*ret = result
	return err
}

// Put adds an entry to the kv store
func (altNode *AlterNode) Put(args *PutArgs, _ *struct{}) error {
	// Find successor
	var ext ExtNode
	altNode.FindSuccessor(args.Key, &ext)
	// This node is successor
	if ext.ID == altNode.ID {
		altNode.DBPut(args, &struct{}{})
	} else {
		fmt.Println("Redirecting put pair " + args.Key + "," + string(args.Val))
		// Put it in the right successor
		return makeRemoteCall(&ext, "DBPut", args, &struct{}{})
	}
	return nil
}
