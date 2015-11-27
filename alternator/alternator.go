package main

import (
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"time"

	"github.com/boltdb/bolt"
)

// All in milliseconds
const stableTime = 1000
const heartbeatTime = 1000
const heartbeatTimeout = 400

var maxSlice, _ = hex.DecodeString("ffffffffffffffffffffffffffffffffffffffff")
var maxKey = sliceToKey(maxSlice)
var minSlice, _ = hex.DecodeString("0000000000000000000000000000000000000000")
var minKey = sliceToKey(minSlice)

// Alternator is a node in Alternator
type Alternator struct {
	ID         Key
	Address    string
	Port       string
	Members    Members
	MemberHist MemberHist
	DB         *bolt.DB
}

// ClientMap is a map of addresses to rpc clients
var ClientMap map[string]*rpc.Client

/* Alternator methods */

// GetMembers sets ret to this node's members
// func (altNode *Alternator) GetMembers(_ struct{}, ret *[]*Peer) error {
// 	*ret = altNode.Members.Slice
// 	return nil
// }

// func (altNode *Alternator) expelForeignKeys(elem *list.Element) {
// 	peer := getExt(elem)
// 	var prevID Key
// 	if prev := getExt(elem.Prev()); prev != nil {
// 		prevID = prev.ID
// 	} else {
// 		prevID = minKey
// 	}
// 	keys, vals := altNode.dbGetRange(prevID, peer.ID)
// 	for i := range keys {
// 		err := makeRemoteCall(peer, "DBPut", PutArgs{keys[i], vals[i]}, &struct{}{})
// 		checkLogErr(err)
// 		if err == nil {
// 			altNode.DBDelete(keys[i], &struct{}{})
// 		}
// 	}
// 	altNode.dbDeleteRange(prevID, peer.ID)
// }

// JoinRequestArgs is the set of return parameters to a JoinRequest
type JoinRequestArgs struct {
	Keys []Key
	Vals [][]byte
}

// JoinRequest handles a request by another node to join the ring
func (altNode *Alternator) JoinRequest(other *Peer, ret *JoinRequestArgs) error {
	// Find pairs in joiner's range
	keys, vals := altNode.dbGetRange(altNode.ID, other.ID)
	// fmt.Printf("giving pairs in range %s to %s\n", keyToString(altNode.ID), keyToString(other.ID))
	for i := range keys {
		fmt.Println(keyToString(keys[i]))
	}

	// Add join to history
	newEntry := HistEntry{time.Now(), histJoin, *other}
	altNode.insertToHistory(newEntry)
	altNode.Members.Insert(other)
	fmt.Println("Members changed:", altNode.Members)
	ret.Keys = keys
	ret.Vals = vals
	return nil
}

// Join joins a node into an existing ring
func (altNode *Alternator) joinRing(broker *Peer) error {
	var successor Peer
	// Find future successor using some broker in ring
	err := makeRemoteCall(broker, "FindSuccessor", altNode.ID, &successor)
	if err != nil {
		return ErrJoinFail
	}

	// Do join through future successor
	var kvPairs JoinRequestArgs
	err = makeRemoteCall(&successor, "JoinRequest", altNode.selfExt(), &kvPairs)
	if err != nil {
		return ErrJoinFail
	}
	// for i := range kvPairs.Keys {
	// 	fmt.Println(keyToString(kvPairs.Keys[i]))
	// }

	// Synchronize history with successor
	altNode.syncMembers(&successor)

	fmt.Println("Successfully joined ring")
	return nil
}

// LeaveRequestArgs holds arguments for a leave request
type LeaveRequestArgs struct {
	Keys           []Key
	Vals           [][]byte
	DepartureEntry HistEntry
}

// LeaveRequest handles a leave request. It appends the departure entry to the node's history.
func (altNode *Alternator) LeaveRequest(args *LeaveRequestArgs, _ *struct{}) error {
	// Insert received keys
	batchArgs := BatchPutArgs{metaDataBucket, args.Keys, args.Vals}
	altNode.BatchPut(batchArgs, &struct{}{})
	altNode.insertToHistory(args.DepartureEntry)
	altNode.Members.Remove(&args.DepartureEntry.Node)
	fmt.Println("Members changed:", altNode.Members)
	// altNode.setPredecessor(altNode.getNthPredecessor(1), "LeaveRequest()")

	// Replicate in one more
	err := makeRemoteCall(altNode.getNthSuccessor(N-1), "BatchPut", batchArgs, &struct{}{})
	checkErr("Unhandled error", err)
	return nil
}

// LeaveRing makes the node leave the ring it is in
func (altNode *Alternator) LeaveRing(_ struct{}, _ *struct{}) error {
	successor := altNode.getSuccessor()
	if *successor == altNode.selfExt() {
		os.Exit(0)
		return nil
	}
	keys, vals := altNode.dbGetRange(altNode.getPredecessor().ID, altNode.ID) // Gather entries
	departureEntry := HistEntry{time.Now(), histLeave, altNode.selfExt()}
	args := LeaveRequestArgs{keys, vals, departureEntry}

	var err error
	// Leave by notifying successor
	err = makeRemoteCall(successor, "LeaveRequest", &args, &struct{}{})
	if err != nil {
		return ErrLeaveFail
	}
	os.Exit(0)
	return nil
}

func (altNode Alternator) string() (str string) {
	str += "ID: " + keyToString(altNode.ID) + "\n"
	// if altNode.Successor != nil {
	// 	str += "Successor: " + keyToString(altNode.Successor.ID) + "\n"
	// } else {
	// 	str += "Successor: <nil>" + "\n"
	// }
	//
	// if altNode.Predecessor != nil {
	// 	str += "Predecessor: " + keyToString(altNode.Predecessor.ID) + "\n"
	// } else {
	// 	str += "Predecessor: <nil>" + "\n"
	// }
	return
}

// FindSuccessor finds the successor of a key in the ring
func (altNode *Alternator) FindSuccessor(k Key, ret *Peer) error {
	succ, err := altNode.Members.FindSuccessor(k)
	*ret = *getExt(succ)
	return err
}

// selfExt returns an peerNode equivalent of altNode
func (altNode *Alternator) selfExt() Peer {
	return Peer{altNode.ID, altNode.Address}
}

// Heartbeat returns an 'OK' to the caller
func (altNode *Alternator) Heartbeat(_ struct{}, ret *string) error {
	// fmt.Println("Heartbeat called!")
	*ret = "OK"
	return nil
}

// checkPredecessor checks if the predecessor has failed
func (altNode *Alternator) checkPredecessor() {
	predecessor := altNode.getPredecessor()
	if keyCompare(predecessor.ID, altNode.ID) == 0 {
		return
	}
	// Ping
	c := make(chan error, 1)
	beat := ""
	go func() {
		c <- makeRemoteCall(predecessor, "Heartbeat", struct{}{}, &beat)
	}()

	// TODO: do something smart with heartbeat failures to autodetect departures
	select {
	case err := <-c:
		// Something wrong
		if (err != nil) || (beat != "OK") {
			// Kill connection
			closeRPC(predecessor)

			// altNode.setPredecessor(nil, "checkPredecessor()")
		}
		// Call timed out
	case <-time.After(heartbeatTimeout * time.Millisecond):
		fmt.Println("Predecessor stopped responding, ceasing connection")
		// Kill connection
		closeRPC(predecessor)

		// altNode.setPredecessor(nil, "checkPredecessor()")
	}
}

// createRing creates a ring with this node as its only member
func (altNode *Alternator) createRing() {
	var successor Peer
	successor.ID = altNode.ID
	successor.Address = altNode.Address
	// Add own join to ring
	self := altNode.selfExt()
	altNode.insertToHistory(HistEntry{time.Now(), histJoin, self})
	altNode.Members.Insert(&self)
}

func (altNode *Alternator) autoCheckPredecessor() {
	for {
		altNode.checkPredecessor()
		time.Sleep(heartbeatTime * time.Millisecond)
	}
}

func (altNode *Alternator) sigHandler() {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, os.Kill)
	for {
		sig := <-sigChan
		switch sig {
		case os.Interrupt:
			altNode.LeaveRing(struct{}{}, &struct{}{})
		case os.Kill:
			os.Exit(1)
		}
	}
}

// InitNode initializes and alternode
func InitNode(port string, address string) {
	// Init connection map
	ClientMap = make(map[string]*rpc.Client)

	// Register node as RPC server
	node := new(Alternator)
	rpc.Register(node)
	rpc.HandleHTTP()
	l, err := net.Listen("tcp", ":"+port)
	checkErr("listen error ", err)
	// Get port selected by server
	port = strings.Split(l.Addr().String(), ":")[3]

	// Initialize Alternator fields
	node.Address = "127.0.0.1:" + port
	node.Port = port
	node.ID = genID(port)
	node.Members.init()
	node.initDB()

	// Join a ring if address is specified
	if address != "" {
		// We do not know the ID of the node connecting to, use stand-in
		var k Key
		broker := Peer{k, address}
		node.joinRing(&broker)
	} else { // Else make a new ring
		node.createRing()
	}
	go node.autoCheckPredecessor()
	go node.autoSyncMembers()
	go node.sigHandler()
	fmt.Println(node.string())
	fmt.Println("Listening on port " + port)
	http.Serve(l, nil)
}

func genID(port string) Key {
	hostname, _ := os.Hostname()
	h := sha1.New()
	rand.Seed(initSeed)
	io.WriteString(h, hostname+port+strconv.Itoa(rand.Int()))
	return sliceToKey(h.Sum(nil))
}
