package main

import (
	"crypto/sha1"
	"encoding/hex"
	"errors"
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
	ID          Key
	Address     string
	Port        string
	Successor   *ExtNode
	Predecessor *ExtNode
	Fingers     Fingers
	MemberHist  MemberHist
	DB          *bolt.DB
}

// ClientMap is a map of addresses to rpc clients
var ClientMap map[string]*rpc.Client

/* Alternator methods */

// GetFingers sets ret to this node's fingers
// func (altNode *Alternator) GetFingers(_ struct{}, ret *[]*ExtNode) error {
// 	*ret = altNode.Fingers.Slice
// 	return nil
// }

// func (altNode *Alternator) expelForeignKeys(elem *list.Element) {
// 	ext := getExt(elem)
// 	var prevID Key
// 	if prev := getExt(elem.Prev()); prev != nil {
// 		prevID = prev.ID
// 	} else {
// 		prevID = minKey
// 	}
// 	keys, vals := altNode.dbGetRange(prevID, ext.ID)
// 	for i := range keys {
// 		err := makeRemoteCall(ext, "DBPut", PutArgs{keys[i], vals[i]}, &struct{}{})
// 		checkLogErr(err)
// 		if err == nil {
// 			altNode.DBDelete(keys[i], &struct{}{})
// 		}
// 	}
// 	altNode.dbDeleteRange(prevID, ext.ID)
// }

func (altNode *Alternator) setSuccessor(new *ExtNode, caller string) {
	altNode.Successor = new
	fmt.Println(caller + " changed successor:")
	fmt.Println(altNode.string())
}

func (altNode *Alternator) setPredecessor(new *ExtNode, caller string) {
	if new == nil {
		altNode.Predecessor = nil
		// fmt.Println(altNode.string())
		// fmt.Println("Changed predecessor to <nil>")
		return
	}
	altNode.Predecessor = new
	fmt.Println(caller + " changed predecessor:")
	fmt.Println(altNode.string())
}

// GetSuccessor sets ret to the successor of an alternode
func (altNode *Alternator) GetSuccessor(_ struct{}, ret *ExtNode) error {
	extNodeCopy(altNode.Successor, ret)
	return nil
}

// GetPredecessor sets ret to the predecessor of an alternode
func (altNode *Alternator) GetPredecessor(_ struct{}, ret *ExtNode) error {
	if altNode.Predecessor == nil {
		return errors.New(ErrNilPredecessor)
	}
	extNodeCopy(altNode.Predecessor, ret)
	return nil
}

// JoinRequestArgs is the set of return parameters to a JoinRequest
type JoinRequestArgs struct {
	Keys []Key
	Vals [][]byte
}

// JoinRequest handles a request by another node to join the ring
func (altNode *Alternator) JoinRequest(other *ExtNode, ret *JoinRequestArgs) error {
	// Find pairs in joiner's range
	keys, vals := altNode.dbGetRange(altNode.ID, other.ID)
	// fmt.Printf("giving pairs in range %s to %s\n", keyToString(altNode.ID), keyToString(other.ID))
	for i := range keys {
		fmt.Println(keyToString(keys[i]))
	}

	// Add join to history
	newEntry := HistEntry{time.Now(), histJoin, *other}
	altNode.insertToHistory(newEntry)
	altNode.Fingers.Insert(other)
	ret.Keys = keys
	ret.Vals = vals
	return nil
}

// Join joins a node into an existing ring
func (altNode *Alternator) joinRing(broker *ExtNode) error {
	var successor ExtNode
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
	fmt.Println("new pairs are:")
	for i := range kvPairs.Keys {
		fmt.Println(keyToString(kvPairs.Keys[i]))
	}

	// Synchronize history with successor
	altNode.syncFingers(&successor)

	altNode.setPredecessor(nil, "Join()")

	// Atomic join
	altNode.Successor = &successor

	fmt.Println("Joined ring")
	fmt.Println(altNode.string())
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
	altNode.Fingers.Remove(&args.DepartureEntry.Node)
	// altNode.setPredecessor(altNode.getNthPredecessor(1), "LeaveRequest()")

	// Replicate in one more
	err := makeRemoteCall(altNode.getNthSuccessor(N-1), "BatchPut", batchArgs, &struct{}{})
	checkErr("Unhandled error", err)
	return nil
}

// LeaveRing makes the node leave the ring it is in
func (altNode *Alternator) LeaveRing(_ struct{}, _ *struct{}) error {
	if (altNode.Successor == nil) || (*altNode.Successor == altNode.selfExt()) {
		os.Exit(0)
		return nil
	}
	keys, vals := altNode.dbGetRange(altNode.Predecessor.ID, altNode.ID) // Gather entries
	departureEntry := HistEntry{time.Now(), histLeave, altNode.selfExt()}
	args := LeaveRequestArgs{keys, vals, departureEntry}

	var err error
	// Leave by notifying successor
	fmt.Println("asking for permission")
	err = makeRemoteCall(altNode.Successor, "LeaveRequest", &args, &struct{}{})
	if err != nil {
		return ErrLeaveFail
	}
	os.Exit(0)
	return nil
}

func (altNode Alternator) string() (str string) {
	str += "ID: " + keyToString(altNode.ID) + "\n"
	if altNode.Successor != nil {
		str += "Successor: " + keyToString(altNode.Successor.ID) + "\n"
	} else {
		str += "Successor: <nil>" + "\n"
	}

	if altNode.Predecessor != nil {
		str += "Predecessor: " + keyToString(altNode.Predecessor.ID) + "\n"
	} else {
		str += "Predecessor: <nil>" + "\n"
	}
	return
}

// FindSuccessor finds the successor of a key in the ring
func (altNode *Alternator) FindSuccessor(k Key, ret *ExtNode) error {
	succ, err := altNode.Fingers.FindSuccessor(k)
	*ret = *getExt(succ)
	return err
}

// selfExt returns an extNode equivalent of altNode
func (altNode *Alternator) selfExt() ExtNode {
	return ExtNode{altNode.ID, altNode.Address}
}

// stabilize fixes the successors periodically
func (altNode *Alternator) stabilize() {
	// fmt.Println("Stabilizing")

	var temp ExtNode
	err := makeRemoteCall(altNode.Successor, "GetPredecessor", struct{}{}, &temp)
	// checkErr("GetPredecessor error", err)

	if err == nil && inRange(temp.ID, altNode.ID, altNode.Successor.ID) {
		altNode.setSuccessor(&temp, "stabilize():")
	}

	self := altNode.selfExt()
	// fmt.Println("Notifying", altNode.Successor)
	makeRemoteCall(altNode.Successor, "Notify", self, nil)
}

// Notify is called by another node when it thinks it might be our predecessor
func (altNode *Alternator) Notify(candidate *ExtNode, _ *struct{}) error {
	// Should update fingers accordingly?
	if (altNode.Predecessor == nil) || (inRange(candidate.ID, altNode.Predecessor.ID, altNode.ID)) {
		altNode.setPredecessor(candidate, "Notify()")
	}
	return nil
}

// Heartbeat returns an 'OK' to the caller
func (altNode *Alternator) Heartbeat(_ struct{}, ret *string) error {
	// fmt.Println("Heartbeat called!")
	*ret = "OK"
	return nil
}

// checkPredecessor checks if the predecessor has failed
func (altNode *Alternator) checkPredecessor() {
	if (altNode.Predecessor == nil) || (keyCompare(altNode.Predecessor.ID, altNode.ID) == 0) {
		return
	}
	// Ping
	c := make(chan error, 1)
	beat := ""
	// fmt.Println("Sending heartbeat to " + altNode.Predecessor.ID)
	go func() {
		c <- makeRemoteCall(altNode.Predecessor, "Heartbeat", struct{}{}, &beat)
	}()

	select {
	case err := <-c:
		// Something wrong
		if (err != nil) || (beat != "OK") {
			// Kill connection
			closeRPC(altNode.Predecessor)

			// altNode.setPredecessor(nil, "checkPredecessor()")
		}
		// Call timed out
	case <-time.After(heartbeatTimeout * time.Millisecond):
		fmt.Println("Predecessor stopped responding, ceasing connection")
		// Kill connection
		closeRPC(altNode.Predecessor)

		// altNode.setPredecessor(nil, "checkPredecessor()")
	}
}

// createRing creates a ring with this node as its only member
func (altNode *Alternator) createRing() {
	altNode.setPredecessor(nil, "createRing()")
	var successor ExtNode
	successor.ID = altNode.ID
	successor.Address = altNode.Address
	// Add own join to ring
	self := altNode.selfExt()
	altNode.insertToHistory(HistEntry{time.Now(), histJoin, self})
	altNode.Fingers.Insert(&self)

	altNode.setSuccessor(&successor, "createRing()")
}

func (altNode *Alternator) autoCheckPredecessor() {
	for {
		altNode.checkPredecessor()
		time.Sleep(heartbeatTime * time.Millisecond)
	}
}

// AutoStabilize runs Stabilize() on timed intervals
func (altNode *Alternator) autoStabilize() {
	for {
		altNode.stabilize()
		time.Sleep(stableTime * time.Millisecond)
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
	node.Fingers.init()
	node.initDB()

	// Join a ring if address is specified
	if address != "" {
		// We do not know the ID of the node connecting to, use stand-in
		var k Key
		broker := ExtNode{k, address}
		node.joinRing(&broker)
	} else { // Else make a new ring
		node.createRing()
	}
	go node.autoCheckPredecessor()
	// go node.autoStabilize()
	go node.autoSyncFingers()
	go node.sigHandler()
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
