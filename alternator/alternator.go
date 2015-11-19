package main

import (
	"container/list"
	"crypto/sha1"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"net/http"
	"net/rpc"
	"os"
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

// Alernator is a node in Alternator
type Alernator struct {
	ID          Key
	Address     string
	Port        string
	Successor   *ExtNode
	Predecessor *ExtNode
	Fingers     Fingers
	DB          *bolt.DB
}

// ClientMap is a map of addresses to rpc clients
var ClientMap map[string]*rpc.Client

/* Alernator methods */

// GetFingers sets ret to this node's fingers
func (altNode *Alernator) GetFingers(_ struct{}, ret *[]*ExtNode) error {
	*ret = altNode.Fingers.Slice
	return nil
}

func (altNode *Alernator) expelForeignKeys(elem *list.Element) {
	ext := getExt(elem)
	var prevID Key
	if prev := getExt(elem.Prev()); prev != nil {
		prevID = prev.ID
	} else {
		prevID = minKey
	}
	keys, vals := altNode.dbGetRange(prevID, ext.ID)
	for i := range keys {
		err := makeRemoteCall(ext, "DBPut", PutArgs{keys[i], vals[i]}, &struct{}{})
		checkLogErr(err)
		if err == nil {
			altNode.DBDelete(keys[i], &struct{}{})
		}
	}
	altNode.dbDeleteRange(prevID, ext.ID)
}

func (altNode *Alernator) setSuccessor(new *ExtNode, caller string) {
	altNode.Successor = new
	fmt.Println(caller + " changed successor:")
	fmt.Println(altNode.string())
}

func (altNode *Alernator) setPredecessor(new *ExtNode, caller string) {
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
func (altNode *Alernator) GetSuccessor(_ struct{}, ret *ExtNode) error {
	extNodeCopy(altNode.Successor, ret)
	return nil
}

// GetPredecessor sets ret to the predecessor of an alternode
func (altNode *Alernator) GetPredecessor(_ struct{}, ret *ExtNode) error {
	if altNode.Predecessor == nil {
		return errors.New(ErrNilPredecessor)
	}
	extNodeCopy(altNode.Predecessor, ret)
	return nil
}

// Join joins a node into an existing ring
func (altNode *Alernator) Join(broker *ExtNode, _ *struct{}) error {
	altNode.setPredecessor(nil, "Join()")
	makeRemoteCall(broker, "FindSuccessor", altNode.ID, &altNode.Successor)

	fmt.Println("Joined ring")
	fmt.Println(altNode.string())
	return nil
}

func (altNode Alernator) string() (str string) {
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
func (altNode *Alernator) FindSuccessor(k Key, ret *ExtNode) error {
	succ, err := altNode.Fingers.FindSuccessor(k)
	*ret = *succ
	return err
}

// stabilize fixes the successors periodically
func (altNode *Alernator) stabilize() {
	// fmt.Println("Stabilizing")

	var temp ExtNode
	err := makeRemoteCall(altNode.Successor, "GetPredecessor", struct{}{}, &temp)
	if err != nil {
		log.Print("GetPredecessor error ", err)
	}

	if err == nil && inRange(temp.ID, altNode.ID, altNode.Successor.ID) {
		fmt.Println("setting successor to", temp)
		altNode.setSuccessor(&temp, "stabilize():")
	}

	selfExt := ExtNode{altNode.ID, altNode.Address}
	// fmt.Println("Notifying", altNode.Successor)
	makeRemoteCall(altNode.Successor, "Notify", selfExt, nil)
}

// Notify is called by another node when it thinks it might be our predecessor
func (altNode *Alernator) Notify(candidate *ExtNode, _ *struct{}) error {
	// Should update fingers accordingly?
	if (altNode.Predecessor == nil) || (inRange(candidate.ID, altNode.Predecessor.ID, altNode.ID)) {
		altNode.setPredecessor(candidate, "Notify()")
	}
	return nil
}

// Heartbeat returns an 'OK' to the caller
func (altNode *Alernator) Heartbeat(_ struct{}, ret *string) error {
	// fmt.Println("Heartbeat called!")
	*ret = "OK"
	return nil
}

// checkPredecessor checks if the predecessor has failed
func (altNode *Alernator) checkPredecessor() {
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
			fmt.Println("Heartbeat error, ceasing connection")
			// Kill connection
			closeRPC(altNode.Predecessor)

			// altNode.setPredecessor(nil, "checkPredecessor()")
		}
		// Call timed out
	case <-time.After(heartbeatTimeout * time.Millisecond):
		fmt.Println("Predecessor stopped responding, ceasing connection")
		// Kill connection
		closeRPC(altNode.Predecessor)

		altNode.setPredecessor(nil, "checkPredecessor()")
	}
}

// CreateRing creates a ring with this node as its only member
func (altNode *Alernator) CreateRing(_ struct{}, _ *struct{}) error {
	altNode.setPredecessor(nil, "CreateRing()")
	var successor ExtNode
	successor.ID = altNode.ID
	successor.Address = altNode.Address
	altNode.setSuccessor(&successor, "CreateRing()")

	return nil
}

func (altNode *Alernator) autoCheckPredecessor() {
	for {
		altNode.checkPredecessor()
		time.Sleep(heartbeatTime * time.Millisecond)
	}
}

// AutoStabilize runs Stabilize() on timed intervals
func (altNode *Alernator) autoStabilize() {
	for {
		altNode.stabilize()
		time.Sleep(stableTime * time.Millisecond)
	}
}

// InitNode initializes and alternode
func InitNode(port string, address string) {
	// Init connection map
	ClientMap = make(map[string]*rpc.Client)

	// Register node as RPC server
	node := new(Alernator)
	rpc.Register(node)
	rpc.HandleHTTP()
	l, err := net.Listen("tcp", ":"+port)
	checkErr("listen error ", err)
	// Get port selected by server
	port = strings.Split(l.Addr().String(), ":")[3]

	// Initialize Alernator fields
	node.Address = "127.0.0.1:" + port
	node.Port = port
	node.ID = genID(port)
	node.initFingers()
	node.initDB()

	// Join a ring if address is specified
	if address != "" {
		// We do not know the ID of the node connecting to, use stand-in
		var k Key
		broker := ExtNode{k, address}
		node.Join(&broker, nil)
	} else { // Else make a new ring
		var s struct{}
		node.CreateRing(s, &s)
	}
	go node.autoCheckPredecessor()
	go node.autoStabilize()
	go node.autoUpdateFingers()
	fmt.Println("Listening on port " + port)
	http.Serve(l, nil)
}

// Very annoying, will be using strings as IDs instead of a byte slice, since slices
// cannot be used as a key in a map. Still, IDs are simply sha1sums of the address.
func genID(port string) Key {
	hostname, _ := os.Hostname()
	h := sha1.New()
	rand.Seed(initSeed)
	io.WriteString(h, hostname+port+strconv.Itoa(rand.Int()))
	return sliceToKey(h.Sum(nil))
}
