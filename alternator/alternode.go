package main

import (
	"bytes"
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

var maxKey, _ = hex.DecodeString("ffffffffffffffffffffffffffffffffffffffff")
var minKey, _ = hex.DecodeString("0000000000000000000000000000000000000000")

// AlterNode is a node in Alternator
type AlterNode struct {
	ID          []byte
	Address     string
	Port        string
	Successor   *ExtNode
	Predecessor *ExtNode
	Fingers     Fingers
	DB          *bolt.DB
}

// ClientMap is a map of addresses to rpc clients
var ClientMap map[string]*rpc.Client

// GetFingers sets ret to this node's fingers
func (altNode *AlterNode) GetFingers(_ struct{}, ret *[]*ExtNode) error {
	*ret = altNode.Fingers.Slice
	return nil
}

/* Utility functions */

func checkError(err error, msg string) {
	if err != nil {
		log.Fatal(msg+": ", err)
	}
}

// Checks if test is in the range (from, to) (non-inclusive)
func inRange(test []byte, from []byte, to []byte) bool {
	if bytes.Compare(from, to) < 0 {
		return (bytes.Compare(test, from) > 0) && (bytes.Compare(test, to) < 0)
	} else if bytes.Compare(from, to) > 0 {
		return ((bytes.Compare(test, from) > 0) && (bytes.Compare(test, maxKey) <= 0)) ||
			((bytes.Compare(test, to) < 0) && (bytes.Compare(test, minKey) >= 0))
	} else {
		return (bytes.Compare(test, from) != 0)
	}
}

/* AlterNode methods */
func (altNode *AlterNode) setSuccessor(new *ExtNode, caller string) {
	altNode.Successor = new
	fmt.Println(caller + " changed successor:")
	fmt.Println(altNode.string())
}

func (altNode *AlterNode) setPredecessor(new *ExtNode, caller string) {
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
func (altNode *AlterNode) GetSuccessor(_ struct{}, ret *ExtNode) error {
	extNodeCopy(altNode.Successor, ret)
	return nil
}

// GetPredecessor sets ret to the predecessor of an alternode
func (altNode *AlterNode) GetPredecessor(_ struct{}, ret *ExtNode) error {
	if altNode.Predecessor == nil {
		return errors.New(ErrNilPredecessor)
	}
	extNodeCopy(altNode.Predecessor, ret)
	return nil
}

// Join joins a node into an existing ring
func (altNode *AlterNode) Join(broker *ExtNode, _ *struct{}) error {
	altNode.setPredecessor(nil, "Join()")
	makeRemoteCall(broker, "FindSuccessor", altNode.ID, &altNode.Successor)

	fmt.Println("Joined ring")
	fmt.Println(altNode.string())
	return nil
}

func (altNode AlterNode) string() (str string) {
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
func (altNode *AlterNode) FindSuccessor(key []byte, ret *ExtNode) error {
	succ, err := altNode.Fingers.FindSuccessor(key)
	*ret = *succ
	return err
}

// stabilize fixes the successors periodically
func (altNode *AlterNode) stabilize() {
	// fmt.Println("Stabilizing")

	var temp ExtNode
	err := makeRemoteCall(altNode.Successor, "GetPredecessor", struct{}{}, &temp)
	if err != nil {
		log.Print("GetPredecessor error ", err)
		// return
	}

	if err == nil && inRange(temp.ID, altNode.ID, altNode.Successor.ID) {
		fmt.Println("setting successor to", temp)
		altNode.setSuccessor(&temp, "stabilize():")
	}

	selfExt := ExtNode{altNode.ID, altNode.Address}
	makeRemoteCall(altNode.Successor, "Notify", selfExt, nil)
}

// AutoStabilize runs Stabilize() on timed intervals
func (altNode *AlterNode) autoStabilize() {
	for {
		altNode.stabilize()
		time.Sleep(stableTime * time.Millisecond)
	}
}

// Notify is called by another node when it thinks it might be our predecessor
func (altNode *AlterNode) Notify(candidate *ExtNode, _ *struct{}) error {
	// Should update fingers accordingly?
	if (altNode.Predecessor == nil) || (inRange(candidate.ID, altNode.Predecessor.ID, altNode.ID)) {
		altNode.setPredecessor(candidate, "Notify()")
	}
	return nil
}

// Heartbeat returns an 'OK' to the caller
func (altNode *AlterNode) Heartbeat(_ struct{}, ret *string) error {
	// fmt.Println("Heartbeat called!")
	*ret = "OK"
	return nil
}

// checkPredecessor checks if the predecessor has failed
func (altNode *AlterNode) checkPredecessor() {
	if (altNode.Predecessor == nil) || (bytes.Compare(altNode.Predecessor.ID, altNode.ID) == 0) {
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

			altNode.setPredecessor(nil, "checkPredecessor()")
		}
		// Call timed out
	case <-time.After(heartbeatTimeout * time.Millisecond):
		fmt.Println("Successor stopped responding, ceasing connection")
		// Kill connection
		closeRPC(altNode.Predecessor)

		altNode.setPredecessor(nil, "checkPredecessor()")
	}
}

func closeRPC(node *ExtNode) {
	fmt.Println("Closing connection to " + node.Address)
	if node == nil {
		return
	}
	if client, exists := ClientMap[node.Address]; exists {
		client.Close()
		delete(ClientMap, node.Address)
	}
}

func (altNode *AlterNode) autoCheckPredecessor() {
	for {
		altNode.checkPredecessor()
		time.Sleep(heartbeatTime * time.Millisecond)
	}
}

// CreateRing creates a ring with this node as its only member
func (altNode *AlterNode) CreateRing(_ struct{}, _ *struct{}) error {
	altNode.setPredecessor(nil, "CreateRing()")
	var successor ExtNode
	successor.ID = altNode.ID
	successor.Address = altNode.Address
	altNode.setSuccessor(&successor, "CreateRing()")

	return nil
}

// makeRemoteCall calls a function at a remote node
func makeRemoteCall(callee *ExtNode, call string, args interface{}, result interface{}) error {
	// fmt.Println("calling " + call)
	// fmt.Println("Calling " + call + "() at " + callee.ID)
	// Check if there is already a connection
	var client *rpc.Client
	var clientOpen bool
	var err error
	client, clientOpen = ClientMap[callee.Address]
	// Open if not
	if !clientOpen {
		fmt.Println("Attempting connection to " + callee.Address)
		client, err = rpc.DialHTTP("tcp", callee.Address)
		if err != nil {
			// Client must be down, ignore
			log.Print("Client down? " + callee.Address)
			return nil
		}
		ClientMap[callee.Address] = client
	}
	err = client.Call("AlterNode."+call, args, result)

	return err
}

// Very annoying, will be using strings as IDs instead of a byte slice, since slices
// cannot be used as a key in a map. Still, IDs are simply sha1sums of the address.
func genID() []byte {
	hostname, _ := os.Hostname()
	h := sha1.New()
	rand.Seed(time.Now().UTC().UnixNano())
	io.WriteString(h, hostname+strconv.Itoa(rand.Int()))
	return h.Sum(nil)
}

// InitNode initializes and alternode
func InitNode(port string, address string) {
	// Init connection map
	ClientMap = make(map[string]*rpc.Client)

	// Register node as RPC server
	node := new(AlterNode)
	rpc.Register(node)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", ":"+port)
	checkError(e, "listen error")
	// Get port selected by server
	port = strings.Split(l.Addr().String(), ":")[3]

	// Initialize AlterNode fields
	node.ID = genID()
	node.Address = "127.0.0.1:" + port
	node.Port = port
	node.initFingers()
	node.initDB()

	// Join a ring if address is specified
	if address != "" {
		// We do not know the ID of the node connecting to, use stand-in
		broker := ExtNode{[]byte("a"), address}
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