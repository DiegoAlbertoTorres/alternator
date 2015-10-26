package main

import (
	"crypto/sha1"
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
)

// All in milliseconds
const stableTime = 1000
const heartbeatTime = 1000
const heartbeatTimeout = 400

// AlterNode is a node in Alternator
type AlterNode struct {
	ID          string
	Address     string
	Successor   *ExtNode
	Predecessor *ExtNode
	// Fingers is a sorted list (by ID) for nodes
	Fingers []*ExtNode
}

// ClientMap is a map of addresses to rpc clients
var ClientMap map[string]*rpc.Client

// ExtNode is an external, non-local node
type ExtNode struct {
	ID      string
	Address string
}

// Checks if test is in the range (a, b)
func inRange(test string, a string, b string) bool {
	if (test > a) && (test < b) {
		return true
	}
	return false
}

/* ExtNode methods */

func (extNode ExtNode) String() (str string) {
	str += "ID:" + extNode.ID + "\n"
	str += "Address:" + extNode.Address + "\n"
	return
}

func extNodeCopy(src *ExtNode, dst *ExtNode) {
	dst.ID = src.ID
	dst.Address = src.Address
}

/* AlterNode methods */

// GetSuccessor sets ret to the successor of an alternode
func (altNode *AlterNode) GetSuccessor(_ struct{}, ret *ExtNode) error {
	extNodeCopy(altNode.Successor, ret)
	return nil
}

// GetPredecessor sets ret to the predecessor of an alternode
func (altNode *AlterNode) GetPredecessor(_ struct{}, ret *ExtNode) error {
	extNodeCopy(altNode.Predecessor, ret)
	return nil
}

// Join joins a node into an existing ring
func (altNode *AlterNode) Join(broker *ExtNode, _ *struct{}) error {
	altNode.Predecessor = nil
	makeRemoteCall(broker, "FindSuccessor", altNode.ID, &altNode.Successor)

	fmt.Println("Joined ring")
	fmt.Println(altNode.string())
	return nil
}

func (altNode AlterNode) string() (str string) {
	str += "ID: " + altNode.ID + "\n"
	if altNode.Successor != nil {
		str += "Successor: " + altNode.Successor.ID + "\n"
	} else {
		str += "Successor: nil" + "\n"
	}

	if altNode.Predecessor != nil {
		str += "Predecessor: " + altNode.Predecessor.ID + "\n"
	} else {
		str += "Predecessor: nil" + "\n"
	}
	return
}

// FindSuccessor finds the successor of a key in the ring
func (altNode *AlterNode) FindSuccessor(key string, ret *ExtNode) error {
	fmt.Println("Finding successor of: " + key)
	// Find ID of successor
	for i, node := range altNode.Fingers {
		fmt.Println("Going over: " + node.ID)
		if node.ID > key {
			// Reply with external node
			ret.ID = altNode.Fingers[i].ID
			ret.Address = altNode.Fingers[i].ID
			return nil
		}
	}
	// Nothing bigger in circle, successor is first node
	ret.ID = altNode.Fingers[0].ID
	ret.Address = altNode.Fingers[0].Address
	return nil
}

// stabilize fixes the successors periodically
func (altNode *AlterNode) stabilize() {
	var temp ExtNode
	makeRemoteCall(altNode.Successor, "GetPredecessor", nil, &temp)
	if temp.ID > altNode.ID && temp.ID < altNode.Successor.ID {
		altNode.Successor = &temp
	}
	makeRemoteCall(altNode.Successor, "Notify", nil, nil)
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
		altNode.Predecessor = candidate
	}
	return nil
}

// Heartbeat returns an 'OK' to the caller
func (altNode *AlterNode) Heartbeat(_ struct{}, ret *string) error {
	*ret = "OK"
	return nil
}

// checkPredecessor checks if the predecessor has failed
func (altNode *AlterNode) checkPredecessor() {
	if altNode.Predecessor == nil {
		return
	}

	// Ping
	c := make(chan error, 1)
	beat := ""
	go func() {
		c <- makeRemoteCall(altNode.Predecessor, "Heartbeat", nil, &beat)
	}()

	select {
	case err := <-c:
		// Something wrong
		if (err != nil) || (beat != "OK") {
			altNode.Predecessor = nil
			// Kill connection
			ClientMap[altNode.Predecessor.Address].Close()
			delete(ClientMap, altNode.Predecessor.Address)
		}
	// Call timed out
	case <-time.After(heartbeatTimeout):
		altNode.Predecessor = nil
		// Kill connection
		ClientMap[altNode.Predecessor.Address].Close()
		delete(ClientMap, altNode.Predecessor.Address)
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
	altNode.Predecessor = nil
	var successor ExtNode
	successor.ID = altNode.ID
	successor.Address = altNode.Address
	altNode.Successor = &successor

	fmt.Println("Created ring:")
	fmt.Println(altNode.string())

	return nil
}

func (altNode *AlterNode) initFingers() {
	selfExt := ExtNode{altNode.ID, altNode.Address}
	altNode.Fingers = append(altNode.Fingers, &selfExt)
	// fmt.Println("Fingers initialized: " + altNode.Fingers[0].String())
}

func makeRemoteCall(callee *ExtNode, call string, args interface{}, result interface{}) error {
	// Check if there is already a connection
	client, clientOpen := ClientMap[callee.Address]
	// Open if not
	if !clientOpen {
		client, err := rpc.DialHTTP("tcp", callee.Address)
		if err != nil {
			log.Fatal("dialing:", err)
		}
		ClientMap[callee.Address] = client
	}
	err := client.Call("AlterNode."+call, args, result)

	return err
}

// Very annoying, will be using strings as IDs instead of a byte slice, since slices
// cannot be used as a key in a map. Still, IDs are simply sha1sums of the address.
func genID() string {
	hostname, _ := os.Hostname()
	h := sha1.New()
	rand.Seed(time.Now().UTC().UnixNano())
	io.WriteString(h, hostname+strconv.Itoa(rand.Int()))
	return fmt.Sprintf("%x", h.Sum(nil))
}

// Init this node
func initNode(address string) {
	// Register node
	node := new(AlterNode)
	rpc.Register(node)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", ":0")
	if e != nil {
		log.Fatal("listen error:", e)
	}
	port := strings.Split(l.Addr().String(), ":")[3]

	node.ID = genID()
	node.Address = "127.0.0.1:" + port
	node.initFingers()

	if address != "" {
		broker := ExtNode{"0", address}
		node.Join(&broker, nil)
	} else {
		var s struct{}
		node.CreateRing(s, &s)
	}

	go node.autoCheckPredecessor()
	go node.autoStabilize()
	fmt.Println("Listening on port " + port)
	http.Serve(l, nil)
}
