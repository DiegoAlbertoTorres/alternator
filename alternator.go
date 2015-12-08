// Package alternator implements a distributed, fault-tolerant hash table. Node nodes arrange
// themselves into a ring, where each node maintains a complete membership list, which is kept
// up-to-date by distributing membership changes (as a history) by a gossip algorithm. Node's
// main feature is that the nodes that replicate any given entry in the DHT can be selected
// arbitrarily, allowing users to localize data in nodes that have features best suited for
// processing or storing that data.
package alternator

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"sync"

	// _ "net/http/pprof" // Uncomment for profiling
	"net/rpc"
	"os"
	"os/signal"
	"runtime"
	// "runtime/pprof"
	"strings"
	"time"
)

// Config stores a node's configuration settings.
type Config struct {
	// Whether output uses complete keys or abbreviations.
	FullKeys bool
	// Time (in ms) between history syncs with a random peer.
	MemberSyncTime int
	// Time (in ms) between heartbeats.
	HeartbeatTime int
	// ResolvePendingTime is the time (in ms) between attempts to resolve pending put operations.
	ResolvePendingTime int
	// Time (in ms) before a heartbeat times out.
	HeartbeatTimeout int
	// PutMDTimeout is the time (in ms) before a PutMD times out.
	PutMDTimeout int
	// PutDataTimeout is the time (in ms) before a PutData times out.
	PutDataTimeout int
	// N is the number of nodes in which metadata is replicated.
	N int
	// Directory for alternator's data.
	DotPath string
	// CPUProfile is true if profiling is enabled.
	CPUProfile bool
}

// To avoid passing to methods not belonging to altNode
var fullKeys = false

// Node is a member of the DHT. All methods belonging to Node that are exported are also accessible
// through RPC (hence the arguments to exported methods are always those required by the rpc
// package)
type Node struct {
	ID          Key
	Address     string
	Port        string
	Members     Members
	MemberHist  history
	DB          *DB
	Config      Config
	RPCListener net.Listener
}

// InitNode initializes a new node (constructor).
func InitNode(conf Config, port string, address string) {
	// Register node as RPC server
	node := new(Node)
	rpc.Register(node)
	rpc.HandleHTTP()
	l, err := net.Listen("tcp", ":"+port)
	checkErr("listen error ", err)
	// Get port selected by server
	port = strings.Split(l.Addr().String(), ":")[3]

	// Initialize Node fields
	node.Address = "127.0.0.1:" + port
	node.Port = port
	node.ID = GenID(port)
	node.RPCListener = l
	node.Config = conf
	node.Members.Init()
	node.initDB()

	if node.Config.FullKeys {
		fullKeys = true
	}

	// Join a ring if address is specified
	if address != "" {
		// We do not know the ID of the node connecting to, use stand-in
		var k Key
		broker := Peer{ID: k, Address: address}
		err = node.joinRing(&broker)
		if err != nil {
			log.Print("Failed to join ring: ", err)
			os.Exit(1)
		}
	} else { // Else make a new ring
		node.createRing()
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go node.autoCheckPredecessor()
	go node.autoSyncMembers()
	go node.autoResolvePending()
	go node.sigHandler()

	if node.Config.CPUProfile {
		runtime.SetBlockProfileRate(1)
		go func() {
			fmt.Println("starting server")
			log.Println(http.ListenAndServe("localhost:6060", nil))
		}()
	}

	fmt.Println(node.string())
	fmt.Println("Listening on port " + port)
	http.Serve(l, nil)
	// TODO: undo hackyness
	wg.Wait()
	return
}

/* Ring join functions */

// JoinRequestArgs is the set of return parameters to a JoinRequest
type JoinRequestArgs struct {
	Keys []Key
	Vals [][]byte
}

// JoinRequest handles a request by another node to join the ring
func (altNode *Node) JoinRequest(other *Peer, ret *JoinRequestArgs) error {
	// Find pairs in joiner's range
	keys, vals := altNode.DB.getMDRange(altNode.ID, other.ID)
	// fmt.Printf("giving pairs in range %s to %s\n", keyToString(altNode.ID), keyToString(other.ID))
	for i := range keys {
		fmt.Println(keys[i])
	}

	// Add join to history
	newEntry := histEntry{Time: time.Now(), Class: histJoin, Node: *other}
	altNode.insertToHistory(newEntry)
	altNode.Members.Insert(other)
	fmt.Println("Members changed:")
	fmt.Println(altNode.Members)
	ret.Keys = keys
	ret.Vals = vals
	return nil
}

// Join joins a node into an existing ring
func (altNode *Node) joinRing(broker *Peer) error {
	var successor Peer
	// Find future successor using some broker in ring
	err := MakeRemoteCall(broker, "FindSuccessor", altNode.ID, &successor)
	if err != nil {
		return ErrJoinFail
	}

	// Do join through future successor
	var kvPairs JoinRequestArgs
	err = MakeRemoteCall(&successor, "JoinRequest", altNode.selfExt(), &kvPairs)
	if err != nil {
		return ErrJoinFail
	}

	// Synchronize history with successor
	altNode.syncMembers(&successor)

	fmt.Println("Successfully joined ring")
	return nil
}

/* Ring leave functions */

// LeaveRequestArgs holds arguments for a leave request
type LeaveRequestArgs struct {
	Keys           []Key
	Vals           [][]byte
	DepartureEntry histEntry
}

// LeaveRequest handles a leave request. It appends the departure entry to the node's history.
func (altNode *Node) LeaveRequest(args *LeaveRequestArgs, _ *struct{}) error {
	// Insert received keys
	batchArgs := BatchPutArgs{metaDataBucket, args.Keys, args.Vals}
	altNode.BatchPut(batchArgs, &struct{}{})
	altNode.insertToHistory(args.DepartureEntry)
	altNode.Members.Remove(&args.DepartureEntry.Node)
	fmt.Println("Members changed:")
	fmt.Println(altNode.Members)

	// Replicate in one more
	err := MakeRemoteCall(altNode.getNthSuccessor(altNode.Config.N-1), "BatchPut", batchArgs, &struct{}{})
	checkErr("Unhandled error", err)
	return nil
}

// LeaveRing makes the node leave the ring to which it belongs
func (altNode *Node) leaveRing() error {
	// Stop accepting RPC calls
	altNode.RPCListener.Close()

	// RePut each entry
	altNode.rePutAllData()

	successor := altNode.getSuccessor()
	if *successor == altNode.selfExt() {
		os.Exit(0)
		return nil
	}

	// Hand keys to successor
	mdKeys, mdVals := altNode.DB.getMDRange(altNode.getPredecessor().ID, altNode.ID) // Gather entries
	departureEntry := histEntry{Time: time.Now(), Class: histLeave, Node: altNode.selfExt()}
	args := LeaveRequestArgs{mdKeys, mdVals, departureEntry}

	var err error
	// Leave by notifying successor
	err = MakeRemoteCall(successor, "LeaveRequest", &args, &struct{}{})
	if err != nil {
		return ErrLeaveFail
	}
	altNode.DB.close()
	os.Exit(0)
	return nil
}

/* Ring maintenance and stabilization functions */

// createRing creates a ring with this node as its only member
func (altNode *Node) createRing() {
	// Add own join to ring
	self := altNode.selfExt()
	altNode.insertToHistory(histEntry{Time: time.Now(), Class: histJoin, Node: self})
	altNode.Members.Insert(&self)
}

func (altNode *Node) syncMembers(peer *Peer) {
	if changes := altNode.syncMemberHist(peer); changes {
		altNode.rebuildMembers()
		fmt.Println("Members changed:")
		fmt.Println(altNode.Members)
	}
}

// Heartbeat returns an 'OK' to the caller
func (altNode *Node) Heartbeat(_ struct{}, ret *string) error {
	// fmt.Println("Heartbeat called!")
	*ret = "OK"
	return nil
}

// checkPredecessor checks if the predecessor has failed
func (altNode *Node) checkPredecessor() {
	predecessor := altNode.getPredecessor()
	if predecessor.ID.Compare(altNode.ID) == 0 {
		return
	}
	// Ping
	c := make(chan error, 1)
	beat := ""
	go func() {
		c <- MakeRemoteCall(predecessor, "Heartbeat", struct{}{}, &beat)
	}()

	// TODO: do something smart with heartbeat failures to autodetect departures
	select {
	case err := <-c:
		// Something wrong
		if (err != nil) || (beat != "OK") {
			RPCClose(predecessor)
		}
	case <-time.After(time.Duration(altNode.Config.HeartbeatTimeout) * time.Millisecond):
		// Call timed out
		fmt.Println("Predecessor stopped responding, ceasing connection")
		RPCClose(predecessor)
	}
}

/* Ring lookup */

// FindSuccessor sets 'ret' to the successor of a key 'k' in the ring
func (altNode *Node) FindSuccessor(k Key, ret *Peer) error {
	succ := altNode.Members.FindSuccessor(k)
	*ret = *getPeer(succ)
	return nil
}

func (altNode *Node) rebuildMembers() {
	var newMembers Members
	newMembers.Init()
	for _, entry := range altNode.MemberHist {
		switch entry.Class {
		case histJoin:
			var copy Peer
			copy = entry.Node
			newMembers.Insert(&copy)
		case histLeave:
			newMembers.Remove(&entry.Node)
		}
	}
	altNode.Members = newMembers
}

/* Background task functions */

// autoSyncMembers automatically syncs members with a random node
func (altNode *Node) autoSyncMembers() {
	for {
		random := altNode.Members.GetRandom()
		altNode.syncMembers(random)
		// altNode.printHist()
		time.Sleep(time.Duration(altNode.Config.MemberSyncTime) * time.Millisecond)
	}
}

func (altNode *Node) autoCheckPredecessor() {
	for {
		altNode.checkPredecessor()
		time.Sleep(time.Duration(altNode.Config.HeartbeatTime) * time.Millisecond)
	}
}

/* Utility functions, not exported */

// syncMemberHist synchronizes the node's member history with an peer node
func (altNode *Node) syncMemberHist(peer *Peer) bool {
	if peer == nil {
		return false
	}
	var peerHist history

	err := MakeRemoteCall(peer, "GetMemberHist", struct{}{}, &peerHist)
	if err != nil {
		return false
	}

	var changes bool
	altNode.MemberHist, changes = mergeHistories(altNode.MemberHist, peerHist)
	return changes
}

// insertToHistory inserts an entry to the node's history
func (altNode *Node) insertToHistory(entry histEntry) {
	altNode.MemberHist.InsertEntry(entry)
}

// sigHandler catches signals sent to the node
func (altNode *Node) sigHandler() {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, os.Kill)
	for {
		sig := <-sigChan
		switch sig {
		case os.Interrupt:
			altNode.leaveRing()
		case os.Kill:
			os.Exit(1)
		}
	}
}

/* Getters */

// GetMembers returns an array of peers with all the members in the ring
func (altNode *Node) GetMembers(_ struct{}, ret *[]Peer) error {
	var members []Peer
	for current := altNode.Members.List.Front(); current != nil; current = current.Next() {
		members = append(members, *getPeer(current))
	}
	*ret = members
	return nil
}

// GetMemberHist returns the node's membership history
func (altNode *Node) GetMemberHist(_ struct{}, ret *[]histEntry) error {
	*ret = altNode.MemberHist
	return nil
}

func (altNode *Node) getSuccessor() *Peer {
	succElt := altNode.Members.Map[altNode.ID]
	succElt = succElt.Next()
	if succElt == nil {
		succElt = altNode.Members.List.Front()
	}
	return getPeer(succElt)
}

func (altNode *Node) getPredecessor() *Peer {
	predElt := altNode.Members.Map[altNode.ID]
	predElt = predElt.Prev()
	if predElt == nil {
		predElt = altNode.Members.List.Back()
	}
	return getPeer(predElt)
}

func (altNode *Node) getNthSuccessor(n int) *Peer {
	// var current *list.Element
	current := altNode.Members.Map[altNode.ID]
	for i := 0; i < n; i++ {
		current = current.Next()
		if current == nil {
			current = altNode.Members.List.Front()
		}
	}
	if current == nil {
		current = altNode.Members.List.Front()
	}
	return getPeer(current)
}

func (altNode *Node) getNthPredecessor(n int) *Peer {
	current := altNode.Members.Map[altNode.ID]
	for i := 0; i < n; i++ {
		current = current.Prev()
		if current == nil {
			current = altNode.Members.List.Back()
		}
	}
	if current == nil {
		current = altNode.Members.List.Back()
	}
	return getPeer(current)
}

func (altNode Node) string() (str string) {
	str += "ID: " + altNode.ID.String() + "\n"
	return
}

// selfExt returns an peerNode equivalent of altNode
func (altNode *Node) selfExt() Peer {
	return Peer{ID: altNode.ID, Address: altNode.Address}
}
