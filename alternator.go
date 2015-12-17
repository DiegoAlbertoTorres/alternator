// Package alternator implements a distributed, fault-tolerant key-value store. Node nodes arrange
// themselves into a ring, where each node maintains a complete membership list, which is kept
// up-to-date by distributing membership changes (as a history) by a gossip algorithm. Alternator's
// main feature is that the user can choose arbitrarily the nodes in the system that will replicate
// any given entry. This gives the user full control of the data-flow in the system.
package alternator

import (
	"container/list"
	"fmt"
	"log"
	"net"
	"net/http"
	"runtime"
	"strings"
	// For profile
	_ "net/http/pprof"
	"net/rpc"
	"os"
	"os/signal"
	"sync"
	"time"
)

// Config stores a node's configuration settings.
type Config struct {
	// FullKeys, when true, makes all console output print complete keys. Otherwise, abbreviations
	// are used.
	FullKeys bool
	// MemberSyncTime is the time (in ms) between history syncs with a random peer.
	MemberSyncTime int
	// HeartbeatTime is the time (in ms) between heartbeats.
	HeartbeatTime int
	// ResolvePendingTime is the time (in ms) between attempts to resolve pending put operations.
	ResolvePendingTime int
	// HeatbeatTimeout is the time (in ms) before a heartbeat times out.
	HeartbeatTimeout int
	// PutMDTimeout is the time (in ms) before a PutMD times out.
	PutMDTimeout int
	// PutDataTimeout is the time (in ms) before a PutData times out.
	PutDataTimeout int
	// N is the number of nodes in which metadata is replicated (size of the replication chain).
	N int
	// Directory for alternator's data, including database files.
	DotPath string
	// CPUProfile enables profiling when set to true.
	CPUProfile bool
}

// To avoid passing to methods not belonging to altNode
var fullKeys = false

// Node is a member of the DHT. All methods belonging to Node that are exported are also accessible
// through RPC (hence the arguments to exported methods are always those required by the rpc
// package)
type Node struct {
	ID           Key
	Address      string
	Port         string
	Members      Members
	memberHist   history
	historyMutex sync.RWMutex
	DB           *dB
	Config       Config
	RPCListener  net.Listener
	rpcServ      RPCService
}

// CreateNode creates a new node, initializes all of its fields, and exposes its
// exported methods through RPC.
func CreateNode(conf Config, port string, address string) {
	// Register node as RPC server
	node := new(Node)
	rpc.Register(node)
	rpc.HandleHTTP()
	l, err := net.Listen("tcp", ":"+port)
	checkErr("listen error ", err)
	// Get port selected by server
	addrstr := l.Addr().String()
	colon := strings.LastIndex(addrstr, ":")
	port = addrstr[colon+1:]

	// Initialize Node fields
	node.Address = getIP() + ":" + port
	node.Port = port
	node.ID = GenID(node.Address)
	node.RPCListener = l
	node.Config = conf
	node.Members.init()
	node.initDB()
	node.rpcServ.Init()

	if conf.CPUProfile {
		go func() {
			log.Println(http.ListenAndServe("localhost:6060", nil))
		}()
	}

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
		runtime.SetCPUProfileRate(10)
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

func (altNode *Node) shutdown() {
	altNode.DB.close()
	fmt.Println("Goodbye!")
	os.Exit(0)
}

/* Ring join functions */

// JoinRequestResp is the set of return parameters to a JoinRequest. The response to a
// join request contains a set of key-value pairs that should be inserted by the new node into
// its metadata database.
type JoinRequestResp struct {
	Keys []Key
	Vals [][]byte
}

// JoinRequest handles a request by another node to join the ring.
func (altNode *Node) JoinRequest(other *Peer, response *JoinRequestResp) error {
	// Find pairs in joiner's range
	keys, vals := altNode.DB.getMDRange(altNode.getNthPredecessor(1).ID, other.ID)
	fmt.Printf("Giving pairs in range %v to %v\n", altNode.getNthPredecessor(1).ID, other.ID)

	// Add join to history
	newEntry := histEntry{Time: time.Now(), Class: histJoin, Node: *other}
	altNode.insertToHistory(newEntry)

	altNode.Members.Lock()
	altNode.Members.insert(other)
	altNode.Members.Unlock()

	fmt.Println("Members changed:")
	altNode.Members.RLock()
	fmt.Println(altNode.Members)
	altNode.Members.RUnlock()

	response.Keys = keys
	response.Vals = vals
	return nil
}

// joinRing joins a node into the ring to which 'broker' belongs.
func (altNode *Node) joinRing(broker *Peer) error {
	var successor Peer
	// Find future successor using some broker in ring
	err := altNode.rpcServ.MakeRemoteCall(broker, "FindSuccessor", altNode.ID, &successor)
	if err != nil {
		return ErrJoinFail
	}

	// Do join through future successor
	var kvPairs JoinRequestResp
	err = altNode.rpcServ.MakeRemoteCall(&successor, "JoinRequest", altNode.selfExt(), &kvPairs)
	if err != nil {
		return ErrJoinFail
	}

	// Store keys received from successor
	altNode.DB.batchPut(metaDataBucket, &kvPairs.Keys, &kvPairs.Vals)

	// Synchronize history with successor
	altNode.syncMembers(&successor)

	fmt.Println("Successfully joined ring")
	return nil
}

/* Ring leave functions */

// LeaveRequestArgs holds arguments for a leave request
type LeaveRequestArgs struct {
	Keys   []Key
	Vals   [][]byte
	Leaver Peer
}

// LeaveRequest handles a leave request. It appends the departure entry to the node's history.
func (altNode *Node) LeaveRequest(args *LeaveRequestArgs, _ *struct{}) error {
	// Insert received keys
	batchArgs := BatchPutArgs{metaDataBucket, args.Keys, args.Vals}
	altNode.BatchPut(batchArgs, &struct{}{})

	departureEntry := histEntry{Time: time.Now(), Class: histLeave, Node: args.Leaver}
	altNode.insertToHistory(departureEntry)

	altNode.Members.Lock()
	altNode.Members.remove(&args.Leaver)
	altNode.Members.Unlock()

	fmt.Println("Members changed:")
	altNode.Members.RLock()
	fmt.Println(altNode.Members)
	altNode.Members.RUnlock()

	// Replicate in one more
	err := altNode.rpcServ.MakeRemoteCall(altNode.getNthSuccessor(altNode.Config.N-1), "BatchPut", batchArgs, &struct{}{})
	checkErr("Unhandled error", err)
	return nil
}

// LeaveRing makes the node leave the ring to which it belongs.
func (altNode *Node) leaveRing() error {
	// Stop accepting RPC calls
	altNode.RPCListener.Close()

	// RePut each entry into the ring
	altNode.rePutAllData()

	successor := altNode.getSuccessor()
	if *successor == altNode.selfExt() {
		os.Exit(0)
		return nil
	}

	// Hand keys to successor
	mdKeys, mdVals := altNode.DB.getMDRange(altNode.getPredecessor().ID, altNode.ID) // Gather entries
	args := LeaveRequestArgs{mdKeys, mdVals, altNode.selfExt()}

	// Leave by notifying successor
	err := altNode.rpcServ.MakeRemoteCall(successor, "LeaveRequest", &args, &struct{}{})
	if err != nil {
		return ErrLeaveFail
	}
	altNode.shutdown()
	return nil
}

/* Ring maintenance and stabilization functions */

// createRing creates a ring with this node as its only member.
func (altNode *Node) createRing() {
	// Add own join to ring
	self := altNode.selfExt()
	altNode.insertToHistory(histEntry{Time: time.Now(), Class: histJoin, Node: self})

	altNode.Members.Lock()
	altNode.Members.insert(&self)
	altNode.Members.Unlock()
}

// syncMembers synchronizes the membership list with a peer by synchronizing histories
// and then rebuilding the list if necessary.
func (altNode *Node) syncMembers(peer *Peer) {
	if changes := altNode.syncMemberHist(peer); changes {
		altNode.rebuildMembers()
		fmt.Println("Members changed:")
		altNode.Members.RLock()
		fmt.Println(altNode.Members)
		altNode.Members.RUnlock()
	}
}

// Heartbeat returns an 'OK' to the caller.
func (altNode *Node) Heartbeat(_ struct{}, ret *string) error {
	*ret = "OK"
	return nil
}

// checkPredecessor checks if the predecessor has failed. If the predecessor has
// failed then it closes the RPC connection.
func (altNode *Node) checkPredecessor() {
	predecessor := altNode.getPredecessor()
	if predecessor.ID.Compare(altNode.ID) == 0 {
		return
	}
	// Ping
	c := make(chan error, 1)
	beat := ""
	go func() {
		c <- altNode.rpcServ.MakeRemoteCall(predecessor, "Heartbeat", struct{}{}, &beat)
	}()

	// TODO: do something smart with heartbeat failures to autodetect departures
	select {
	case err := <-c:
		// Something wrong
		if (err != nil) || (beat != "OK") {
			altNode.rpcServ.CloseIfBad(err, predecessor)
		}
	case <-time.After(time.Duration(altNode.Config.HeartbeatTimeout) * time.Millisecond):
		// Call timed out
		// fmt.Println("Predecessor stopped responding, ceasing connection")
	}
}

/* Ring lookup */

// FindSuccessor sets 'succ' to the successor of the key 'k'.
func (altNode *Node) FindSuccessor(k Key, succ *Peer) error {
	altNode.Members.RLock()
	*succ = *getPeer(altNode.Members.findSuccessor(k))
	altNode.Members.RUnlock()
	return nil
}

// fingListSuccessor is similar to 'FindSuccessor', except that it returns
// a list element in altNode.Members.List, instead of the value of the
// element.
func (altNode *Node) findListSuccessor(k Key) *list.Element {
	altNode.Members.RLock()
	succ := altNode.Members.findSuccessor(k)
	altNode.Members.RUnlock()

	return succ
}

// rebuildMembers rebuilds the membership list of the node from the node's history.
func (altNode *Node) rebuildMembers() {
	var newMembers Members
	newMembers.init()
	for _, entry := range altNode.memberHist {
		switch entry.Class {
		case histJoin:
			copy := entry.Node
			newMembers.insert(&copy)
		case histLeave:
			newMembers.remove(&entry.Node)
		}
	}
	altNode.Members.Lock()
	oldMembers := altNode.Members
	altNode.Members = newMembers
	// Unlock the old, the new are unlocked from creation
	oldMembers.Unlock()
}

/* Membership maintenance functions */

// syncMemberHist synchronizes the node's member history with a peer node
func (altNode *Node) syncMemberHist(peer *Peer) bool {
	if peer == nil {
		return false
	}
	var peerHist history

	err := altNode.rpcServ.MakeRemoteCall(peer, "GetMemberHist", struct{}{}, &peerHist)
	if err != nil {
		return false
	}

	merged, changes := mergeHistories(altNode.memberHist, peerHist)
	altNode.historyMutex.Lock()
	altNode.memberHist = merged
	altNode.historyMutex.Unlock()
	return changes
}

// insertToHistory inserts an entry to the node's history
func (altNode *Node) insertToHistory(entry histEntry) {
	altNode.historyMutex.Lock()
	altNode.memberHist.InsertEntry(entry)
	altNode.historyMutex.Unlock()
}

/* Background task functions */

// autoSyncMembers syncs the membership list with a random peer at an interval.
func (altNode *Node) autoSyncMembers() {
	for {
		altNode.Members.RLock()
		random := altNode.Members.getRandom()
		altNode.Members.RUnlock()

		altNode.syncMembers(random)
		// altNode.printHist()
		time.Sleep(time.Duration(altNode.Config.MemberSyncTime) * time.Millisecond)
	}
}

// autoCheckPredecessor calls checkPredecessor at a time interval.
func (altNode *Node) autoCheckPredecessor() {
	for {
		altNode.checkPredecessor()
		time.Sleep(time.Duration(altNode.Config.HeartbeatTime) * time.Millisecond)
	}
}

/* Getters */

// GetMembers returns an array of peers with all the members in the ring through 'ret'.
func (altNode *Node) GetMembers(_ struct{}, ret *[]Peer) error {
	var members []Peer
	altNode.Members.RLock()
	for current := altNode.Members.List.Front(); current != nil; current = current.Next() {
		members = append(members, *getPeer(current))
	}
	altNode.Members.RUnlock()
	*ret = members
	return nil
}

// GetMemberHist returns the node's membership history through 'ret'.
func (altNode *Node) GetMemberHist(_ struct{}, ret *[]histEntry) error {
	altNode.historyMutex.RLock()
	*ret = altNode.memberHist
	altNode.historyMutex.RUnlock()
	return nil
}

// getSuccessor returns the successor of the node.
func (altNode *Node) getSuccessor() *Peer {
	altNode.Members.RLock()
	succElt := altNode.Members.Map[altNode.ID]
	succElt = succElt.Next()
	if succElt == nil {
		succElt = altNode.Members.List.Front()
	}
	altNode.Members.RUnlock()
	return getPeer(succElt)
}

// getListSuccessor is just like getSuccesor, but returns list element.
// Useful because the list element can be used to iterate through the ring.
func (altNode *Node) getListSuccessor() *list.Element {
	altNode.Members.RLock()
	succElt := altNode.Members.Map[altNode.ID]
	succElt = succElt.Next()
	if succElt == nil {
		succElt = altNode.Members.List.Front()
	}
	altNode.Members.RUnlock()
	return succElt
}

// getPredecessor returns the predecessor of the node.
func (altNode *Node) getPredecessor() *Peer {
	altNode.Members.RLock()
	predElt := altNode.Members.Map[altNode.ID]
	predElt = predElt.Prev()
	if predElt == nil {
		predElt = altNode.Members.List.Back()
	}
	altNode.Members.RUnlock()
	return getPeer(predElt)
}

// getNthSuccessor returns the nth successor of the node.
func (altNode *Node) getNthSuccessor(n int) *Peer {
	altNode.Members.RLock()
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
	altNode.Members.RUnlock()
	return getPeer(current)
}

// getNthPredecessor returns the nth predecessor of the node.
func (altNode *Node) getNthPredecessor(n int) *Peer {
	altNode.Members.RLock()
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
	altNode.Members.RUnlock()
	return getPeer(current)
}

func (altNode *Node) string() (str string) {
	str += "ID: " + altNode.ID.String() + "\n"
	return
}

// selfExt returns an peerNode equivalent of altNode
func (altNode *Node) selfExt() Peer {
	return Peer{ID: altNode.ID, Address: altNode.Address}
}

/* Misc */

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
