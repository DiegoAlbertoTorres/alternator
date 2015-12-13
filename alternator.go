// Package alternator implements a distributed, fault-tolerant hash table. Node nodes arrange
// themselves into a ring, where each node maintains a complete membership list, which is kept
// up-to-date by distributing membership changes (as a history) by a gossip algorithm. Alternator's
// main feature is that the nodes that replicate any given entry in the DHT can be selected
// arbitrarily, allowing users to localize data in nodes that have features best suited for
// processing or storing that data.
package alternator

import (
	"container/list"
	"fmt"
	"log"
	"net"
	"net/http"
	// For profile
	_ "net/http/pprof"
	"net/rpc"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"sync"
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
	ID           Key
	Address      string
	Port         string
	Members      Members
	membersMutex sync.RWMutex
	MemberHist   history
	historyMutex sync.RWMutex
	DB           *dB
	Config       Config
	RPCListener  net.Listener
	rpcServ      RPCService
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

// JoinRequestRet is the set of return parameters to a JoinRequest
type JoinRequestRet struct {
	Keys []Key
	Vals [][]byte
}

// JoinRequest handles a request by another node to join the ring
func (altNode *Node) JoinRequest(other *Peer, ret *JoinRequestRet) error {
	// Find pairs in joiner's range
	keys, vals := altNode.DB.getMDRange(altNode.ID, other.ID)
	fmt.Printf("Giving pairs in range %v to %v\n", altNode.ID, other.ID)

	// Add join to history
	newEntry := histEntry{Time: time.Now(), Class: histJoin, Node: *other}
	altNode.insertToHistory(newEntry)

	altNode.membersMutex.Lock()
	altNode.Members.Insert(other)
	altNode.membersMutex.Unlock()

	fmt.Println("Members changed:")
	altNode.membersMutex.RLock()
	fmt.Println(altNode.Members)
	altNode.membersMutex.RUnlock()

	ret.Keys = keys
	ret.Vals = vals
	return nil
}

// Join joins a node into an existing ring
func (altNode *Node) joinRing(broker *Peer) error {
	var successor Peer
	// Find future successor using some broker in ring
	err := altNode.rpcServ.MakeRemoteCall(broker, "FindSuccessor", altNode.ID, &successor)
	if err != nil {
		return ErrJoinFail
	}

	// Do join through future successor
	var kvPairs JoinRequestRet
	err = altNode.rpcServ.MakeRemoteCall(&successor, "JoinRequest", altNode.selfExt(), &kvPairs)
	if err != nil {
		return ErrJoinFail
	}

	altNode.DB.batchPut(metaDataBucket, &kvPairs.Keys, &kvPairs.Vals)

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

	altNode.membersMutex.Lock()
	altNode.Members.Remove(&args.DepartureEntry.Node)
	altNode.membersMutex.Unlock()

	fmt.Println("Members changed:")
	altNode.membersMutex.RLock()
	fmt.Println(altNode.Members)
	altNode.membersMutex.RUnlock()

	// Replicate in one more
	err := altNode.rpcServ.MakeRemoteCall(altNode.getNthSuccessor(altNode.Config.N-1), "BatchPut", batchArgs, &struct{}{})
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
	err = altNode.rpcServ.MakeRemoteCall(successor, "LeaveRequest", &args, &struct{}{})
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

	altNode.membersMutex.Lock()
	altNode.Members.Insert(&self)
	altNode.membersMutex.Unlock()
}

func (altNode *Node) syncMembers(peer *Peer) {
	if changes := altNode.syncMemberHist(peer); changes {
		altNode.rebuildMembers()
		fmt.Println("Members changed:")
		altNode.membersMutex.RLock()
		fmt.Println(altNode.Members)
		altNode.membersMutex.RUnlock()
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
		c <- altNode.rpcServ.MakeRemoteCall(predecessor, "Heartbeat", struct{}{}, &beat)
	}()

	// TODO: do something smart with heartbeat failures to autodetect departures
	select {
	case err := <-c:
		// Something wrong
		if (err != nil) || (beat != "OK") {
			altNode.rpcServ.rpcClose(predecessor)
		}
	case <-time.After(time.Duration(altNode.Config.HeartbeatTimeout) * time.Millisecond):
		// Call timed out
		// fmt.Println("Predecessor stopped responding, ceasing connection")
		altNode.rpcServ.rpcClose(predecessor)
	}
}

/* Ring lookup */

// FindSuccessor sets 'ret' to the successor of a key 'k' in the ring
func (altNode *Node) FindSuccessor(k Key, ret *Peer) error {
	altNode.membersMutex.RLock()
	succ := altNode.Members.FindSuccessor(k)
	altNode.membersMutex.RUnlock()

	*ret = *getPeer(succ)
	return nil
}

// fingListSuccessor is similar to 'FindSuccessor', except that it returns
// a list element in altNode.Members.List, instead of the value of the
// element.
func (altNode *Node) findListSuccessor(k Key) *list.Element {
	altNode.membersMutex.RLock()
	succ := altNode.Members.FindSuccessor(k)
	altNode.membersMutex.RUnlock()

	return succ
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
	altNode.membersMutex.Lock()
	altNode.Members = newMembers
	altNode.membersMutex.Unlock()
}

/* Background task functions */

// autoSyncMembers automatically syncs members with a random node
func (altNode *Node) autoSyncMembers() {
	for {
		altNode.membersMutex.RLock()
		random := altNode.Members.GetRandom()
		altNode.membersMutex.RUnlock()

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

	merged, changes := mergeHistories(altNode.MemberHist, peerHist)
	altNode.historyMutex.Lock()
	altNode.MemberHist = merged
	altNode.historyMutex.Unlock()
	return changes
}

// insertToHistory inserts an entry to the node's history
func (altNode *Node) insertToHistory(entry histEntry) {
	altNode.historyMutex.Lock()
	altNode.MemberHist.InsertEntry(entry)
	altNode.historyMutex.Unlock()
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
	altNode.membersMutex.RLock()
	for current := altNode.Members.List.Front(); current != nil; current = current.Next() {
		members = append(members, *getPeer(current))
	}
	altNode.membersMutex.RUnlock()
	*ret = members
	return nil
}

// GetMemberHist returns the node's membership history
func (altNode *Node) GetMemberHist(_ struct{}, ret *[]histEntry) error {
	altNode.historyMutex.RLock()
	*ret = altNode.MemberHist
	altNode.historyMutex.RUnlock()
	return nil
}

func (altNode *Node) getSuccessor() *Peer {
	altNode.membersMutex.RLock()
	succElt := altNode.Members.Map[altNode.ID]
	succElt = succElt.Next()
	if succElt == nil {
		succElt = altNode.Members.List.Front()
	}
	altNode.membersMutex.RUnlock()
	return getPeer(succElt)
}

func (altNode *Node) getListSuccessor() *list.Element {
	altNode.membersMutex.RLock()
	succElt := altNode.Members.Map[altNode.ID]
	succElt = succElt.Next()
	if succElt == nil {
		succElt = altNode.Members.List.Front()
	}
	altNode.membersMutex.RUnlock()
	return succElt
}

func (altNode *Node) getPredecessor() *Peer {
	altNode.membersMutex.RLock()
	predElt := altNode.Members.Map[altNode.ID]
	predElt = predElt.Prev()
	if predElt == nil {
		predElt = altNode.Members.List.Back()
	}
	altNode.membersMutex.RUnlock()
	return getPeer(predElt)
}

func (altNode *Node) getNthSuccessor(n int) *Peer {
	altNode.membersMutex.RLock()
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
	altNode.membersMutex.RUnlock()
	return getPeer(current)
}

func (altNode *Node) getNthPredecessor(n int) *Peer {
	altNode.membersMutex.RLock()
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
	altNode.membersMutex.RUnlock()
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
