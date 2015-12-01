package alternator

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"os/signal"
	"strings"
	"time"
)

// Alternator is a node in Alternator
type Alternator struct {
	ID         Key
	Address    string
	Port       string
	Members    Members
	MemberHist history
	DB         *DB
	Config     Config
}

// Config stores Alternator's configuration settings
type Config struct {
	// Whether output uses complete keys or abbreviations
	FullKeys bool
	// Time between history syncs with a random peer
	MemberSyncTime int
	// Time between heartbeats
	HeartbeatTime int
	// Time before a heartbeat times out
	HeartbeatTimeout int
	// N is the number of nodes in which metadata is replicated
	N int
	// Directory for alternator's data
	DotPath string
}

/* Alternator methods */

// GetMembers sets ret to this node's members
// func (altNode *Alternator) GetMembers(_ struct{}, ret *[]*Peer) error {
// 	*ret = altNode.Members.Slice
// 	return nil
// }

// func (altNode *Alternator) expelForeignKeys(elem *list.Element) {
// 	peer := getPeer(elem)
// 	var prevID Key
// 	if prev := getPeer(elePrev()); prev != nil {
// 		prevID = prev.ID
// 	} else {
// 		prevID = minKey
// 	}
// 	keys, vals := altNode.dbGetRange(prevID, peer.ID)
// 	for i := range keys {
// 		err := MakeRemoteCall(peer, "DBPut", PutArgs{keys[i], vals[i]}, &struct{}{})
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
	keys, vals := altNode.DB.getRange(altNode.ID, other.ID)
	// fmt.Printf("giving pairs in range %s to %s\n", keyToString(altNode.ID), keyToString(other.ID))
	for i := range keys {
		fmt.Println(keys[i])
	}

	// Add join to history
	newEntry := histEntry{Time: time.Now(), Class: histJoin, Node: *other}
	altNode.insertTohistory(newEntry)
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
	DepartureEntry histEntry
}

// LeaveRequest handles a leave request. It appends the departure entry to the node's history.
func (altNode *Alternator) LeaveRequest(args *LeaveRequestArgs, _ *struct{}) error {
	// Insert received keys
	batchArgs := BatchPutArgs{metaDataBucket, args.Keys, args.Vals}
	altNode.BatchPut(batchArgs, &struct{}{})
	altNode.insertTohistory(args.DepartureEntry)
	altNode.Members.Remove(&args.DepartureEntry.Node)
	fmt.Println("Members changed:", altNode.Members)
	// altNode.setPredecessor(altNode.getNthPredecessor(1), "LeaveRequest()")

	// Replicate in one more
	err := MakeRemoteCall(altNode.getNthSuccessor(altNode.Config.N-1), "BatchPut", batchArgs, &struct{}{})
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
	keys, vals := altNode.DB.getRange(altNode.getPredecessor().ID, altNode.ID) // Gather entries
	departureEntry := histEntry{Time: time.Now(), Class: histLeave, Node: altNode.selfExt()}
	args := LeaveRequestArgs{keys, vals, departureEntry}

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

func (altNode Alternator) string() (str string) {
	str += "ID: " + altNode.ID.String() + "\n"
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
	*ret = *getPeer(succ)
	return err
}

// selfExt returns an peerNode equivalent of altNode
func (altNode *Alternator) selfExt() Peer {
	return Peer{ID: altNode.ID, Address: altNode.Address}
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
			// Kill connection
			Close(predecessor)

			// altNode.setPredecessor(nil, "checkPredecessor()")
		}
		// Call timed out
	case <-time.After(time.Duration(altNode.Config.HeartbeatTimeout) * time.Millisecond):
		fmt.Println("Predecessor stopped responding, ceasing connection")
		// Kill connection
		Close(predecessor)

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
	altNode.insertTohistory(histEntry{Time: time.Now(), Class: histJoin, Node: self})
	altNode.Members.Insert(&self)
}

func (altNode *Alternator) rebuildMembers() {
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

func (altNode *Alternator) syncMembers(peer *Peer) {
	if changes := altNode.syncMemberHist(peer); changes {
		altNode.rebuildMembers()
		fmt.Println("Members changed:", altNode.Members)
		// fmt.Println("Members are ", altNode.Members)
	}
}

// autoSyncMembers automatically syncs members with a random node
func (altNode *Alternator) autoSyncMembers() {
	for {
		random := altNode.Members.GetRandom()
		altNode.syncMembers(random)
		// altNode.printHist()
		time.Sleep(time.Duration(altNode.Config.MemberSyncTime) * time.Millisecond)
	}
}

func (altNode *Alternator) getSuccessor() *Peer {
	succElt := altNode.Members.Map[altNode.ID]
	succElt = succElt.Next()
	if succElt == nil {
		succElt = altNode.Members.List.Front()
	}
	return getPeer(succElt)
}

func (altNode *Alternator) getPredecessor() *Peer {
	predElt := altNode.Members.Map[altNode.ID]
	predElt = predElt.Prev()
	if predElt == nil {
		predElt = altNode.Members.List.Back()
	}
	return getPeer(predElt)
}

func (altNode *Alternator) getNthSuccessor(n int) *Peer {
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

func (altNode *Alternator) getNthPredecessor(n int) *Peer {
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

func (altNode *Alternator) autoCheckPredecessor() {
	for {
		altNode.checkPredecessor()
		time.Sleep(time.Duration(altNode.Config.HeartbeatTime) * time.Millisecond)
	}
}

// GetMemberHist returns the node's membership history
func (altNode *Alternator) GetMemberHist(_ struct{}, ret *[]histEntry) error {
	*ret = altNode.MemberHist
	return nil
}

// syncMemberHist synchronizes the node's member history with an peerernal node
func (altNode *Alternator) syncMemberHist(peer *Peer) bool {
	if peer == nil {
		return false
	}
	var peerHist history

	err := MakeRemoteCall(peer, "GetMemberHist", struct{}{}, &peerHist)
	// fmt.Printf("Comparing members with %v\n", peer)
	// fmt.Printf("His history is %v\n", peerMemberHist)
	if err != nil {
		return false
	}

	var changes bool
	altNode.MemberHist, changes = mergeHistories(altNode.MemberHist, peerHist)
	// fmt.Printf("my new history is %v\n", altNode.MemberHist)
	return changes
}

func (altNode *Alternator) insertTohistory(entry histEntry) {
	altNode.MemberHist.InsertEntry(entry)
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
func InitNode(conf Config, port string, address string) {
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
	node.ID = GenID(port)
	node.Config = conf
	node.Members.Init()
	node.initDB()

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
	go node.autoCheckPredecessor()
	go node.autoSyncMembers()
	go node.sigHandler()
	fmt.Println(node.string())
	fmt.Println("Listening on port " + port)
	http.Serve(l, nil)
}
