package main

import (
	"container/list"
	"fmt"
	"time"
)

// Members stores the members of a node
type Members struct {
	List *list.List
	// Slice []*Peer
	Map map[Key]*list.Element
}

const memberUpdateTime = 400

func (members *Members) init() {
	// selfExt := Peer{altNode.ID, altNode.Address}
	members.List = list.New()
	members.Map = make(map[Key]*list.Element)
	// altNode.Members.Add(&selfExt)
}

func getExt(e *list.Element) *Peer {
	if e != nil {
		return e.Value.(*Peer)
	}
	return nil
}

// Inserts a member
// func (members *Members) sliceInsert(new *Peer, i int) {
// 	// Grow the slice by one element.
// 	members.Slice = append(members.Slice, &Peer{})
// 	copy(members.Slice[i+1:], members.Slice[i:])
// 	members.Slice[i] = new
// }

// FindSuccessor finds the successor of a key in the ring
func (members *Members) FindSuccessor(k Key) (*list.Element, error) {
	// Find ID of successor
	for e := members.List.Front(); e != nil; e = e.Next() {
		if keyCompare(getExt(e).ID, k) > 0 {
			return e, nil
		}
	}
	// Nothing bigger in ring, successor is first node
	return (members.List.Front()), nil
}

// Insert adds a node to the members if not already there, returns true if added
func (members *Members) Insert(peer *Peer) *list.Element {
	i := 0
	// Iterate through members
	for e := members.List.Front(); e != nil; e = e.Next() {
		if e == nil {
			break
		}
		current := getExt(e)
		var prevID Key
		if prev := e.Prev(); prev != nil {
			prevID = getExt(prev).ID
		} else {
			prevID = minKey
		}
		// Already in Members
		if keyCompare(peer.ID, current.ID) == 0 {
			return nil
		} else if (keyCompare(peer.ID, prevID) > 0) && (keyCompare(peer.ID, current.ID) < 0) {
			// Correct spot to add, add to list, slice and map
			// members.sliceInsert(peer, i)
			e := members.List.InsertBefore(peer, e)
			members.Map[peer.ID] = e
			return e
		}
		i++
	}
	// Append at end if not in members and not added by loop
	// members.sliceInsert(peer, 0)
	e := members.List.PushBack(peer)
	members.Map[peer.ID] = e
	return e
}

// Remove deletes a node from members
func (members *Members) Remove(del *Peer) {
	for e := members.List.Front(); e != nil; e = e.Next() {
		if getExt(e).ID == del.ID {
			members.List.Remove(e)
			delete(members.Map, del.ID)
		}
	}
}

func (members Members) String() (str string) {
	i := 0
	for e := members.List.Front(); e != nil; e = e.Next() {
		str += fmt.Sprintf("member %d:\n%s\n", i, getExt(e).String())
		i++
	}
	return
}

// Compares member table to neighbors, adds new
// func (altNode *Alternator) updateMembers() {
// 	var successorMembers []*Peer
// 	// Get successors members
// 	if altNode.Successor != nil {
// 		makeRemoteCall(altNode.Successor, "GetMembers", struct{}{}, &successorMembers)
// 	}
//
// 	for _, member := range successorMembers {
// 		altNode.Members.AddIfMissing(member)
// 		// elem := altNode.Members.AddIfMissing(member)
// 		// if elem != nil {
// 		// 	// Send this node the keys that belong to it
// 		// 	altNode.expelForeignKeys(elem)
// 		// }
// 	}
// }

func (members Members) getRandomMember() *Peer {
	// i := 0
	// random := rand.Intn(len(members.Map))
	// for _, member := range members.Map {
	// 	if i == random {
	// 		return getExt(member)
	// 	}
	// 	i++
	// }
	for _, member := range members.Map {
		return getExt(member)
	}
	return nil
}

func (altNode *Alternator) rebuildMembers() {
	var newMembers Members
	newMembers.init()
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
		random := altNode.Members.getRandomMember()
		altNode.syncMembers(random)
		// altNode.printHist()
		time.Sleep(memberUpdateTime * time.Millisecond)
	}
}

func (altNode *Alternator) getSuccessor() *Peer {
	succElt := altNode.Members.Map[altNode.ID]
	succElt = succElt.Next()
	if succElt == nil {
		succElt = altNode.Members.List.Front()
	}
	return getExt(succElt)
}

func (altNode *Alternator) getPredecessor() *Peer {
	predElt := altNode.Members.Map[altNode.ID]
	predElt = predElt.Prev()
	if predElt == nil {
		predElt = altNode.Members.List.Back()
	}
	return getExt(predElt)
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
	return getExt(current)
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
	return getExt(current)
}
