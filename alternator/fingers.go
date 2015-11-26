package main

import (
	"container/list"
	"fmt"
	"time"
)

// Fingers stores the fingers of a node
type Fingers struct {
	List *list.List
	// Slice []*ExtNode
	Map map[Key]*list.Element
}

const fingerUpdateTime = 400

func (fingers *Fingers) init() {
	// selfExt := ExtNode{altNode.ID, altNode.Address}
	fingers.List = list.New()
	fingers.Map = make(map[Key]*list.Element)
	// altNode.Fingers.Add(&selfExt)
}

func getExt(e *list.Element) *ExtNode {
	if e != nil {
		return e.Value.(*ExtNode)
	}
	return nil
}

// Inserts a finger
// func (fingers *Fingers) sliceInsert(new *ExtNode, i int) {
// 	// Grow the slice by one element.
// 	fingers.Slice = append(fingers.Slice, &ExtNode{})
// 	copy(fingers.Slice[i+1:], fingers.Slice[i:])
// 	fingers.Slice[i] = new
// }

// FindSuccessor finds the successor of a key in the ring
func (fingers *Fingers) FindSuccessor(k Key) (*list.Element, error) {
	// Find ID of successor
	for e := fingers.List.Front(); e != nil; e = e.Next() {
		if keyCompare(getExt(e).ID, k) > 0 {
			return e, nil
		}
	}
	// Nothing bigger in ring, successor is first node
	return (fingers.List.Front()), nil
}

// Insert adds a node to the fingers if not already there, returns true if added
func (fingers *Fingers) Insert(ext *ExtNode) *list.Element {
	i := 0
	// Iterate through fingers
	for e := fingers.List.Front(); e != nil; e = e.Next() {
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
		// Already in Fingers
		if keyCompare(ext.ID, current.ID) == 0 {
			return nil
		} else if (keyCompare(ext.ID, prevID) > 0) && (keyCompare(ext.ID, current.ID) < 0) {
			// Correct spot to add, add to list, slice and map
			// fingers.sliceInsert(ext, i)
			e := fingers.List.InsertBefore(ext, e)
			fingers.Map[ext.ID] = e
			return e
		}
		i++
	}
	// Append at end if not in fingers and not added by loop
	// fingers.sliceInsert(ext, 0)
	e := fingers.List.PushBack(ext)
	fingers.Map[ext.ID] = e
	return e
}

// Remove deletes a node from fingers
func (fingers *Fingers) Remove(del *ExtNode) {
	for e := fingers.List.Front(); e != nil; e = e.Next() {
		if getExt(e).ID == del.ID {
			fingers.List.Remove(e)
			delete(fingers.Map, del.ID)
		}
	}
}

func (fingers Fingers) String() (str string) {
	i := 0
	for e := fingers.List.Front(); e != nil; e = e.Next() {
		str += fmt.Sprintf("Finger %d:\n%s\n", i, getExt(e).String())
		i++
	}
	return
}

// Compares finger table to neighbors, adds new
// func (altNode *Alternator) updateFingers() {
// 	var successorFingers []*ExtNode
// 	// Get successors fingers
// 	if altNode.Successor != nil {
// 		makeRemoteCall(altNode.Successor, "GetFingers", struct{}{}, &successorFingers)
// 	}
//
// 	for _, finger := range successorFingers {
// 		altNode.Fingers.AddIfMissing(finger)
// 		// elem := altNode.Fingers.AddIfMissing(finger)
// 		// if elem != nil {
// 		// 	// Send this node the keys that belong to it
// 		// 	altNode.expelForeignKeys(elem)
// 		// }
// 	}
// }

func (fingers Fingers) getRandomFinger() *ExtNode {
	// i := 0
	for _, finger := range fingers.Map {
		// if i == 1 {
		// 	return getExt(finger)
		// }
		// i++
		return getExt(finger)
	}
	return nil
}

func (altNode *Alternator) rebuildFingers() {
	var newFingers Fingers
	newFingers.init()
	for _, entry := range altNode.MemberHist {
		switch entry.Class {
		case histJoin:
			newFingers.Insert(entry.Node)
		case histLeave:
			newFingers.Remove(entry.Node)
		}
	}
	altNode.Fingers = newFingers
}

func (altNode *Alternator) syncFingers(ext *ExtNode) {
	if changes := altNode.syncMemberHist(ext); changes {
		altNode.rebuildFingers()
	}
}

// autoSyncFingers automatically syncs fingers with a random node
func (altNode *Alternator) autoSyncFingers() {
	for {
		altNode.syncFingers(altNode.Fingers.getRandomFinger())
		// fmt.Println("Fingers are " + altNode.Fingers.String())
		// altNode.printHist()
		time.Sleep(fingerUpdateTime * time.Millisecond)
	}
}
