package main

import (
	"bytes"
	"container/list"
	"fmt"
	"time"
)

// Fingers stores the fingers of a node
type Fingers struct {
	List  *list.List
	Slice []*ExtNode
}

const fingerUpdateTime = 400

func (altNode *AlterNode) initFingers() {
	selfExt := ExtNode{altNode.ID, altNode.Address}
	altNode.Fingers.List = list.New()
	altNode.Fingers.AddIfMissing(&selfExt)
}

func getExt(e *list.Element) *ExtNode {
	return e.Value.(*ExtNode)
}

// Inserts a finger
func (fingers *Fingers) sliceInsert(new *ExtNode, i int) {
	// Grow the slice by one element.
	fingers.Slice = append(fingers.Slice, &ExtNode{})
	copy(fingers.Slice[i+1:], fingers.Slice[i:])
	fingers.Slice[i] = new
}

// FindSuccessor finds the successor of a key in the ring
func (fingers *Fingers) FindSuccessor(key []byte) (*ExtNode, error) {
	// Find ID of successor
	for e := fingers.List.Front(); e != nil; e = e.Next() {
		finger := getExt(e)
		if bytes.Compare(finger.ID, key) > 0 {
			return finger, nil
		}
	}
	// Nothing bigger in circle, successor is first node
	return getExt(fingers.List.Front()), nil
}

// AddIfMissing adds a node to the fingers if not already there, returns true if added
func (fingers *Fingers) AddIfMissing(ext *ExtNode) bool {
	i := 0
	// Iterate through fingers
	for e := fingers.List.Front(); e != nil; e = e.Next() {
		current := getExt(e)
		var prevID []byte
		if prev := e.Prev(); prev != nil {
			prevID = getExt(prev).ID
		} else {
			prevID = minKey
		}
		// Already in Fingers
		if bytes.Compare(ext.ID, current.ID) == 0 {
			return false
		} else if (bytes.Compare(ext.ID, prevID) > 0) && (bytes.Compare(ext.ID, current.ID) < 0) {
			// Correct spot to add, add to list and slice
			fingers.List.InsertBefore(ext, e)
			fingers.sliceInsert(ext, i)
			return true
		}
		i++
	}
	// Append at end if not in fingers and not added by loop
	fingers.List.PushBack(ext)
	fingers.sliceInsert(ext, 0)
	return true
}

func (fingers Fingers) String() (str string) {
	i := 0
	for e := fingers.List.Front(); e != nil; e = e.Next() {
		str += fmt.Sprintf("Finger %d:\n%s", i, getExt(e).String())
		i++
	}
	return
}

// Compares finger table to neighbors, adds new
func (altNode *AlterNode) updateFingers() {
	var successorFingers []*ExtNode
	// Get successors fingers
	makeRemoteCall(altNode.Successor, "GetFingers", struct{}{}, &successorFingers)

	for _, finger := range successorFingers {
		altNode.Fingers.AddIfMissing(finger)
	}
}

func (altNode *AlterNode) autoUpdateFingers() {
	for {
		altNode.updateFingers()
		time.Sleep(fingerUpdateTime * time.Millisecond)
	}
}
