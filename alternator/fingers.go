package main

import (
	"fmt"
	"time"
)

// Fingers stores the fingers of a node
type Fingers struct {
	Slice []ExtNode
}

const fingerUpdateTime = 400

// Inserts a finger
func (fingers *Fingers) insert(i int, new ExtNode) {
	// Grow the slice by one element.
	fingers.Slice = append(fingers.Slice, ExtNode{})
	copy(fingers.Slice[i+1:], fingers.Slice[i:])
	fingers.Slice[i] = new
}

// FindSuccessor finds the successor of a key in the ring
func (fingers *Fingers) FindSuccessor(key string) (ExtNode, error) {
	// Find ID of successor
	for i, node := range fingers.Slice {
		// fmt.Println("Going over: " + node.ID)
		if node.ID > key {
			// Reply with external node
			return fingers.Slice[i], nil
		}
	}
	// Nothing bigger in circle, successor is first node
	return fingers.Slice[0], nil
}

// AddIfMissing adds a finger if not already in fingers
func (fingers *Fingers) AddIfMissing(ext ExtNode) {
	prevID := minKey
	length := len(fingers.Slice)
	// Iterate through fingers
	for i := 0; i < length; i++ {
		// Already in Fingers
		if ext.ID == fingers.Slice[i].ID {
			return
			// Correct spot to add
		} else if (ext.ID > prevID) && (ext.ID < fingers.Slice[i].ID) {
			fingers.insert(i, ext)
			return
		}
		prevID = fingers.Slice[i].ID
	}
	// Append at end if not in fingers and not added by loop
	fingers.Slice = append(fingers.Slice, ext)
}

func (fingers Fingers) String() (str string) {
	for i, node := range fingers.Slice {
		str += fmt.Sprintf("Finger %d:\n%s", i, node.String())
	}
	return
}

// Compares finger table to neighbors, adds new
func (altNode *AlterNode) updateFingers() {
	// fmt.Println("My fingers are: " + altNode.Fingers.String())
	var successorFingers []ExtNode
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
