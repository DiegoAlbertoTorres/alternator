package main

import (
	"fmt"
	"time"
)

// MemberHist represents a membership history
type MemberHist []histEntry

type histEntry struct {
	Time  time.Time
	Class int
	Node  *ExtNode
}

// Entry classes
const (
	histJoin = iota
	histLeave
)

// String gives a string representation of a histEntry
func (entry histEntry) String() string {
	var class string
	switch entry.Class {
	case histJoin:
		class = "join"
	case histLeave:
		class = "leave"
	}
	return fmt.Sprintf("%v: %s, %v", entry.Time, class, entry.Node)
}

func compareEntries(a histEntry, b histEntry) bool {
	// fmt.Printf("Comparing:\n %v\nand\n%v\n", a, b)
	if (a.Time.After(b.Time.Add(-1 * time.Second))) && (a.Time.Before(b.Time.Add(1 * time.Second))) {
		if (a.Class == b.Class) && extCompare(a.Node, b.Node) {
			// fmt.Println("Result: equal")
			return true
		}
	}
	// fmt.Println("Result: different")
	return false
}

func (altNode *Alternator) printHist() {
	for _, entry := range altNode.MemberHist {
		fmt.Println(entry.String())
	}
}

// GetMemberHist returns the node's membership history
func (altNode *Alternator) GetMemberHist(_ struct{}, ret *[]histEntry) error {
	*ret = altNode.MemberHist
	return nil
}

// func (altNode Alternator) GetLastEntry

func (altNode *Alternator) updateMemberHist() bool {
	// Compare with random node
	ext := altNode.getRandomFinger()
	if ext == nil {
		return false
	}
	fmt.Printf("Comparing with %v\n", ext)
	var extMemberHist MemberHist
	// Last time updated
	// lastTime := altNode.MemberHist[len(altNode.MemberHist)-1].Time

	err := makeRemoteCall(ext, "GetMemberHist", struct{}{}, &extMemberHist)
	if err != nil {
		return false
	}

	var changes bool
	altNode.MemberHist, changes = mergeHistories(altNode.MemberHist, extMemberHist)
	return changes
}

func (altNode *Alternator) insertToHistory(entry histEntry) {
	altNode.MemberHist = insertEntry(altNode.MemberHist, entry)
}

func insertEntry(hist MemberHist, entry histEntry) MemberHist {
	for i := range hist {
		if entry.Time.After(hist[i].Time) {
			hist = append(hist, histEntry{})
			copy(hist[i+1:], hist[i:])
			hist[i] = entry
			return hist
		}
	}
	// Add at end if needed
	hist = append(hist, entry)
	return hist
}

func mergeHistories(a MemberHist, b MemberHist) (MemberHist, bool) {
	findInA := func(entry histEntry) bool {
		for i := range a {
			if compareEntries(entry, a[i]) {
				return true
			}
		}
		return false
	}
	changes := false
	// Merge b into a
	for i := range b {
		if !findInA(b[i]) {
			a = insertEntry(a, b[i])
			changes = true
		}
	}
	return a, changes
}
