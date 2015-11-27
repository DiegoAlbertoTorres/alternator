package members

import (
	"fmt"
	p "git/alternator/peer"
	"time"
)

// History represents a membership history
type History []HistEntry

// HistEntry is an entry in the membership history
type HistEntry struct {
	Time  time.Time
	Class int
	Node  p.Peer
}

// Entry classes
const (
	HistJoin = iota
	HistLeave
)

// String gives a string representation of a HistEntry
func (entry HistEntry) String() string {
	var class string
	switch entry.Class {
	case HistJoin:
		class = "join"
	case HistLeave:
		class = "leave"
	}
	return fmt.Sprintf("%v: %s, %v\n", entry.Time, class, entry.Node)
}

func compareEntries(a HistEntry, b HistEntry) bool {
	if (a.Time.After(b.Time.Add(-1 * time.Second))) && (a.Time.Before(b.Time.Add(1 * time.Second))) {
		if (a.Class == b.Class) && (a.Node == b.Node) {
			return true
		}
	}
	return false
}

func (hist History) String() {
	for _, entry := range hist {
		fmt.Println(entry.String())
	}
}

// InsertEntry returns an entry to hist, returning the new history
func InsertEntry(hist History, entry HistEntry) History {
	i := 0
	// Find correct position for entry (sorted chronologically)
	for i = range hist {
		if entry.Time.Before(hist[i].Time) {
			hist = append(hist, HistEntry{})
			copy(hist[i+1:], hist[i:])
			hist[i] = entry
			return hist
		}
	}
	// Add at end if needed
	hist = append(hist, entry)
	return hist
}

// MergeHistories merges two histories, returning one with all entries. Second return value is
// true iff a and b are different.
func MergeHistories(a, b History) (History, bool) {
	findInA := func(entry HistEntry) bool {
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
			a = InsertEntry(a, b[i])
			changes = true
		}
	}
	return a, changes
}
