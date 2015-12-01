package alternator

import (
	"fmt"
	"time"
)

// history represents a membership history
type history []histEntry

// histEntry is an entry in the membership history
type histEntry struct {
	Time  time.Time
	Class int
	Node  Peer
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
	return fmt.Sprintf("%v: %s, %v\n", entry.Time, class, entry.Node)
}

func compareEntries(a histEntry, b histEntry) bool {
	if (a.Time.After(b.Time.Add(-1 * time.Second))) && (a.Time.Before(b.Time.Add(1 * time.Second))) {
		if (a.Class == b.Class) && (a.Node == b.Node) {
			return true
		}
	}
	return false
}

func (hist history) String() {
	for _, entry := range hist {
		fmt.Println(entry.String())
	}
}

// InsertEntry returns an entry to hist, returning the new history
func (hist *history) InsertEntry(entry histEntry) {
	histSlice := *hist
	i := 0
	// Find correct position for entry (sorted chronologically)
	for i = range histSlice {
		if entry.Time.Before(histSlice[i].Time) {
			histSlice = append(histSlice, histEntry{})
			copy(histSlice[i+1:], histSlice[i:])
			histSlice[i] = entry
			*hist = histSlice
		}
	}
	// Add at end if needed
	histSlice = append(histSlice, entry)
	*hist = histSlice
}

// mergeHistories merges two histories, returning one with all entries. Second return value is
// true iff a and b are different.
func mergeHistories(a, b history) (history, bool) {
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
			a.InsertEntry(b[i])
			changes = true
		}
	}
	return a, changes
}
