package peer

import (
	k "git/alternator/key"
)

// Peer is an peerernal, non-local node
type Peer struct {
	ID      k.Key
	Address string
}

func (peerNode Peer) String() (str string) {
	str += "ID: " + peerNode.ID.String()
	// str += "Address: " + peerNode.Address + "\n"
	return
}

// Unresolved: why does this not need to be an actual copy?
func peerNodeCopy(src *Peer, dst *Peer) {
	// fmt.Println("copying this " + keyToString(src.ID))
	// fmt.Printf("copied %d\n", copy(dst.ID, src.ID))
	dst.ID = src.ID
	dst.Address = src.Address
}

func peerCompare(a *Peer, b *Peer) bool {
	if (a.ID == b.ID) && (a.Address == b.Address) {
		return true
	}
	return false
}
