package alternator

// Peer is an peerernal, non-local node
type Peer struct {
	ID      Key
	Address string
}

func (peerNode Peer) String() (str string) {
	str += "ID: " + peerNode.ID.String()
	// str += "Address: " + peerNode.Address + "\n"
	return
}
