// Package mail is responsible for sending 'Packets' to other nodes.
package mail

// Peer is an entity that a node can send packets to.
type Peer struct {
	Protocol string
	Address  string
}

// Define packet types
const (
	Join = iota
)

// Packet is what is sent by mail
type Packet struct {
	Content string
	Type    int
	Sender  string
	Peer    Peer
}

func (peer Peer) String() string {
	return peer.Protocol + " " + peer.Address
}

func (p Packet) String() (str string) {
	str += "Sender ID:" + p.Sender + "\n"
	str += "Type:" + string(p.Type) + "\n"
	str += "Content:" + p.Content + "\n"
	return str
}

// Send mails a packet
func Send(packet *Packet, peer *Peer) {
	if peer.Protocol == "UDP" {
		udpSend(packet, peer.Address)
	}
}

// Receive is the API call to catch a packet in a protocol's queue
func Receive(protocol string, ret *Packet) {
	if protocol == "UDP" {
		udpReceive(ret)
	}
}
