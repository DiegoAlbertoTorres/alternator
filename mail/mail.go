/* Package responsible for sending 'messages' to other nodes.
 * */
package mail

type Peer struct {
	Protocol string
	Address  string
}

type Packet struct {
	Content string
}

func Send(packet *Packet, peer *Peer) {
	if peer.Protocol == "UDP" {
		udpSend(packet, peer.Address)
	}
}

// Returns the receiving port
func Receive(protocol string, ret *Packet) {
	if protocol == "UDP" {
		udpReceive(ret)
	}
}
