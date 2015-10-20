/* UDP protocol functionality implementation
 * */
package mail

import (
	"fmt"
	"net"
	"os"
)

func udpSend(packet *Packet, addr string) {
	udpAddr, err := net.ResolveUDPAddr("udp", addr)
	// udpAddr, err := net.ResolveUDPAddr("udp", "127.0.0.1:10001")
	checkError(err)

	conn, err := net.DialUDP("udp", nil, udpAddr)
	checkError(err)

	_, err = conn.Write([]byte(packet.Content))
	checkError(err)
}

func udpReceive(ret *Packet) {
	// Resolve local ip
	serverAddr, err := net.ResolveUDPAddr("udp", "127.0.0.1:10001")
	checkError(err)

	// Now listen
	serverConn, err := net.ListenUDP("udp", serverAddr)
	checkError(err)
	defer serverConn.Close()

	buf := make([]byte, 1024)

	n, _, err := serverConn.ReadFromUDP(buf)
	checkError(err)
	ret.Content = string(buf[0:n])
}

func checkError(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "Fatal error ", err.Error())
		os.Exit(1)
	}
}
