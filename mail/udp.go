/* UDP protocol functionality implementation
 * */
package mail

import (
	"fmt"
	"net"
	"os"
  "strings"
  "strconv"
)

var serverConn *net.UDPConn

func udpSend(packet *Packet, addr string) {
	udpAddr, err := net.ResolveUDPAddr("udp", addr)
	// udpAddr, err := net.ResolveUDPAddr("udp", "127.0.0.1:10001")
	checkError(err)

	conn, err := net.DialUDP("udp", nil, udpAddr)
	checkError(err)

	_, err = conn.Write([]byte(packet.Content))
	checkError(err)
}

// Must be called before using mail.receive for UDP
func UdpBind() int {
  var err error
  var serverAddr *net.UDPAddr

	// Resolve local ip
	serverAddr, err = net.ResolveUDPAddr("udp", "127.0.0.1:0")
  checkError(err)

  // Now listen there, this also resolves the port
	serverConn, err = net.ListenUDP("udp", serverAddr)
	checkError(err)
  // For some reason, you can only get the chosen port by extracting from this string
  // Apparently there is no parser either
  port, err := strconv.Atoi(strings.Split(serverConn.LocalAddr().String(), ":")[1])
  checkError(err)

  return port
}

func UdpClose() {
  serverConn.Close()
}

func udpReceive(ret *Packet) {

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
