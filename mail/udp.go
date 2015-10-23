/*Package mail UDP protocol functionality implementation
 * */
package mail

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
)

var serverConn *net.UDPConn

// UDPStrAddr converts an IP (as a string) and a port into a single address string
func UDPStrAddr(ip string, port int) string {
	return ip + ":" + strconv.Itoa(port)
}

// UDPBind must be called before using mail.receive for UDP
func UDPBind() int {
	var err error
	var serverAddr *net.UDPAddr

	// Port 0 means find some port
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

func udpSend(packet *Packet, addr string) {
	udpAddr, err := net.ResolveUDPAddr("udp", addr)
	// udpAddr, err := net.ResolveUDPAddr("udp", "127.0.0.1:10001")
	checkError(err)

	conn, err := net.DialUDP("udp", nil, udpAddr)
	checkError(err)

	// Will use gob for serialization.
	// _, err = conn.Write([]byte(packet.Content))
	// buf := make([]byte, 1024)
	// bufferedWriter := bytes.NewReader(buf)
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	// Hopefully fits in a single UDP packet
	enc.Encode(packet)
	conn.Write(buf.Bytes())
	checkError(err)
}

// UDPClose closes the UDP connection
func UDPClose() {
	serverConn.Close()
}

func udpReceive(ret *Packet) {
	buf := make([]byte, 1024)
	// var buf bytes.Buffer

	_, sender, err := serverConn.ReadFromUDP(buf)
	// fmt.Println(buf)
	// fmt.Println([]byte("hello!"))
	// Now unserialize with gob
	bufferedReader := bytes.NewReader(buf)
	dec := gob.NewDecoder(bufferedReader)
	dec.Decode(ret)

	checkError(err)
	ret.Peer.Protocol = "UDP"
	ret.Peer.Address = UDPStrAddr(sender.IP.String(), sender.Port)
	// ret.Content = string(buf[0:n])
}

func checkError(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "Fatal error ", err.Error())
		os.Exit(1)
	}
}
