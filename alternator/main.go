package main

import (
	"fmt"
	"git/mail"
	"strconv"
)

func addrStr(ip string, port int) string {
	return ip + ":" + strconv.Itoa(port)
}

func main() {
	localhost := "127.0.0.1"
  // Bind to a UDP port
  port := mail.UdpBind()
  fmt.Println("Listening at port " + strconv.Itoa(port))

	packet := mail.Packet{"Long live Go!"}
	peer := mail.Peer{"UDP", addrStr(localhost, port)}

	mail.Send(&packet, &peer)

	mail.Receive("UDP", &packet)
	fmt.Println(packet.Content)

	return
}
