package main

import (
	"fmt"
	"git/mail"
	"os"
	"strconv"
)

func handlerLoop(ch chan *mail.Packet) {
	for {
		packet := <-ch
		processPacket(packet)
	}
}

func receiveLoop(ch chan *mail.Packet) {
	for {
		var packet mail.Packet
		mail.Receive("UDP", &packet)
		ch <- &packet
	}
}

func main() {
	localhost := "127.0.0.1"
	port := mail.UDPBind()
	id := getID(port)

	var packet mail.Packet
	if len(os.Args) > 1 {
		packet.Sender = id
		packet.Content = ""
		// Fill packet.Content
		for _, arg := range os.Args[2:] {
			packet.Content += arg + " "
		}
		peer := mail.Peer{Protocol: "UDP", Address: localhost + ":" + os.Args[1]}
		packet.Type = mail.Join
		mail.Send(&packet, &peer)
	} else {
		// Bind to a UDP port
		fmt.Println("Now listening at port " + strconv.Itoa(port))
		fmt.Println("Node ID is: " + id)
		fmt.Println()

		ch := make(chan *mail.Packet)
		go handlerLoop(ch)
		receiveLoop(ch)
	}

	return
}
