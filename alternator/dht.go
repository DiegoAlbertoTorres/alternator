package main

import (
	// "fmt"
	"crypto/sha1"
	"fmt"
	"git/mail"
	"io"
	"math/rand"
	"os"
	"strconv"
	"time"
)

type nodeRange struct {
	Min string
	Max string
}

type alternode struct {
	ID    string
	Peer  mail.Peer
	Range nodeRange
}

func (node alternode) String() (str string) {
	str += "ID: " + node.ID + "\n"
	str += "Address: " + node.Peer.String() + "\n"
	return str
}

// Map ids to members
var alternodes = make(map[string]*alternode)

// Very annoying, will be using strings as IDs instead of a byte slice, since slices
// cannot be used as a key in a map. Still, IDs are simply sha1sums of the address.
func getID(port int) string {
	hostname, _ := os.Hostname()
	h := sha1.New()
	rand.Seed(time.Now().UTC().UnixNano())
	io.WriteString(h, hostname+strconv.Itoa(port)+strconv.Itoa(rand.Int()))
	return fmt.Sprintf("%x", h.Sum(nil))
}

func processPacket(packet *mail.Packet) {
	// fmt.Println(packet.String())
	fmt.Println("Got package:")
	fmt.Println(packet.String())
	switch packet.Type {
	case mail.Join:
		// Create a new node
		newNode := alternode{packet.Sender, packet.Peer, nodeRange{"a", "b"}}
		// Add to member list
		alternodes[packet.Sender] = &newNode

		fmt.Println("Map now looks like:")
		// fmt.Println(packet.String())
		for _, node := range alternodes {
			fmt.Printf("Node %s\n", node.String())
		}
	}
}

func findCoordinator(key string) *alternode {
	for _, node := range alternodes {
		if (key <= node.Range.Max) && (key >= node.Range.Min) {
			return node
		}
	}
	return nil
}
