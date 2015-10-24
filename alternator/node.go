package main

import (
	"crypto/sha1"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"time"
)

// AlterNode is a node in Alternator
type AlterNode struct {
	ID      string
	Address string
	Fingers []*ExtNode
}

// ExtNode is an external, non-local node
type ExtNode struct {
	ID      string
	Address string
}

/* Node methods */

func (extNode ExtNode) String() (str string) {
	str += "ID:" + extNode.ID
	str += "Address:" + extNode.Address
	return
}

// FindSuccessor finds the successor of a key in the ring
func (altNode *AlterNode) FindSuccessor(key string, ret *ExtNode) error {
	// Find ID of successor
	succIdx := 0
	for i, node := range altNode.Fingers {
		if key > node.ID {
			succIdx = i
		}
		if key < node.ID {
			break
		}
	}

	// Reply with external node
	ret.ID = altNode.Fingers[succIdx].ID
	ret.Address = altNode.Fingers[succIdx].ID
	return nil
}

// Very annoying, will be using strings as IDs instead of a byte slice, since slices
// cannot be used as a key in a map. Still, IDs are simply sha1sums of the address.
func genID() string {
	hostname, _ := os.Hostname()
	h := sha1.New()
	rand.Seed(time.Now().UTC().UnixNano())
	io.WriteString(h, hostname+strconv.Itoa(rand.Int()))
	return fmt.Sprintf("%x", h.Sum(nil))
}

func (altNode *AlterNode) initFingers() {
	selfExt := ExtNode{altNode.ID, altNode.Address}
	altNode.Fingers = append(altNode.Fingers, &selfExt)
	fmt.Println(altNode.Fingers[0].String())
}

// Init this node
func initNode() {
	// Register node
	node := new(AlterNode)
	rpc.Register(node)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", ":0")
	if e != nil {
		log.Fatal("listen error:", e)
	}
	port := strings.Split(l.Addr().String(), ":")[3]

	node.ID = genID()
	node.Address = "127.0.0.1:" + port
	node.initFingers()

	fmt.Println("Listening on port " + port)
	http.Serve(l, nil)
}
