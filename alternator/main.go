package main

import (
	"flag"
	"fmt"
	"log"
	"net/rpc"
)

func main() {
	var port string
	var addr string
	var command string
	var joinPort string
	// var new bool

	flag.StringVar(&port, "port", "0", "port that the node will use for communication.")
	flag.StringVar(&addr, "target", "127.0.0.1", "target address for commands.")
	flag.StringVar(&command, "command", "", "name of command.")
	flag.StringVar(&joinPort, "join", "0", "joins the ring that the node at [address]:[port] belongs to.")
	// flag.BoolVar(&new, "new", false, "creates a new ring")
	flag.Parse()

	// Use -c to issue a command
	if command != "" {
		// Control panel mode
		switch command {
		case "FindSuccessor":
			client, err := rpc.DialHTTP("tcp", addr+":"+port)
			if err != nil {
				log.Fatal("dialing:", err)
			}

			id := genID()
			fmt.Println("Finding successor of " + id)
			var reply ExtNode
			err = client.Call("AlterNode."+command, id, &reply)
			fmt.Println(reply.String())
		}

	} else {
		if joinPort != "0" {
			InitNode(port, addr+":"+joinPort)
		} else {
			InitNode(port, "")
		}
	}
	return
}
