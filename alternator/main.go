package main

import (
	"flag"
	"fmt"
	"log"
	"net/rpc"
	"os"
)

func main() {
	var port string
	var addr string
	var isCommand bool
	var command string
	var join bool
	// var new bool

	flag.StringVar(&port, "port", "0", "port that the node will use for communication.")
	flag.StringVar(&addr, "target", "127.0.0.1", "target address for commands.")
	flag.StringVar(&command, "command", "", "name of command.")
	flag.BoolVar(&isCommand, "c", false, "use flag to run in command mode.")
	flag.BoolVar(&join, "join", false, "joins the ring that the node at [address]:[port] belongs to.")
	// flag.BoolVar(&new, "new", false, "creates a new ring")
	flag.Parse()

	// Use -c to issue a command
	if isCommand {
		command := os.Args[3]
		// Control panel mode
		switch command {
		case "FindSuccessor":
			client, err := rpc.DialHTTP("tcp", addr+":"+port)
			if err != nil {
				log.Fatal("dialing:", err)
			}

			id := genID()
			var reply ExtNode
			err = client.Call("AlterNode."+command, id, &reply)
			fmt.Println(reply.String())
		}

	} else {
		if join {
			initNode(addr + ":" + port)
		} else {
			initNode("")
		}
	}
	return
}
