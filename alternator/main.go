package main

import (
	"encoding/hex"
	"flag"
	"fmt"
	"net/rpc"
	"os"
)

// Config stores Alternator's configuration settings
var Config struct {
	fullKeys bool
}

var sigChan chan os.Signal

func main() {
	var port string
	var addr string
	var command string
	var joinPort string

	flag.StringVar(&port, "port", "0", "port that the node will use for communication.")
	flag.StringVar(&addr, "target", "127.0.0.1", "target address for commands.")
	flag.StringVar(&command, "command", "", "name of command.")
	flag.StringVar(&joinPort, "join", "0", "joins the ring that the node at [address]:[port] belongs to.")
	flag.BoolVar(&Config.fullKeys, "fullKeys", false, "if true all keys (hashes) are printed completely")
	flag.Parse()

	if command != "" {
		// Control panel mode
		client, err := rpc.DialHTTP("tcp", addr+":"+port)
		checkErr("dialing:", err)
		switch command {
		case "FindSuccessor":
			key := randomKey()
			fmt.Println("Finding successor of " + keyToString(key))
			var reply ExtNode
			err = client.Call("Alternator."+command, key, &reply)
			checkErr("RPC failed", err)
			fmt.Println(reply.String())
		case "Put":
			args := flag.Args()
			if len(args) < 2 {
				printExit("Usage: Put key value dest1 dest2 dest3...")
			}
			// Prepare key and value
			name := args[0]
			val := []byte(args[1])
			var dests []Key
			if len(args[2:]) < 0 {
				fmt.Println("Need to give at least one destination node")
				return
			}
			for _, arg := range args[2:] {
				dest, _ := hex.DecodeString(arg)
				dests = append(dests, sliceToKey(dest))
			}

			fmt.Println("Putting pair " + name + "," + args[1])
			err = client.Call("Alternator."+command, &PutArgs{name, val, dests, 0}, &struct{}{})
			checkErr("RPC failed", err)
			fmt.Println("Success!")
		case "Get":
			args := flag.Args()
			if len(args) < 1 {
				printExit("Usage: Get key")
			}
			// Prepare key and value
			name := args[0]
			var val []byte
			fmt.Println("Getting " + name)
			err = client.Call("Alternator."+command, name, &val)
			checkErr("RPC failed", err)
			fmt.Println(string(val))
		case "LeaveRing":
			err = client.Call("Alternator."+command, struct{}{}, &struct{}{})
			checkErr("RPC failed", err)
		}
	} else {
		// Create a new node
		if joinPort != "0" { // Join an existing ring
			InitNode(port, addr+":"+joinPort)
		} else {
			InitNode(port, "") // Create a new ring
		}
	}
	return
}
