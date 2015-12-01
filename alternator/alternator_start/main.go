package main

import (
	"encoding/hex"
	"flag"
	"fmt"
	"git/alternator"
	"log"
	"net/rpc"
	"os"
)

var config alternator.Config

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
	flag.IntVar(&config.MemberSyncTime, "memberSyncTime", 400, "sets the time between membership syncs with a random node.")
	flag.IntVar(&config.HeartbeatTime, "heartbeatTime", 1000, "sets the amount of time between timeouts.")
	flag.IntVar(&config.HeartbeatTimeout, "heartbeatTimeout", 400, "sets the amount of time before a heartbeat timesout.")
	flag.IntVar(&config.N, "n", 2, "sets the amount of nodes that replicate metadata.")
	flag.StringVar(&config.DotPath, "dotPath", os.Getenv("HOME")+"/.alternator/", "sets the directory for alternator's data.")
	flag.BoolVar(&config.FullKeys, "fullKeys", false, "if true all keys (hashes) are printed completely.")
	flag.Parse()

	if command != "" {
		// Control panel mode
		client, err := rpc.DialHTTP("tcp", addr+":"+port)
		checkErr("dialing:", err)
		switch command {
		case "FindSuccessor":
			key := alternator.RandomKey()
			fmt.Println("Finding successor of ", key)
			var reply alternator.Peer
			err = client.Call("Alternator."+command, key, &reply)
			checkErr("RPC failed", err)
			fmt.Println(reply.String())
		case "Put":
			args := flag.Args()
			if len(args) < 2 {
				fmt.Println("Usage: Put key value dest1 dest2 dest3...")
				os.Exit(1)
			}
			// Prepare key and value
			name := args[0]
			val := []byte(args[1])
			var dests []alternator.Key
			if len(args[2:]) < 0 {
				fmt.Println("Need to give at least one destination node")
				return
			}
			for _, arg := range args[2:] {
				dest, _ := hex.DecodeString(arg)
				dests = append(dests, alternator.SliceToKey(dest))
			}

			fmt.Println("Putting pair " + name + "," + args[1])
			putArgs := alternator.PutArgs{Name: name, V: val, Replicants: dests, Success: 0}
			err = client.Call("Alternator."+command, &putArgs, &struct{}{})
			checkErr("RPC failed", err)
			fmt.Println("Success!")
		case "Get":
			args := flag.Args()
			if len(args) < 1 {
				fmt.Println("Usage: Get key")
				os.Exit(1)
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
			alternator.InitNode(config, port, addr+":"+joinPort)
		} else {
			alternator.InitNode(config, port, "") // Create a new ring
		}
	}
	return
}

func checkErr(str string, err error) bool {
	if err != nil {
		log.Print(str+": ", err)
		return true
	}
	return false
}
