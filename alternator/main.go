// package alternator compiles to a binary that launches a single alternator node. It takes command
// line arguments to use as the node's configuration. It can also be used as a command line tool to
// make RPC calls to existing nodes, although this is not the recommended way of interfacing with
// alternator, instead you should import the alternator library, the same way alternator_test
// package does.
package main

import (
	"crypto/sha1"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/rpc"
	"os"
	"time"

	"github.com/DiegoAlbertoTorres/alternator"
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
	flag.IntVar(&config.MemberSyncTime, "memberSyncTime", 500, "sets the time between membership syncs with a random node.")
	flag.IntVar(&config.HeartbeatTime, "heartbeatTime", 1000, "sets the amount of time between timeouts.")
	flag.IntVar(&config.ResolvePendingTime, "resolvePendingtime", 10000, "sets the amount of time (in ms) between attempts to resolve pending put operations.")
	flag.IntVar(&config.HeartbeatTimeout, "heartbeatTimeout", 400, "sets the amount of time before a heartbeat times out.")
	flag.IntVar(&config.PutMDTimeout, "putMDTimeout", 5000, "sets the amount of time before a PutMD times out.")
	flag.IntVar(&config.PutDataTimeout, "putDataTimeout", 5000, "sets the amount of time before a PutData times out.")
	flag.IntVar(&config.N, "n", 3, "sets the amount of nodes that replicate metadata.")
	flag.StringVar(&config.DotPath, "dotPath", os.Getenv("HOME")+"/.alternator/", "sets the directory for alternator's data.")
	flag.BoolVar(&config.FullKeys, "fullKeys", false, "if true all keys (hashes) are printed completely.")
	flag.BoolVar(&config.CPUProfile, "cpuprofile", false, "write cpu profile to file")
	flag.Parse()

	if command != "" {
		// Control panel mode
		client, err := rpc.DialHTTP("tcp", addr+":"+port)
		checkErr("dialing:", err)
		switch command {
		case "FindSuccessor":
			key := randomKey()
			fmt.Println("Finding successor of ", key)
			var reply alternator.Peer
			err = client.Call("Node."+command, key, &reply)
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
			putArgs := alternator.PutArgs{Name: name, V: val, Replicators: dests, Success: 0}
			err = client.Call("Node."+command, &putArgs, &struct{}{})
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
			err = client.Call("Node."+command, name, &val)
			checkErr("RPC failed", err)
			fmt.Println(string(val))
		case "LeaveRing":
			err = client.Call("Node."+command, struct{}{}, &struct{}{})
			checkErr("RPC failed", err)
		case "GetMembers":
			var members []alternator.Peer
			err = client.Call("Node."+command, struct{}{}, &members)
			for _, member := range members {
				fmt.Println(member)
			}
		case "DumpData":
			err = client.Call("Node."+command, struct{}{}, &struct{}{})
		case "DumpMetadata":
			err = client.Call("Node."+command, struct{}{}, &struct{}{})
		}
	} else {
		// Create a new node
		if joinPort != "0" { // Join an existing ring
			alternator.CreateNode(config, port, addr+":"+joinPort)
		} else {
			alternator.CreateNode(config, port, "") // Create a new ring
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

// randomKey produces a random key.
func randomKey() alternator.Key {
	h := sha1.New()
	rand.Seed(time.Now().UnixNano())
	io.WriteString(h, randString(100))
	return alternator.SliceToKey(h.Sum(nil))
}

func randString(n int) string {
	letterBytes := "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return string(b)
}
