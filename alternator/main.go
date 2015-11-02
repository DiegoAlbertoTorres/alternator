package main

import (
	"crypto/sha1"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/rpc"
	"os"
	"strconv"
)

func main() {
	var port string
	var addr string
	var command string
	var joinPort string

	flag.StringVar(&port, "port", "0", "port that the node will use for communication.")
	flag.StringVar(&addr, "target", "127.0.0.1", "target address for commands.")
	flag.StringVar(&command, "command", "", "name of command.")
	flag.StringVar(&joinPort, "join", "0", "joins the ring that the node at [address]:[port] belongs to.")
	flag.Parse()

	if command != "" {
		// Control panel mode
		client, err := rpc.DialHTTP("tcp", addr+":"+port)
		checkErr("dialing:", err)
		switch command {
		case "FindSuccessor":
			id := genID()
			fmt.Println("Finding successor of " + id)
			var reply ExtNode
			err = client.Call("AlterNode."+command, id, &reply)
			checkErr("RPC failed", err)
			fmt.Println(reply.String())
		case "Put":
			args := flag.Args()
			if len(args) < 2 {
				printExit("Usage: Put key value")
			}
			// Prepare key and value
			key := genKey(args[0])
			val := []byte(args[1])
			fmt.Println("Putting pair " + key + "," + args[1])
			err = client.Call("AlterNode."+command, &PutArgs{key, val}, &struct{}{})
			checkErr("RPC failed", err)
			fmt.Println("Success!")
		case "Get":
			args := flag.Args()
			if len(args) < 1 {
				printExit("Usage: Get key")
			}
			// Prepare key and value
			key := genKey(args[0])
			var val []byte
			fmt.Println("Getting " + key)
			err = client.Call("AlterNode."+command, key, &val)
			checkErr("RPC failed", err)
			fmt.Println(string(val))
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

func genKey(str string) string {
	h := sha1.New()
	rand.Seed(10)
	io.WriteString(h, str+strconv.Itoa(rand.Int()))
	return fmt.Sprintf("%x", h.Sum(nil))
}

func printExit(msg string) {
	fmt.Println(msg)
	os.Exit(1)
}

func checkErr(str string, err error) {
	if err != nil {
		log.Fatal(str+" ", err)
	}
}
