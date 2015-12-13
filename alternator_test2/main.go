// package alternator_test implements a test for alternator. Note that it also takes care of
// creating a ring by calling the alternator binary, meaning that testing is completely automated.
package main

import (
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net/rpc"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"

	alt "github.com/DiegoAlbertoTorres/alternator"
)

const term = "konsole"
const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

var done int

// Config stores configuration options globally
var Config struct {
	diego     bool
	nNodes    int
	firstPort string
}

var altCmd = "alternator"
var rpcServ alt.RPCService
var peers []alt.Peer
var entryMap map[string][]byte

func main() {
	flag.BoolVar(&Config.diego, "diego", false, "assumes diego's linux environment, which has nice properties ;)")
	flag.IntVar(&Config.nNodes, "n", 8, "number of entries to be inserted")
	flag.StringVar(&Config.firstPort, "first", "38650", "number of the port of the first port")
	flag.Parse()

	if Config.nNodes < 1 {
		os.Exit(1)
	}

	rand.Seed(time.Now().UTC().UnixNano())
	rpcServ.Init()
	entryMap = make(map[string][]byte)

	// Start first
	if Config.diego {
		exec.Command("i3-msg", "workspace", "next").Run()
	}
	startNode("0", Config.firstPort)
	time.Sleep(200 * time.Millisecond)
	if Config.diego {
		exec.Command("i3-msg", "workspace", "prev").Run()
	}

	// Launch other nodes
	launchNodes(Config.nNodes - 1)
	getPeers()

	var peers []alt.Peer
	firstPeer := alt.Peer{ID: alt.GenID(Config.firstPort), Address: "127.0.0.1:" + Config.firstPort}
	err := rpcServ.MakeRemoteCall(&firstPeer, "GetMembers", struct{}{}, &peers)
	if err != nil {
		fmt.Println("Failed to get members from first node!")
		os.Exit(1)
	}

	scanner := bufio.NewScanner(os.Stdin)
	fmt.Println("Please execute a command:")
	for scanner.Scan() {
		args := strings.Split(scanner.Text(), " ")
		switch args[0] {
		case "create":
			if len(args) < 2 {
				fmt.Println("Usage: create [int]")
				break
			}
			n, err := strconv.Atoi(args[1])
			if checkErr("Usage: create [int]", err) {
				break
			}
			launchNodes(n)
		case "insert":
			if len(args) < 2 {
				fmt.Println("Usage: insert [int]")
				break
			}
			n, err := strconv.Atoi(args[1])
			if checkErr("Usage: insert [int]", err) {
				break
			}
			putKeys(n)
		case "test":
			testKeys()
		}
		fmt.Println("Please execute a command:")
	}
}

func testKeys() {
	correct := 0
	for name, v := range entryMap {
		var result []byte
		for {
			err := rpcServ.MakeRemoteCall(&peers[rand.Intn(len(peers))], "Get", name, &result)
			if err == nil || err.Error() == alt.ErrDataLost.Error() {
				break
			} else {
				fmt.Println("Get failed", err)
			}
		}
		if bytes.Compare(v, result) == 0 {
			correct++
		}
	}
	fmt.Printf("%d/%d entries are correct\n", correct, len(entryMap))
	if correct == len(entryMap) {
		fmt.Println("PERFECT!")
	}
}

func putKeys(n int) {
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		reps := randomIDs(peers)
		name := randString(10)
		v := []byte(randString(20))
		args := alt.PutArgs{Name: name, V: v, Replicants: reps, Success: 0}
		call := rpcServ.MakeAsyncCall(&peers[0], "Put", &args, &struct{}{})
		wg.Add(1)
		go func(call *rpc.Call, i int) {
			select {
			case reply := <-call.Done:
				if reply.Error != nil {
					fmt.Println(reply.Error)
					fmt.Printf("PUT for %v failed\n", reply.Args.(*alt.PutArgs).Name)
				} else {
					fmt.Println("Success!")
					entryMap[name] = v
				}
				wg.Done()
			case <-time.After(10000 * time.Millisecond):
				fmt.Println("Timeout!")
				wg.Done()
			}
		}(call, i)
	}
	wg.Wait()
	fmt.Printf("Done inserting %d keys\n", n)
}

func checkErr(str string, err error) bool {
	if err != nil {
		log.Print(str+": ", err)
		return true
	}
	return false
}

func launchNodes(n int) {
	// If on diego's machine, create on next workspace
	if Config.diego {
		exec.Command("i3-msg", "workspace", "next").Run()
	}
	// Launch other nodes
	for i := 0; i < n; i++ {
		startNode(Config.firstPort, "0")
		time.Sleep(200 * time.Millisecond)
	}
	if Config.diego {
		exec.Command("i3-msg", "workspace", "prev").Run()
	}
	return
}

func getPeers() {
	var rpcServ alt.RPCService
	rpcServ.Init()
	firstPeer := alt.Peer{ID: alt.GenID(Config.firstPort), Address: "127.0.0.1:" + Config.firstPort}
	err := rpcServ.MakeRemoteCall(&firstPeer, "GetMembers", struct{}{}, &peers)
	if err != nil {
		fmt.Println("Failed to get members from first node!")
		os.Exit(1)
	}
}

func startNode(join string, port string) {
	args := []string{"-e", altCmd}
	if join != "0" {
		args = append(args, "--join="+join)
	}
	if port != "0" {
		args = append(args, "--port="+port)
	}
	if port == Config.firstPort {
		args = append(args, "--fullKeys")
	}
	exec.Command("konsole", args...).Run()
}

func randString(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return string(b)
}

func getIDs(peers []alt.Peer) []alt.Key {
	var ids []alt.Key
	for _, peer := range peers {
		ids = append(ids, peer.ID)
	}
	return ids
}

func randomIDs(peers []alt.Peer) []alt.Key {
	n := rand.Intn(len(peers)-1) + 1

	for i := len(peers) - 1; i > 0; i-- {
		j := rand.Intn(i)
		peers[i], peers[j] = peers[j], peers[i]
	}

	var keys []alt.Key
	for i := 0; i < n; i++ {
		keys = append(keys, peers[i].ID)
	}
	return keys
}

func randomCmds(cmds []*exec.Cmd) []*exec.Cmd {
	// rand.Seed(time.Now().UTC().UnixNano())
	n := rand.Intn(len(cmds)-1) + 1

	for i := len(cmds) - 1; i > 0; i-- {
		j := rand.Intn(i)
		cmds[i], cmds[j] = cmds[j], cmds[i]
	}
	return cmds[0:n]
}
