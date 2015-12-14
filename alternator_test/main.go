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
var mapLock sync.RWMutex

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
			time.Sleep(5000 * time.Millisecond)
			getPeers()
		case "insert":
			if len(args) < 2 {
				fmt.Println("Usage: insert [int]")
				break
			}
			n, err := strconv.Atoi(args[1])
			if checkErr("Usage: insert [int]", err) {
				break
			}
			getPeers()
			putKeys(n)
		case "insertseq":
			if len(args) < 2 {
				fmt.Println("Usage: insertseq [int]")
				break
			}
			n, err := strconv.Atoi(args[1])
			if checkErr("Usage: insert [int]", err) {
				break
			}
			getPeers()
			putKeysSeq(n)
		case "dumpdata":
			dumpData()
		case "dumpmeta":
			dumpMetadata()
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

func putKeysSeq(n int) {
	rand.Seed(time.Now().UTC().UnixNano())
	for i := 0; i < n; i++ {
		reps := randomIDs(peers)
		name := randString(30)
		v := []byte(randString(30))
		args := alt.PutArgs{Name: name, V: v, Replicants: reps, Success: 0}
		// // Wait a bit to avoid overflowing
		err := rpcServ.MakeRemoteCall(&peers[rand.Intn(len(peers))], "Put", &args, &struct{}{})
		if err != nil {
			fmt.Printf("PUT for %v failed, %v\n", name, err)
		} else {
			// fmt.Println("Success!")
			entryMap[name] = v
		}
		fmt.Printf("Done inserting %d keys\n", n)
	}
}

func putKeys(n int) {
	rand.Seed(time.Now().UTC().UnixNano())
	success := 0
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		reps := randomIDs(peers)
		name := randString(30)
		v := []byte(randString(30))
		args := alt.PutArgs{Name: name, V: v, Replicants: reps, Success: 0}
		// fmt.Printf("Execing %d\n", i)
		peer := &peers[rand.Intn(len(peers))]
		call := rpcServ.MakeAsyncCall(peer, "Put", &args, &struct{}{})
		fmt.Printf("Putting on %v\n", peer.ID)
		wg.Add(1)
		go func(call *rpc.Call, start time.Time, peer *alt.Peer) {
			defer wg.Done()
			select {
			case reply := <-call.Done:
				if reply.Error == nil {
					// elapsed := time.Since(start)
					// fmt.Printf("Took %s\n", elapsed)
					mapLock.Lock()
					entryMap[name] = v
					success++
					mapLock.Unlock()
				} else {
					rpcServ.CloseIfBad(reply.Error, peer)
					fmt.Printf("PUT for %v failed, %v\n", reply.Args.(*alt.PutArgs).Name, reply.Error)
				}
				// case <-time.After(5000 * time.Millisecond):
				// 	mapLock.Lock()
				// 	entryMap[name] = v
				// 	mapLock.Unlock()
			}
		}(call, time.Now(), peer)
		// time.Sleep(50 * time.Millisecond)
	}
	wg.Wait()
	fmt.Printf("Done inserting %d keys\n", n)
	fmt.Printf("Success count is %d\n", success)
}

func dumpData() {
	for i := range peers {
		rpcServ.MakeRemoteCall(&peers[i], "DumpData", struct{}{}, &struct{}{})
	}
}

func dumpMetadata() {
	for i := range peers {
		rpcServ.MakeRemoteCall(&peers[i], "DumpMetadata", struct{}{}, &struct{}{})
	}
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
	args := []string{"--hold", "-e", altCmd}
	if join != "0" {
		args = append(args, "--join="+join)
	}
	if port != "0" {
		args = append(args, "--port="+port)
	}
	if port == Config.firstPort {
		// args = append(args, "--fullKeys")
		args = append(args, "--cpuprofile")
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
	// n := rand.Intn(2) + 1
	n := 1

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
