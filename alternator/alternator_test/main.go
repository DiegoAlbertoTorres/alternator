package main

import (
	"bytes"
	"flag"
	"fmt"
	"git/alternator/altrpc"
	"git/alternator/altutil"
	k "git/alternator/key"
	p "git/alternator/peer"
	"math/rand"
	"net/rpc"
	"os/exec"
	"strconv"
	"sync"
	"time"

	// . "git/alternator"
)

const term = "konsole"
const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

var done int

// PutArgs is a struct to represent the arguments of Put or DBPut
type PutArgs struct {
	Name       string
	V          []byte
	Replicants []k.Key
	Success    int
}

// Config stores configuration options globally
var Config struct {
	diego    bool
	nEntries int
}

func main() {
	flag.BoolVar(&Config.diego, "diego", false, "assumes diego's linux environment, which has nice properties ;)")
	flag.IntVar(&Config.nEntries, "entries", 100, "number of entries to be inserted")
	flag.Parse()

	rand.Seed(time.Now().UTC().UnixNano())

	ports := []int{38650, 34001, 50392, 43960, 56083, 54487, 56043, 33846}
	// ports := []int{38650, 50392, 56083, 56043}
	nPeers := len(ports)
	ids := makeIDs(ports)
	peers := makePeers(ports)

	verificationMap := make(map[string][]byte, Config.nEntries)

	if Config.diego {
		exec.Command("i3-msg", "workspace", "next").Run()
	}
	// Start first
	exec.Command(term, "-e", "alternator", "--port="+strconv.Itoa(ports[0])).Run()

	// Launch other nodes
	for _, port := range ports[1:] {
		exec.Command(term, "-e", "alternator", "--join="+strconv.Itoa(ports[0]), "--port="+strconv.Itoa(port)).Run()
		time.Sleep(200 * time.Millisecond)
	}

	time.Sleep(3 * time.Second)

	if Config.diego {
		exec.Command("i3-msg", "workspace", "prev").Run()
	}

	var wg sync.WaitGroup
	// Randomly generate nEntries, insert them to Alternator
	for i := 0; i < Config.nEntries; i++ {
		name := randString(10)
		v := []byte(randString(20))
		reps := randomIDs(ids)
		fmt.Printf("PUT %v, w/e in %v\n", name, reps)
		// Insert it into Alternator
		args := PutArgs{Name: name, V: v, Replicants: reps, Success: 0}
		// Insert into own map for later verification
		verificationMap[name] = v
		call := altrpc.MakeAsyncCall(&peers[rand.Intn(nPeers)], "Put", &args, &struct{}{})
		wg.Add(1)
		go func(call *rpc.Call, i int) {
			defer wg.Done()
			reply := <-call.Done
			fmt.Printf("Finished %d\n", i)
			if reply.Error != nil {
				fmt.Printf("PUT for %v failed\n", reply.Args.(*PutArgs).Name)
			}
		}(call, i)
	}

	wg.Wait()

	// Now check each entry
	correct := 0
	for name, v := range verificationMap {
		var result []byte
		err := altrpc.MakeRemoteCall(&peers[rand.Intn(nPeers)], "Get", name, &result)
		if err != nil {
			fmt.Println("Get failed!!")
		}
		if bytes.Compare(v, result) == 0 {
			correct++
		}
	}
	fmt.Printf("%d/%d entries are correct\n", correct, Config.nEntries)
	if correct == Config.nEntries {
		fmt.Println("PERFECT!")
	}
}

func randString(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return string(b)
}

func makeIDs(ports []int) []k.Key {
	var ids []k.Key
	for i := range ports {
		ids = append(ids, altutil.GenID(strconv.Itoa(ports[i])))
	}
	return ids
}

func makePeers(ports []int) []p.Peer {
	var peers []p.Peer
	for i := range ports {
		peer := p.Peer{ID: altutil.GenID(strconv.Itoa(ports[i])), Address: "127.0.0.1:" + strconv.Itoa(ports[i])}
		peers = append(peers, peer)
	}
	return peers
}

func randomIDs(ids []k.Key) []k.Key {
	n := rand.Intn(len(ids)-1) + 1

	for i := len(ids) - 1; i > 0; i-- {
		j := rand.Intn(i)
		ids[i], ids[j] = ids[j], ids[i]
	}
	return ids[0:n]
}
