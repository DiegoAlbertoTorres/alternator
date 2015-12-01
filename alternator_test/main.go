package main

import (
	"bytes"
	"flag"
	"fmt"
	alt "git/alternator"
	"math/rand"
	"net/rpc"
	"os/exec"
	"strconv"
	"sync"
	"time"
)

const term = "konsole"
const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

var done int

// Config stores configuration options globally
var Config struct {
	diego    bool
	nEntries int
}

var altCmd = "alternator_start"

func main() {
	flag.BoolVar(&Config.diego, "diego", false, "assumes diego's linux environment, which has nice properties ;)")
	flag.IntVar(&Config.nEntries, "entries", 100, "number of entries to be inserted")
	flag.Parse()

	ports := []int{38650, 34001, 50392, 43960, 56083, 54487, 56043, 33846}
	// ports := []int{38650, 50392, 56083, 56043}
	nPeers := len(ports)
	ids := makeIDs(ports)
	peers := makePeers(ports)
	var cmds []*exec.Cmd

	verificationMap := make(map[string][]byte, Config.nEntries)
	rand.Seed(time.Now().UTC().UnixNano())

	if Config.diego {
		exec.Command("i3-msg", "workspace", "next").Run()
	}
	// Start first
	cmd := exec.Command(term, "-e", altCmd, "--port="+strconv.Itoa(ports[0]))
	cmds = append(cmds, cmd)
	cmd.Start()

	// Launch other nodes
	for _, port := range ports[1:] {
		cmd = exec.Command(term, "-e", altCmd, "--join="+strconv.Itoa(ports[0]), "--port="+strconv.Itoa(port))
		cmds = append(cmds, cmd)
		cmd.Start()
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
		args := alt.PutArgs{Name: name, V: v, Replicants: reps, Success: 0}
		// Insert into own map for later verification
		verificationMap[name] = v
		call := alt.MakeAsyncCall(&peers[rand.Intn(nPeers)], "Put", &args, &struct{}{})
		wg.Add(1)
		go func(call *rpc.Call, i int) {
			defer wg.Done()
			reply := <-call.Done
			// fmt.Printf("Finished %d\n", i)
			if reply.Error != nil {
				fmt.Printf("PUT for %v failed\n", reply.Args.(*alt.PutArgs).Name)
			}
		}(call, i)
	}

	wg.Wait()

	// Kill some processes
	// fmt.Printf("There are %d cmds\n", len(cmds))
	// cmds = randomCmds(cmds)
	// fmt.Printf("There are %d random cmds\n", len(cmds))
	// for _, cmd := range cmds {
	// 	fmt.Println("killing someone")
	// 	cmd.Process.Signal(os.Interrupt)
	// 	// cmd.Process.Kill()
	// }

	fmt.Println("kill some stuff!")
	time.Sleep(5 * time.Second)

	// Now check each entry
	correct := 0
	for name, v := range verificationMap {
		var result []byte
		for {
			err := alt.MakeRemoteCall(&peers[rand.Intn(nPeers)], "Get", name, &result)
			if err == nil || err.Error() == alt.ErrDataLost.Error() {
				break
			} else {
				fmt.Println("rofl", err)
			}
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

func makeIDs(ports []int) []alt.Key {
	var ids []alt.Key
	for i := range ports {
		ids = append(ids, alt.GenID(strconv.Itoa(ports[i])))
	}
	return ids
}

func makePeers(ports []int) []alt.Peer {
	var peers []alt.Peer
	for i := range ports {
		peer := alt.Peer{ID: alt.GenID(strconv.Itoa(ports[i])), Address: "127.0.0.1:" + strconv.Itoa(ports[i])}
		peers = append(peers, peer)
	}
	return peers
}

func randomIDs(ids []alt.Key) []alt.Key {
	// rand.Seed(time.Now().UTC().UnixNano())
	n := rand.Intn(len(ids)-1) + 1

	for i := len(ids) - 1; i > 0; i-- {
		j := rand.Intn(i)
		ids[i], ids[j] = ids[j], ids[i]
	}
	return ids[0:n]
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
