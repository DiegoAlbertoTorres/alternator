package altutil

import (
	"crypto/sha1"
	k "git/alternator/key"
	"io"
	"math/rand"
	"os"
	"strconv"
)

const initSeed = 500

func intMax(a, b int) int {
	if a >= b {
		return a
	}
	return b
}

// GenID generates the ID of a node given its port
func GenID(port string) k.Key {
	hostname, _ := os.Hostname()
	h := sha1.New()
	rand.Seed(initSeed)
	io.WriteString(h, hostname+port+strconv.Itoa(rand.Int()))
	return k.SliceToKey(h.Sum(nil))
}
