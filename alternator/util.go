package alternator

import (
	"crypto/sha1"
	"io"
	"math/rand"
	"os"
	"strconv"
)

func intMax(a, b int) int {
	if a >= b {
		return a
	}
	return b
}

// GenID generates the ID of a node given its port
func GenID(port string) Key {
	hostname, _ := os.Hostname()
	h := sha1.New()
	rand.Seed(initSeed)
	io.WriteString(h, hostname+port+strconv.Itoa(rand.Int()))
	return SliceToKey(h.Sum(nil))
}
