package alternator

import (
	"bytes"
	"crypto/sha1"
	"encoding/gob"
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

// bytesToMetadata converts a byte array into a metadata struct
func bytesToMetadata(data []byte) (md Metadata) {
	buf := bytes.NewBuffer(data)
	dec := gob.NewDecoder(buf)
	dec.Decode(&md)
	return md
}

// serialize serializes an object into an array of bytes
func serialize(obj interface{}) []byte {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(obj)
	checkErr("serialization failed", err)
	return buf.Bytes()
}
