package alternator

import (
	"bytes"
	"crypto/sha1"
	"encoding/gob"
	"io"
	"math/rand"
	"net"
	"os"
)

func intMax(a, b int) int {
	if a >= b {
		return a
	}
	return b
}

// GenID generates the ID of a node given its address.
func GenID(address string) Key {
	h := sha1.New()
	rand.Seed(initSeed)
	io.WriteString(h, address)
	return SliceToKey(h.Sum(nil))
}

// bytesToMetadata converts a byte array into a metadata struct
func bytesToMetadata(data []byte) (md Metadata) {
	buf := bytes.NewBuffer(data)
	dec := gob.NewDecoder(buf)
	dec.Decode(&md)
	return md
}

func bytesToPendingPut(data []byte) (pp PendingPut) {
	buf := bytes.NewBuffer(data)
	dec := gob.NewDecoder(buf)
	dec.Decode(&pp)
	return pp
}

// serialize serializes an object into an array of bytes
func serialize(obj interface{}) []byte {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(obj)
	checkErr("serialization failed", err)
	return buf.Bytes()
}

func getIP() string {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		os.Stderr.WriteString("Oops: " + err.Error() + "\n")
		os.Exit(1)
	}

	for _, a := range addrs {
		if ipnet, ok := a.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				return ipnet.IP.String()
			}
		}
	}
	return ""
}
