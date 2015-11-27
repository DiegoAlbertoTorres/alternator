package main

import (
	"bytes"
	"encoding/gob"
)

const initSeed = 500

func intMax(a, b int) int {
	if a >= b {
		return a
	}
	return b
}

func bytesToMetadata(data []byte) (md Metadata) {
	buf := bytes.NewBuffer(data)
	dec := gob.NewDecoder(buf)
	dec.Decode(&md)
	return md
}

func serialize(obj interface{}) []byte {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(obj)
	checkErr("serialization failed", err)
	return buf.Bytes()
}

func getFirstReturn(f func(interface{}) (interface{}, interface{}), arg interface{}) interface{} {
	ret, _ := f(arg)
	return ret
}
