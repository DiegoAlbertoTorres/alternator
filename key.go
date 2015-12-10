package alternator

import (
	"bytes"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"io"
	"math"
	"math/rand"
	"strconv"
	"time"
	"unsafe"
)

const initSeed = 500

var maxSlice, _ = hex.DecodeString("ffffffffffffffffffffffffffffffffffffffff")
var minSlice, _ = hex.DecodeString("0000000000000000000000000000000000000000")

// MaxKey is the highest possible key
var MaxKey = SliceToKey(maxSlice)

// MinKey is the lowest possible key
var MinKey = SliceToKey(minSlice)

// Key is a sha1 hash. A key is used as the primary of the consistent hashing scheme.
type Key [sha1.Size]byte

// Compare compares two keys, behaves just like bytes.Compare
func (k Key) Compare(other Key) int {
	return bytes.Compare(k[:], other[:])
}

// SliceToKey converts a slice to a key (an alias for an array)
func SliceToKey(src []byte) (dst Key) {
	// dst = (key)(&src[0])
	dst = *(*[sha1.Size]byte)(unsafe.Pointer(&src[0]))
	return
}

// inRange checks if test is in the key range [from, to]
func inRange(test, from, to Key) bool {
	if from.Compare(to) < 0 {
		return (test.Compare(from) > 0) && (test.Compare(to) < 0)
	} else if from.Compare(to) > 0 {
		return ((test.Compare(from) > 0) && (test.Compare(MaxKey) <= 0)) ||
			((test.Compare(to) < 0) && (test.Compare(MinKey) >= 0))
	} else {
		return (test.Compare(from) != 0)
	}
}

// StringToKey hashes a string, returning the hash as a key
func StringToKey(str string) Key {
	h := sha1.New()
	io.WriteString(h, str)
	return SliceToKey(h.Sum(nil))
}

// RandomKey returns a random key
func RandomKey() Key {
	h := sha1.New()
	rand.Seed(time.Now().UnixNano())
	io.WriteString(h, strconv.Itoa(rand.Int()))
	return SliceToKey(h.Sum(nil))
}

func (k Key) String() string {
	keyOut := k[0:10]
	if fullKeys {
		keyOut = k[:]
	}
	// Only first ten characters for simplicity's sake
	return fmt.Sprintf(k.xColor()+"%x\x1b[0m", keyOut)
}

// xColor creates terminal truecolor escape sequence for the given key
func (k Key) xColor() string {
	if len(k) < 1 {
		return ""
	}
	// Convert to [0-1] range
	f := float64(k[0]) / float64(255)
	/*convert to long rainbow RGB*/
	a := (1 - f) / 0.2
	X := math.Floor(a)
	Y := int(math.Floor(255 * (a - X)))
	// fmt.Printf("f: %f, a: %f, x: %f, y: %d\n", f, a, X, Y)
	var r, g, b int
	switch X {
	case 0:
		r = 255
		g = Y
		b = 0
		break
	case 1:
		r = 255 - Y
		g = 255
		b = 0
		break
	case 2:
		r = 0
		g = 255
		b = Y
		break
	case 3:
		r = 0
		g = 255 - Y
		b = 255
		break
	case 4:
		r = Y
		g = 0
		b = 255
		break
	case 5:
		r = 255
		g = 0
		b = 255
		break
	}
	// Use first three bytes as RGB code
	return fmt.Sprintf("\x1b[38;2;%d;%d;%dm", r, g, b)
}
