package alternator

import (
	"bytes"
	"crypto/sha1"
	"fmt"
	"io"
	"math"
	"unsafe"
)

const initSeed = 500

// Key is a sha1 hash. A key is used as the primary identifier of entries in Alternator.
// Additionally, node identifiers are also a key, and so nodes share the key-space with entries,
// so that responsibility for key-value pairs is assigned to the successor of the key.
type Key [sha1.Size]byte

// MaxKey is the highest possible key in the key-space.
var MaxKey = [sha1.Size]byte{1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1}

// MinKey is the lowest possible key in the key-space.
var MinKey = [sha1.Size]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}

func (k Key) String() string {
	keyOut := k[:]
	if !fullKeys {
		keyOut = k[0:10]
	}
	return fmt.Sprintf(k.xColor()+"%x\x1b[0m", keyOut)
}

// Compare compares two keys. Behaves just like bytes.Compare.
func (k Key) Compare(other Key) int {
	return bytes.Compare(k[:], other[:])
}

// SliceToKey converts a slice to a Key (which is an alias for a byte array).
func SliceToKey(src []byte) (dst Key) {
	dst = *(*[sha1.Size]byte)(unsafe.Pointer(&src[0]))
	return
}

// inRange checks if test is in the key range [from, to].
func inRange(test, from, to Key) bool {
	if from.Compare(to) < 0 {
		return (test.Compare(from) > 0) && (test.Compare(to) < 0)
	} else if from.Compare(to) > 0 {
		return ((test.Compare(from) > 0) && (test.Compare(MaxKey) <= 0)) ||
			((test.Compare(to) < 0) && (test.Compare(MinKey) >= 0))
	} else {
		return (test.Compare(from) == 0) || (test.Compare(to) == 0)
	}
}

// StringToKey hashes a string, returning the hash as a key
func StringToKey(str string) Key {
	h := sha1.New()
	io.WriteString(h, str)
	return SliceToKey(h.Sum(nil))
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
