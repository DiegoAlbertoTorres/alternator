package main

import (
	"bytes"
	"crypto/sha1"
	"fmt"
	"io"
	"math"
	"math/rand"
	"strconv"
	"time"
	"unsafe"
)

// Key is the sha1sum of a value
type Key [sha1.Size]byte

func keyCompare(a, b Key) int {
	return bytes.Compare(a[:], b[:])
}

func sliceToKey(src []byte) (dst Key) {
	// dst = (key)(&src[0])
	dst = *(*[sha1.Size]byte)(unsafe.Pointer(&src[0]))
	return
}

// Checks if test is in the range (from, to) (non-inclusive)
func inRange(test, from, to Key) bool {
	if keyCompare(from, to) < 0 {
		return (keyCompare(test, from) > 0) && (keyCompare(test, to) < 0)
	} else if keyCompare(from, to) > 0 {
		return ((keyCompare(test, from) > 0) && (keyCompare(test, maxKey) <= 0)) ||
			((keyCompare(test, to) < 0) && (keyCompare(test, minKey) >= 0))
	} else {
		return (keyCompare(test, from) != 0)
	}
}

// genKey generates a key for a given string
func stringToKey(str string) Key {
	h := sha1.New()
	rand.Seed(initSeed)
	io.WriteString(h, str+strconv.Itoa(rand.Int()))
	return sliceToKey(h.Sum(nil))
}

func randomKey() Key {
	h := sha1.New()
	rand.Seed(time.Now().UnixNano())
	io.WriteString(h, strconv.Itoa(rand.Int()))
	return sliceToKey(h.Sum(nil))
}

func keyToString(k Key) string {
	return fmt.Sprintf(keyToXColor(k)+"%x\x1b[0m", k)
}

// Creates terminal truecolor escape sequence for the given key
func keyToXColor(k Key) string {
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
