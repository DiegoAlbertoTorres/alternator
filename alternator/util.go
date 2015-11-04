package main

import (
	"crypto/sha1"
	"fmt"
	"io"
	"log"
	"math"
	"math/rand"
	"os"
	"strconv"
)

// genKey generates a key for a given string
func genKey(str string) []byte {
	h := sha1.New()
	rand.Seed(10)
	io.WriteString(h, str+strconv.Itoa(rand.Int()))
	return h.Sum(nil)
}

// Returns true if no error
func checkFatal(err error) bool {
	if err != nil {
		// Exits
		log.Fatal(err)
	}
	// No fatal
	return false
}

func printExit(msg string) {
	fmt.Println(msg)
	os.Exit(1)
}

func checkErr(str string, err error) {
	if err != nil {
		log.Fatal(str+" ", err)
	}
}

// Creates terminal truecolor escape sequence for the given key
func keyToXColor(key []byte) string {
	// Convert to [0-1] range
	f := float64(key[0]) / float64(255)
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

func keyToString(key []byte) string {
	return fmt.Sprintf(keyToXColor(key)+"%x\x1b[0m", key)
}
