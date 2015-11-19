package main

import (
	"errors"
	"fmt"
	"log"
	"os"
)

const (
	// ErrNilPredecessor specifies a nil predecessor error
	ErrNilPredecessor = "nil predecessor"
)

var (
	// ErrKeyNotFound occurs when a Get was made for an unexisting key
	ErrKeyNotFound = errors.New("key not found")
)

func assertRemoteErr(err error, typ string) bool {
	if err == nil {
		return false
	} else if err.Error() == typ {
		// Matched error type
		return true
	} else {
		// Log the incident
		log.Print("Unexpected error ", err)
		return false
	}
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

func checkLogErr(err error) bool {
	if err != nil {
		log.Print(err)
		return true
	}
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
