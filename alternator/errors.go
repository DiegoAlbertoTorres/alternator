package main

import (
	"errors"
	"log"
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
