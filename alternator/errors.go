package alternator

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

	// ErrDataLost occurs when metadata for a name was found, but not its data
	ErrDataLost = errors.New("data lost")

	// ErrPutMDFail occurs when a node fails to put the metadata in N/2 successors
	ErrPutMDFail = errors.New("failed to put metadata in n/2 successors")

	// ErrPutFail occurs when a node fails to put the data in required amount of nodes
	ErrPutFail = errors.New("failed to replicate in required amount of nodes")

	// ErrJoinFail occurs when a join request fails
	ErrJoinFail = errors.New("join request failed")

	// ErrLeaveFail occurs when a leave fails
	ErrLeaveFail = errors.New("leave fail")

	// ErrBatchPutIncomplete occurs when a batch put fails to put some pairs in the DB
	ErrBatchPutIncomplete = errors.New("batch operation did not put all pairs successfully")
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
func checkFatal(msg string, err error) bool {
	if err != nil {
		// Exits
		log.Fatal(msg+": ", err)
	}
	// No fatal
	return false
}

func printExit(msg string) {
	fmt.Println(msg)
	os.Exit(1)
}

func checkErr(str string, err error) bool {
	if err != nil {
		log.Print(str+": ", err)
		return true
	}
	return false
}
