package altrpc

import (
	"errors"
	"fmt"
	p "git/alternator/peer"
	"log"
	"net/rpc"
)

// ClientMap is a map of addresses to rpc clients
var ClientMap map[string]*rpc.Client

func init() {
	// Init connection map
	ClientMap = make(map[string]*rpc.Client)
}

// ErrDataLost occurs when metadata for a name was found, but not its data
var ErrDataLost = errors.New("data lost")

// MakeRemoteCall calls a function at a remote peer synchronously
func MakeRemoteCall(callee *p.Peer, call string, args interface{}, result interface{}) error {
	// Check if there is already a connection
	var client *rpc.Client
	var clientOpen bool
	var err error
	client, clientOpen = ClientMap[callee.Address]
	// Open if not
	if !clientOpen {
		client, err = rpc.DialHTTP("tcp", callee.Address)
		if err != nil {
			// Client must be down, ignore
			// log.Print("RPC dial failed, client "+callee.Address+" down? ", err)
			return err
		}
		ClientMap[callee.Address] = client
	}
	err = client.Call("Alternator."+call, args, result)
	if err != nil {
		log.Print("RPC call failed, client "+callee.Address+" down? ", err)
		Close(callee)
	}

	return err
}

// MakeAsyncCall calls a function at a remote peer asynchronously
func MakeAsyncCall(callee *p.Peer, call string, args interface{}, result interface{}) *rpc.Call {
	// Check if there is already a connection
	var client *rpc.Client
	var clientOpen bool
	var err error
	client, clientOpen = ClientMap[callee.Address]
	// Open if not
	if !clientOpen {
		client, err = rpc.DialHTTP("tcp", callee.Address)
		if err != nil {
			// Client must be down, ignore
			log.Print("Async RPC dial failed, client "+callee.Address+" down? ", err)
			return nil
		}
		ClientMap[callee.Address] = client
	}
	asyncCall := client.Go("Alternator."+call, args, result, nil)

	return asyncCall
}

// Close closes and RPC connection
func Close(node *p.Peer) {
	if node == nil {
		return
	}
	if client, exists := ClientMap[node.Address]; exists {
		fmt.Println("Closing connection to " + node.Address)
		client.Close()
		delete(ClientMap, node.Address)
	}
}
