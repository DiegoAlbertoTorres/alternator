package alternator

import (
	"fmt"
	"log"
	"net/rpc"
)

// clientMap is a map of addresses to rpc clients
var clientMap map[string]*rpc.Client

func init() {
	// Init connection map
	clientMap = make(map[string]*rpc.Client)
}

// MakeRemoteCall calls a function at a remote peer synchronously
func MakeRemoteCall(callee *Peer, call string, args interface{}, result interface{}) error {
	// Check if there is already a connection
	var client *rpc.Client
	var clientOpen bool
	var err error
	client, clientOpen = clientMap[callee.Address]
	// Open if not
	if !clientOpen {
		client, err = rpc.DialHTTP("tcp", callee.Address)
		if err != nil {
			// Client must be down, ignore
			// log.Print("RPC dial failed, client "+callee.Address+" down? ", err)
			return err
		}
		clientMap[callee.Address] = client
	}
	err = client.Call("Node."+call, args, result)
	if err != nil {
		log.Print("RPC call failed, client "+callee.Address+" down? ", err)
		rpcClose(callee)
	}

	return err
}

// MakeAsyncCall calls a function at a remote peer asynchronously
func MakeAsyncCall(callee *Peer, call string, args interface{}, result interface{}) *rpc.Call {
	// Check if there is already a connection
	var client *rpc.Client
	var clientOpen bool
	var err error
	client, clientOpen = clientMap[callee.Address]
	// Open if not
	if !clientOpen {
		client, err = rpc.DialHTTP("tcp", callee.Address)
		if err != nil {
			// Client must be down, ignore
			log.Print("Async RPC dial failed, client "+callee.Address+" down? ", err)
			return nil
		}
		clientMap[callee.Address] = client
	}
	asyncCall := client.Go("Node."+call, args, result, nil)

	return asyncCall
}

// RPCClose closes and RPC connection
func rpcClose(node *Peer) {
	if node == nil {
		return
	}
	if client, exists := clientMap[node.Address]; exists {
		fmt.Println("Closing connection to " + node.Address)
		client.Close()
		delete(clientMap, node.Address)
	}
}
