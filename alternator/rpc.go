package main

import (
	"fmt"
	"log"
	"net/rpc"
)

// makeRemoteCall calls a function at a remote node
func makeRemoteCall(callee *ExtNode, call string, args interface{}, result interface{}) error {
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
		closeRPC(callee)
	}

	return err
}

func closeRPC(node *ExtNode) {
	if node == nil {
		return
	}
	if client, exists := ClientMap[node.Address]; exists {
		fmt.Println("Closing connection to " + node.Address)
		client.Close()
		delete(ClientMap, node.Address)
	}
}
