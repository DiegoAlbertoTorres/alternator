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
		fmt.Println("Attempting connection to " + callee.Address)
		client, err = rpc.DialHTTP("tcp", callee.Address)
		if err != nil {
			// Client must be down, ignore
			log.Print("RPC dial failed, client down? "+callee.Address, err)
			return nil
		}
		ClientMap[callee.Address] = client
	}
	err = client.Call("Alernator."+call, args, result)
	if err != nil {
		log.Print("RPC call failed, Client down? "+callee.Address, err)
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
