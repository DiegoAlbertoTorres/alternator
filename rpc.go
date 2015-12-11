package alternator

import (
	"fmt"
	"log"
	"net/rpc"
	"sync"
)

// RPCService provides bindings to make rpc calls to other nodes
type RPCService struct {
	clientMap   map[string]*rpc.Client
	clientMutex sync.RWMutex
}

// Init initializes an RPCService
func (rpcServ *RPCService) Init() {
	rpcServ.clientMutex.Lock()
	rpcServ.clientMap = make(map[string]*rpc.Client)
	rpcServ.clientMutex.Unlock()
}

// MakeRemoteCall calls a function at a remote peer synchronously
func (rpcServ *RPCService) MakeRemoteCall(callee *Peer, call string, args interface{}, result interface{}) error {
	// Check if there is already a connection
	var client *rpc.Client
	var clientOpen bool
	var err error

	rpcServ.clientMutex.RLock()
	client, clientOpen = rpcServ.clientMap[callee.Address]
	rpcServ.clientMutex.RUnlock()

	// Open if not
	if !clientOpen {
		client, err = rpc.DialHTTP("tcp", callee.Address)
		if err != nil {
			// Client must be down, ignore
			// log.Print("RPC dial failed, client "+callee.Address+" down? ", err)
			return err
		}
		rpcServ.clientMutex.Lock()
		rpcServ.clientMap[callee.Address] = client
		rpcServ.clientMutex.Unlock()
	}
	err = client.Call("Node."+call, args, result)
	if err != nil {
		log.Print("RPC call failed, client "+callee.Address+" down? ", err)
		rpcServ.rpcClose(callee)
	}

	return err
}

// MakeAsyncCall calls a function at a remote peer asynchronously
func (rpcServ *RPCService) MakeAsyncCall(callee *Peer, call string, args interface{}, result interface{}) *rpc.Call {
	// Check if there is already a connection
	var client *rpc.Client
	var clientOpen bool
	var err error

	rpcServ.clientMutex.RLock()
	client, clientOpen = rpcServ.clientMap[callee.Address]
	rpcServ.clientMutex.RUnlock()

	// Open if not
	if !clientOpen {
		client, err = rpc.DialHTTP("tcp", callee.Address)
		if err != nil {
			// Client must be down, ignore
			log.Print("Async RPC dial failed, client "+callee.Address+" down? ", err)
			return nil
		}
		rpcServ.clientMutex.Lock()
		rpcServ.clientMap[callee.Address] = client
		rpcServ.clientMutex.Unlock()
	}
	asyncCall := client.Go("Node."+call, args, result, nil)

	return asyncCall
}

// RPCClose closes and RPC connection
func (rpcServ *RPCService) rpcClose(node *Peer) {
	if node == nil {
		return
	}

	rpcServ.clientMutex.RLock()
	client, exists := rpcServ.clientMap[node.Address]
	rpcServ.clientMutex.RUnlock()
	if exists {
		fmt.Println("Closing connection to " + node.Address)
		client.Close()
		rpcServ.clientMutex.Lock()
		delete(rpcServ.clientMap, node.Address)
		rpcServ.clientMutex.Unlock()
	}
}
