package alternator

import (
	"fmt"
	"log"
	"net/rpc"
	"reflect"
	"sync"
)

// RPCService provides bindings to make rpc calls to other nodes
type RPCService struct {
	sync.RWMutex
	clientMap map[string]*rpc.Client
}

// Init initializes an RPCService
func (rpcServ *RPCService) Init() {
	rpcServ.Lock()
	rpcServ.clientMap = make(map[string]*rpc.Client)
	rpcServ.Unlock()
}

// MakeRemoteCall calls a function at a remote peer synchronously
func (rpcServ *RPCService) MakeRemoteCall(callee *Peer, call string, args interface{}, result interface{}) error {
	if callee == nil {
		return nil
	}
	// Check if there is already a connection
	var client *rpc.Client
	var err error

	rpcServ.RLock()
	client = rpcServ.clientMap[callee.Address]
	rpcServ.RUnlock()

	// Open if not
	if client == nil {
		client, err = rpcServ.rpcConnect(callee)
		if err != nil {
			fmt.Println("RPC Connect failed!")
			return err
		}
	}
	err = client.Call("Node."+call, args, result)
	if err != nil {
		log.Print("RPC call failed, client "+callee.Address+" down? ", err)
		if err == rpc.ErrShutdown || reflect.TypeOf(err) == reflect.TypeOf((*rpc.ServerError)(nil)).Elem() {
			rpcServ.rpcClose(callee)
		}
	}

	return err
}

// MakeAsyncCall calls a function at a remote peer asynchronously
func (rpcServ *RPCService) MakeAsyncCall(callee *Peer, call string, args interface{}, result interface{}) *rpc.Call {
	if callee == nil {
		return nil
	}
	// Check if there is already a connection
	var client *rpc.Client

	rpcServ.RLock()
	client = rpcServ.clientMap[callee.Address]
	rpcServ.RUnlock()

	// Open if not
	var err error
	if client == nil {
		client, err = rpcServ.rpcConnect(callee)
		if err != nil {
			fmt.Println("RPC Connect failed!")
			return nil
		}
	}
	asyncCall := client.Go("Node."+call, args, result, nil)

	return asyncCall
}

func (rpcServ *RPCService) rpcConnect(node *Peer) (*rpc.Client, error) {
	client, err := rpc.DialHTTP("tcp", node.Address)
	rpcServ.Lock()
	rpcServ.clientMap[node.Address] = client
	rpcServ.Unlock()
	return client, err
}

// CloseIfBad closes a connection if err is a bad connection error
func (rpcServ *RPCService) CloseIfBad(err error, node *Peer) {
	if err == rpc.ErrShutdown || reflect.TypeOf(err) == reflect.TypeOf((*rpc.ServerError)(nil)).Elem() {
		rpcServ.rpcClose(node)
	}
}

// RPCClose closes and RPC connection
func (rpcServ *RPCService) rpcClose(node *Peer) {
	if node == nil {
		return
	}
	rpcServ.Lock()
	client, exists := rpcServ.clientMap[node.Address]
	if exists {
		client.Close()
		delete(rpcServ.clientMap, node.Address)
	}
	rpcServ.Unlock()
}
