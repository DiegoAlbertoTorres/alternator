package alternator

import (
	"fmt"
	"log"
	"net/rpc"
	"reflect"
	"sync"
)

// RPCService provides an interface to make RPC calls to other nodes. Currently, RPCService only
// uses HTTP as a network channel. To communicate with other nodes first the RPCService must be
// initialized by a call to RPCService.Init(). Then, MakeRemoteCall and MakeAsyncCall can be used to
// call functions exposed by other nodes.
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

// MakeRemoteCall calls a function at a remote peer 'callee' synchronously. The usage of the three
// last arguments is identical to that of net/rpc's '(client *Client) Call' function.
func (rpcServ *RPCService) MakeRemoteCall(callee *Peer, call string, args interface{},
	result interface{}) error {
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

// MakeAsyncCall calls a function at a remote peer 'callee' asynchronously. The three last arguments
// are identical to that of net/rpc's '(client *Client) Go' function.
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

func (rpcServ *RPCService) rpcConnect(peer *Peer) (*rpc.Client, error) {
	// A client need not be dialed by HTTP. To implement other protocols all that
	// is needed is to identify the protocol used by the Peer's address and
	// create a client for the correct protocol.
	client, err := rpc.DialHTTP("tcp", peer.Address)
	rpcServ.Lock()
	rpcServ.clientMap[peer.Address] = client
	rpcServ.Unlock()
	return client, err
}

// CloseIfBad closes a connection if err crresponds to a bad connection error.
func (rpcServ *RPCService) CloseIfBad(err error, node *Peer) {
	if err == rpc.ErrShutdown || reflect.TypeOf(err) == reflect.TypeOf((*rpc.ServerError)(nil)).Elem() {
		rpcServ.rpcClose(node)
	}
}

// RPCClose closes the connection with a peer.
func (rpcServ *RPCService) rpcClose(peer *Peer) {
	if peer == nil {
		return
	}
	rpcServ.Lock()
	client, exists := rpcServ.clientMap[peer.Address]
	if exists {
		client.Close()
		delete(rpcServ.clientMap, peer.Address)
	}
	rpcServ.Unlock()
}
