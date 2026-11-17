// Package labrpc simulates a network of RPC endpoints for Raft labs.
//
// It is compatible with the classic MIT 6.824/6.5840 labrpc API:
//
//	net := labrpc.MakeNetwork()
//	end := net.MakeEnd(endname)              // a client endpoint, identified by any comparable value
//	srv := labrpc.MakeServer()
//	srv.Register(raftInstance)               // or srv.AddService(labrpc.MakeService(raftInstance))
//	net.AddServer(servername, srv)
//	net.Connect(endname, servername)
//	net.Enable(endname, true|false)          // disconnect/reconnect a link
//	ok := end.Call("Raft.AppendEntries", &args, &reply)
//
// The network can delay, reorder and drop messages so that Raft implementations
// are exercised under realistic conditions. Call() never crashes the caller:
// a lost request or reply is reported by returning false.
package labrpc

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"log"
	"math/rand"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type reqMsg struct {
	endname  interface{}
	svcMeth  string
	argsType reflect.Type
	args     []byte
	replyCh  chan replyMsg
}

type replyMsg struct {
	ok    bool
	reply []byte
}

// ClientEnd is a client-side handle to one (possibly disconnected) server.
type ClientEnd struct {
	endname interface{}
	ch      chan reqMsg
	done    chan struct{}
}

// Call sends an RPC and waits for the reply. It returns false if no reply
// arrived (dropped request/reply, disabled endpoint, or deleted server).
func (e *ClientEnd) Call(svcMeth string, args interface{}, reply interface{}) bool {
	req := reqMsg{
		endname:  e.endname,
		svcMeth:  svcMeth,
		argsType: reflect.TypeOf(args),
		replyCh:  make(chan replyMsg),
	}

	buffer := new(bytes.Buffer)
	if err := gob.NewEncoder(buffer).Encode(args); err != nil {
		return false
	}
	req.args = buffer.Bytes()

	select {
	case e.ch <- req:
	case <-e.done:
		return false
	}

	response := <-req.replyCh
	if !response.ok {
		return false
	}
	if err := gob.NewDecoder(bytes.NewBuffer(response.reply)).Decode(reply); err != nil {
		return false
	}
	return true
}

// Network holds all endpoints, servers, and links between them.
type Network struct {
	mu             sync.Mutex
	reliable       bool
	longDelays     bool
	longReordering bool
	ends           map[interface{}]*ClientEnd
	enabled        map[interface{}]bool
	servers        map[interface{}]*Server
	connections    map[interface{}]interface{}
	endCh          chan reqMsg
	done           chan struct{}
	cleanupOnce    sync.Once
	count          int64
	bytesSent      int64
}

// MakeNetwork creates an empty, reliable network.
func MakeNetwork() *Network {
	network := &Network{
		reliable:    true,
		ends:        make(map[interface{}]*ClientEnd),
		enabled:     make(map[interface{}]bool),
		servers:     make(map[interface{}]*Server),
		connections: make(map[interface{}]interface{}),
		endCh:       make(chan reqMsg),
		done:        make(chan struct{}),
	}
	go func() {
		for {
			select {
			case request := <-network.endCh:
				atomic.AddInt64(&network.count, 1)
				atomic.AddInt64(&network.bytesSent, int64(len(request.args)))
				go network.processReq(request)
			case <-network.done:
				return
			}
		}
	}()
	return network
}

// Cleanup releases the network's background goroutine.
func (network *Network) Cleanup() {
	network.cleanupOnce.Do(func() { close(network.done) })
}

// Reliable toggles message loss and short delays.
func (network *Network) Reliable(yes bool) {
	network.mu.Lock()
	defer network.mu.Unlock()
	network.reliable = yes
}

// LongDelays makes disconnected endpoints delay failures for several seconds,
// which is useful when checking that Raft never makes blocking RPCs.
func (network *Network) LongDelays(yes bool) {
	network.mu.Lock()
	defer network.mu.Unlock()
	network.longDelays = yes
}

// LongReordering occasionally delays replies substantially.
func (network *Network) LongReordering(yes bool) {
	network.mu.Lock()
	defer network.mu.Unlock()
	network.longReordering = yes
}

func (network *Network) endpointInfo(endname interface{}) (enabled bool, servername interface{}, server *Server, reliable, longReordering bool) {
	network.mu.Lock()
	defer network.mu.Unlock()
	enabled = network.enabled[endname]
	servername = network.connections[endname]
	if servername != nil {
		server = network.servers[servername]
	}
	reliable = network.reliable
	longReordering = network.longReordering
	return
}

func (network *Network) serverDead(endname, servername interface{}, server *Server) bool {
	network.mu.Lock()
	defer network.mu.Unlock()
	return !network.enabled[endname] || network.servers[servername] != server
}

func (network *Network) processReq(request reqMsg) {
	enabled, servername, server, reliable, longReordering := network.endpointInfo(request.endname)

	if enabled && servername != nil && server != nil {
		if !reliable {
			time.Sleep(time.Duration(rand.Intn(27)) * time.Millisecond)
			if rand.Intn(1000) < 100 {
				request.replyCh <- replyMsg{}
				return
			}
		}

		resultCh := make(chan replyMsg, 1)
		go func() { resultCh <- server.dispatch(request) }()

		var result replyMsg
		delivered := false
		dead := false
		for !delivered && !dead {
			select {
			case result = <-resultCh:
				delivered = true
			case <-time.After(100 * time.Millisecond):
				dead = network.serverDead(request.endname, servername, server)
			}
		}
		if dead {
			go func() { <-resultCh }() // let a late handler finish without blocking
		}

		if !delivered || dead || (!reliable && rand.Intn(1000) < 100) {
			request.replyCh <- replyMsg{}
			return
		}
		if longReordering && rand.Intn(900) < 600 {
			delay := time.Duration(200+rand.Intn(2001)) * time.Millisecond
			time.AfterFunc(delay, func() {
				atomic.AddInt64(&network.bytesSent, int64(len(result.reply)))
				request.replyCh <- result
			})
			return
		}
		atomic.AddInt64(&network.bytesSent, int64(len(result.reply)))
		request.replyCh <- result
		return
	}

	delay := time.Duration(rand.Intn(100)) * time.Millisecond
	if network.longDelays {
		delay = time.Duration(rand.Intn(7000)) * time.Millisecond
	}
	time.AfterFunc(delay, func() { request.replyCh <- replyMsg{} })
}

// MakeEnd creates a client endpoint. endname may be a string, an int, or any
// other comparable value; it identifies this particular link.
func (network *Network) MakeEnd(endname interface{}) *ClientEnd {
	network.mu.Lock()
	defer network.mu.Unlock()
	if _, exists := network.ends[endname]; exists {
		log.Panicf("labrpc.MakeEnd: endpoint %v already exists", endname)
	}
	end := &ClientEnd{endname: endname, ch: network.endCh, done: network.done}
	network.ends[endname] = end
	network.enabled[endname] = false
	network.connections[endname] = nil
	return end
}

// DeleteEnd removes an endpoint from the network.
func (network *Network) DeleteEnd(endname interface{}) {
	network.mu.Lock()
	defer network.mu.Unlock()
	delete(network.ends, endname)
	delete(network.enabled, endname)
	delete(network.connections, endname)
}

// AddServer registers a server under a name.
func (network *Network) AddServer(servername interface{}, server *Server) {
	network.mu.Lock()
	defer network.mu.Unlock()
	network.servers[servername] = server
}

// DeleteServer kills a server: subsequent and in-flight calls to it fail.
func (network *Network) DeleteServer(servername interface{}) {
	network.mu.Lock()
	defer network.mu.Unlock()
	network.servers[servername] = nil
}

// Connect points an endpoint at a server. The endpoint normally also needs
// Enable(endname, true) before calls can succeed.
func (network *Network) Connect(endname, servername interface{}) error {
	network.mu.Lock()
	defer network.mu.Unlock()
	if _, ok := network.servers[servername]; !ok {
		return fmt.Errorf("labrpc: connect to unknown server %v", servername)
	}
	network.connections[endname] = servername
	network.enabled[endname] = true
	return nil
}

// Enable turns a link on or off (a convenient way to simulate partitions).
func (network *Network) Enable(endname interface{}, enabled bool) {
	network.mu.Lock()
	defer network.mu.Unlock()
	network.enabled[endname] = enabled
}

// Disconnect disables a link. It is an alias for Enable(endname, false).
func (network *Network) Disconnect(endname interface{}, _ ...interface{}) {
	network.Enable(endname, false)
}

// GetCount returns the number of RPCs delivered to a server.
func (network *Network) GetCount(servername interface{}) int {
	network.mu.Lock()
	defer network.mu.Unlock()
	if server := network.servers[servername]; server != nil {
		return server.GetCount()
	}
	return 0
}

// GetTotalCount returns the number of RPCs sent through the network.
func (network *Network) GetTotalCount() int { return int(atomic.LoadInt64(&network.count)) }

// GetTotalBytes returns the number of bytes sent through the network.
func (network *Network) GetTotalBytes() int64 { return atomic.LoadInt64(&network.bytesSent) }

// Server is a collection of services that share one RPC dispatcher.
type Server struct {
	mu       sync.Mutex
	services map[string]*Service
	count    int
}

// MakeServer creates an empty server.
func MakeServer() *Server { return &Server{services: make(map[string]*Service)} }

// AddService adds a service to the server.
func (server *Server) AddService(service *Service) {
	server.mu.Lock()
	defer server.mu.Unlock()
	server.services[service.name] = service
}

// Register is a convenience wrapper around AddService(MakeService(rcvr)).
func (server *Server) Register(rcvr interface{}) {
	server.AddService(MakeService(rcvr))
}

// GetCount returns the number of RPCs dispatched to this server.
func (server *Server) GetCount() int {
	server.mu.Lock()
	defer server.mu.Unlock()
	return server.count
}

func (server *Server) dispatch(request reqMsg) replyMsg {
	dot := strings.LastIndex(request.svcMeth, ".")
	if dot < 0 {
		return replyMsg{}
	}
	serviceName, methodName := request.svcMeth[:dot], request.svcMeth[dot+1:]

	server.mu.Lock()
	server.count++
	service, ok := server.services[serviceName]
	server.mu.Unlock()

	if !ok {
		return replyMsg{}
	}
	return service.dispatch(methodName, request)
}

// Service exposes the RPC-compatible methods of one receiver object.
type Service struct {
	name    string
	rcvr    reflect.Value
	methods map[string]reflect.Method
}

// MakeService inspects rcvr for methods of the form
// func (*T) Method(args *Args, reply *Reply).
func MakeService(rcvr interface{}) *Service {
	service := &Service{
		rcvr:    reflect.ValueOf(rcvr),
		methods: make(map[string]reflect.Method),
	}
	value := service.rcvr
	if value.Kind() == reflect.Pointer {
		value = value.Elem()
	}
	service.name = value.Type().Name()

	typeOfReceiver := service.rcvr.Type()
	for index := 0; index < typeOfReceiver.NumMethod(); index++ {
		method := typeOfReceiver.Method(index)
		methodType := method.Type
		if method.PkgPath != "" || // only exported methods
			methodType.NumIn() != 3 ||
			methodType.In(2).Kind() != reflect.Pointer ||
			methodType.NumOut() != 0 {
			continue
		}
		service.methods[method.Name] = method
	}
	return service
}

func (service *Service) dispatch(methodName string, request reqMsg) replyMsg {
	method, ok := service.methods[methodName]
	if !ok {
		return replyMsg{}
	}

	args := reflect.New(request.argsType)
	if err := gob.NewDecoder(bytes.NewBuffer(request.args)).Decode(args.Interface()); err != nil {
		return replyMsg{}
	}
	replyType := method.Type.In(2).Elem()
	replyValue := reflect.New(replyType)
	method.Func.Call([]reflect.Value{service.rcvr, args.Elem(), replyValue})

	buffer := new(bytes.Buffer)
	if err := gob.NewEncoder(buffer).EncodeValue(replyValue); err != nil {
		return replyMsg{}
	}
	return replyMsg{ok: true, reply: buffer.Bytes()}
}
