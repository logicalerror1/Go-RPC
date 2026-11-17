// Package rpc is an intentionally incomplete RMI assignment template.
package rpc

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"reflect"
	"sync"
	"time"
	"unicode"
)

const (
	maxArguments       = 8
	maxArgumentBytes   = 1 << 20
	maxRequestBytes    = 4 << 20
	defaultCallTimeout = 5 * time.Second
)

type request struct {
	Method string            `json:"method"`
	Args   []json.RawMessage `json:"args"`
}

type response struct {
	OK    bool            `json:"ok"`
	Value json.RawMessage `json:"value,omitempty"`
	Error string          `json:"error,omitempty"`
}

// Server exposes exported methods of a receiver over TCP.
type Server struct {
	receiver   any
	listener   net.Listener
	stopped    chan struct{}
	acceptDone chan struct{}
	workers    sync.WaitGroup
	mu         sync.Mutex
}

// NewServer creates a server for a non-nil pointer receiver.
func NewServer(receiver any) (*Server, error) {
	if receiver == nil {
		return nil, errors.New("receiver cannot be nil")
	}
	value := reflect.ValueOf(receiver)
	if value.Kind() != reflect.Pointer || value.IsNil() {
		return nil, errors.New("receiver must be a non-nil pointer")
	}
	return &Server{receiver: receiver}, nil
}

// TODO 1: Create the TCP listener, reject duplicate starts, save lifecycle state,
// and launch the accept loop without blocking the caller.
// Start begins accepting remote calls at address. Use port 0 to request an available port.
func (s *Server) Start(address string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// A second Start is only legal after the previous server has been stopped.
	if s.listener != nil {
		return errors.New("server already started")
	}

	listener, err := net.Listen("tcp", address)
	if err != nil {
		return err
	}

	s.listener = listener
	s.stopped = make(chan struct{})
	s.acceptDone = make(chan struct{})

	go s.acceptLoop(listener, s.stopped, s.acceptDone)
	return nil
}

// TODO 2: Return the listener address safely while allowing port 0 discovery.
// Address returns the address assigned to the running server.
func (s *Server) Address() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.listener == nil {
		return ""
	}
	return s.listener.Addr().String()
}

// TODO 3: Close the listener, signal the accept loop, wait for its workers,
// and leave the server in a state where Start can be called again.
// Stop stops accepting new connections and waits for active handlers to finish.
func (s *Server) Stop() error {
	s.mu.Lock()
	if s.listener == nil {
		s.mu.Unlock()
		return nil
	}

	listener := s.listener
	stopped := s.stopped
	acceptDone := s.acceptDone
	// Clearing the listener first prevents the accept loop from registering any
	// more workers while Stop is waiting for the existing ones.
	s.listener = nil
	s.mu.Unlock()

	close(stopped)
	err := listener.Close()
	<-acceptDone
	s.workers.Wait()
	return err
}

// acceptLoop accepts connections until Stop signals shutdown. The lifecycle
// channels are passed explicitly so a later Start cannot affect this loop.
func (s *Server) acceptLoop(listener net.Listener, stopped <-chan struct{}, acceptDone chan struct{}) {
	defer close(acceptDone)
	for {
		connection, err := listener.Accept()
		if err != nil {
			select {
			case <-stopped:
			default:
			}
			return
		}

		s.mu.Lock()
		if s.listener == nil {
			s.mu.Unlock()
			connection.Close()
			return
		}
		// Add under the lock: Stop clears listener under the same lock before
		// calling Wait, so no Add can race with Wait.
		s.workers.Add(1)
		s.mu.Unlock()

		go func() {
			defer s.workers.Done()
			s.handle(connection)
		}()
	}
}

// TODO 4: Study this starter request handler, then complete and test the protocol
// contract. Add tests for malformed JSON, oversized input, remote errors, panics,
// and successful calls, and make sure one bad request cannot crash the server or
// leave a connection hanging. Connect this handler to your completed Start and
// Stop lifecycle so each accepted connection is processed by a worker safely.
// handle reads one request, invokes it, and writes one response.
func (s *Server) handle(connection net.Conn) {
	defer connection.Close()
	encoder := json.NewEncoder(connection)
	defer func() {
		if recovered := recover(); recovered != nil {
			_ = encoder.Encode(response{Error: fmt.Sprintf("remote method panicked: %v", recovered)})
		}
	}()

	decoder := json.NewDecoder(bufio.NewReader(io.LimitReader(connection, maxRequestBytes+1)))
	var req request
	if err := decoder.Decode(&req); err != nil {
		_ = encoder.Encode(response{Error: fmt.Sprintf("invalid request: %v", err)})
		return
	}
	if err := validateRequest(req); err != nil {
		_ = encoder.Encode(response{Error: err.Error()})
		return
	}
	result, err := s.invoke(req)
	if err != nil {
		_ = encoder.Encode(response{Error: err.Error()})
		return
	}
	payload, err := json.Marshal(result)
	if err != nil {
		_ = encoder.Encode(response{Error: fmt.Sprintf("encode result: %v", err)})
		return
	}
	_ = encoder.Encode(response{OK: true, Value: payload})
}

// TODO 5: Reject empty or unsafe method names, too many arguments, arguments
// larger than maxArgumentBytes, and requests larger than maxRequestBytes.
// validateRequest rejects malformed or abusive requests before reflection runs.
func validateRequest(req request) error {
	if req.Method == "" {
		return errors.New("method cannot be empty")
	}
	for _, character := range req.Method {
		if unicode.IsSpace(character) || unicode.IsControl(character) {
			return fmt.Errorf("method name contains whitespace or control characters")
		}
	}
	if len(req.Args) > maxArguments {
		return fmt.Errorf("too many arguments: got %d, limit is %d", len(req.Args), maxArguments)
	}

	total := 0
	for index, raw := range req.Args {
		if len(raw) > maxArgumentBytes {
			return fmt.Errorf("argument %d is larger than %d bytes", index, maxArgumentBytes)
		}
		total += len(raw)
	}
	if total > maxRequestBytes {
		return fmt.Errorf("encoded arguments are larger than %d bytes", maxRequestBytes)
	}
	return nil
}

// TODO 6: Study and test this starter reflection bridge. Your required work is to
// support and test exported methods with zero arguments, typed arguments, no
// return value, one return value, and a final error return; unknown methods,
// wrong argument counts, invalid JSON arguments, returned errors, and panics must
// become clear errors instead of crashing the server. Extend the bridge only when
// your chosen assignment protocol requires another result or argument rule.
// invoke finds a method, converts JSON arguments to its parameter types, and calls it.
func (s *Server) invoke(req request) (any, error) {
	method := reflect.ValueOf(s.receiver).MethodByName(req.Method)
	if !method.IsValid() {
		return nil, fmt.Errorf("unknown method %q", req.Method)
	}
	typeOfMethod := method.Type()
	if typeOfMethod.NumIn() != len(req.Args) {
		return nil, fmt.Errorf("method %q expects %d arguments, got %d", req.Method, typeOfMethod.NumIn(), len(req.Args))
	}
	arguments := make([]reflect.Value, len(req.Args))
	for index, raw := range req.Args {
		argument := reflect.New(typeOfMethod.In(index))
		if err := json.Unmarshal(raw, argument.Interface()); err != nil {
			return nil, fmt.Errorf("argument %d: %v", index, err)
		}
		arguments[index] = argument.Elem()
	}
	outputs, err := callMethodSafely(method, arguments)
	if err != nil {
		return nil, err
	}
	if len(outputs) == 0 {
		return nil, nil
	}
	if last := outputs[len(outputs)-1]; last.Type().Implements(reflect.TypeOf((*error)(nil)).Elem()) {
		if !isNilValue(last) {
			return nil, last.Interface().(error)
		}
		outputs = outputs[:len(outputs)-1]
	}
	if len(outputs) == 0 {
		return nil, nil
	}
	return outputs[0].Interface(), nil
}

func isNilValue(value reflect.Value) bool {
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

func callMethodSafely(method reflect.Value, arguments []reflect.Value) (outputs []reflect.Value, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("remote method panicked: %v", recovered)
		}
	}()
	return method.Call(arguments), nil
}

// Client calls methods on a remote Server.
type Client struct {
	Address string
	Timeout time.Duration
}

// NewClient creates a client for a server address.
func NewClient(address string) (*Client, error) {
	if address == "" {
		return nil, errors.New("address cannot be empty")
	}
	return &Client{Address: address, Timeout: defaultCallTimeout}, nil
}

// TODO 7: Open one connection, apply a deadline, encode the request, read one
// response, return remote errors, and decode the response into out.
// Call invokes method and decodes its result into out. Pass nil for no result.
func (c *Client) Call(method string, out any, args ...any) error {
	if method == "" {
		return errors.New("method cannot be empty")
	}

	encodedArgs := make([]json.RawMessage, len(args))
	for index, argument := range args {
		encoded, err := json.Marshal(argument)
		if err != nil {
			return fmt.Errorf("encode argument %d: %w", index, err)
		}
		encodedArgs[index] = encoded
	}

	timeout := c.Timeout
	if timeout <= 0 {
		timeout = defaultCallTimeout
	}
	connection, err := net.DialTimeout("tcp", c.Address, timeout)
	if err != nil {
		return fmt.Errorf("connect to %s: %w", c.Address, err)
	}
	defer connection.Close()
	if err := connection.SetDeadline(time.Now().Add(timeout)); err != nil {
		return fmt.Errorf("set deadline: %w", err)
	}

	encoder := json.NewEncoder(connection)
	if err := encoder.Encode(request{Method: method, Args: encodedArgs}); err != nil {
		return fmt.Errorf("encode request: %w", err)
	}

	var reply response
	if err := json.NewDecoder(bufio.NewReader(connection)).Decode(&reply); err != nil {
		return fmt.Errorf("read response: %w", err)
	}
	if !reply.OK {
		if reply.Error == "" {
			return errors.New("remote call failed")
		}
		return errors.New(reply.Error)
	}
	if out == nil || len(reply.Value) == 0 {
		return nil
	}
	if err := json.Unmarshal(reply.Value, out); err != nil {
		return fmt.Errorf("decode result: %w", err)
	}
	return nil
}
