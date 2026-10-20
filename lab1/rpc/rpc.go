// Package rpc implements a small, reflection-based Remote Procedure Call (RPC)
// system over TCP.
//
// # What is RPC?
//
// Remote Procedure Call lets a client invoke a method that lives in another
// process (or on another machine) as if it were a normal function call. The
// client marshals the method name and arguments into a message, sends it to a
// server, and the server executes the method and sends the result back.
//
// # Wire protocol
//
// The protocol is one JSON request per TCP connection and one JSON response
// back. The server closes the connection after it writes the response.
//
// Request:
//
//	{"method":"Add","args":[2,3]}
//
// Successful response:
//
//	{"ok":true,"value":5}
//
// Failed response:
//
//	{"ok":false,"error":"unknown method \"Missing\""}
//
// # Design overview
//
//   - Server holds a receiver object. Every exported method on that object
//     becomes remotely callable; unexported methods are invisible.
//   - Reflection (reflect.MethodByName) finds the method at runtime, and each
//     JSON argument is decoded into the exact Go parameter type before the
//     method is called.
//   - Requests are validated against fixed size limits before reflection or
//     application code runs, so hostile input is rejected cheaply.
//   - Every accepted connection is handled by its own goroutine. A
//     sync.WaitGroup lets Stop wait for in-flight handlers.
//   - A method panic is recovered and converted into an error response, so a
//     single bad call can never crash the server.
//   - The client opens a new connection for each call and applies a deadline,
//     which keeps calls independent and makes timeouts predictable.
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

// Protocol limits. They are deliberately small and fixed so that one client
// cannot force the server to allocate unbounded memory.
const (
	// maxArguments is the largest number of parameters a remote call may carry.
	maxArguments = 8
	// maxArgumentBytes is the largest encoded size of a single JSON argument.
	maxArgumentBytes = 1 << 20 // 1 MiB
	// maxRequestBytes is the largest combined encoded size of all arguments.
	maxRequestBytes = 4 << 20 // 4 MiB
	// defaultCallTimeout is used by NewClient and by Call when Timeout <= 0.
	defaultCallTimeout = 5 * time.Second
)

// request is the JSON body the client sends.
//
// Args uses json.RawMessage because the server does not know the parameter
// types until reflection has found the method. Keeping the bytes raw lets the
// server delay decoding until it knows the target type.
type request struct {
	Method string            `json:"method"`
	Args   []json.RawMessage `json:"args"`
}

// response is the JSON body the server sends back.
//
// Value is omitted when it is empty (for example on an error). Error is omitted
// on success. The JSON field names are fixed by the assignment protocol and
// must not be changed.
type response struct {
	OK    bool            `json:"ok"`
	Value json.RawMessage `json:"value,omitempty"`
	Error string          `json:"error,omitempty"`
}

// Server exposes exported methods of a receiver over TCP.
//
// Lifecycle:
//
//	server, _ := NewServer(&Service{})
//	server.Start("127.0.0.1:0")   // running, OS picked a port
//	addr := server.Address()      // "127.0.0.1:54321"
//	...                           // clients call methods
//	server.Stop()                 // stop accepting, wait for handlers
//
// A Server may be started again after Stop. It is safe to call Stop before
// Start and safe to call it more than once.
type Server struct {
	// receiver is the object whose exported methods are callable remotely.
	receiver any

	// listener is the active TCP listener. It is nil before Start and after
	// Stop. Access is protected by mu because Address and Stop may run in
	// different goroutines than Start.
	listener net.Listener

	// stopped is closed by Stop to tell the accept loop to stop accepting.
	stopped chan struct{}

	// acceptDone is closed by the accept loop when it has fully exited.
	acceptDone chan struct{}

	// workers counts connection handlers that are still running. Stop waits on
	// it so shutdown does not abandon an in-flight call.
	workers sync.WaitGroup

	// mu protects the lifecycle fields above. It is held only for short
	// bookkeeping, never while waiting for handlers, so a slow handler cannot
	// block Address.
	mu sync.Mutex
}

// NewServer creates a server for a non-nil pointer receiver.
//
// The receiver must be a pointer because reflection needs the method set of the
// concrete object (value types would expose a copy) and because a nil pointer
// would panic when a method is invoked.
//
// Usage:
//
//	type Calculator struct{}
//	func (*Calculator) Add(a, b int) int { return a + b }
//
//	server, err := NewServer(&Calculator{})
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
//
// IMPLEMENTED:
//   - net.Listen("tcp", address) creates the listening socket. If the address is
//     invalid or the port is already in use, its error is returned unchanged.
//   - s.listener != nil means the server is already running; a second Start is
//     rejected instead of leaking a second listener.
//   - Fresh stopped/acceptDone channels are created on every Start. This is what
//     makes restart after Stop possible: closed channels cannot be reused.
//   - The accept loop runs in a new goroutine, so Start returns immediately.
//
// Example:
//
//	err := server.Start("127.0.0.1:0") // port 0 = let the OS choose
//	addr := server.Address()           // the real port is now visible
func (s *Server) Start(address string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// A non-nil listener means Start has already been called and Stop has not
	// completed. Starting twice would leak the first listener and create two
	// accept loops, so reject it.
	if s.listener != nil {
		return errors.New("server already started")
	}

	listener, err := net.Listen("tcp", address)
	if err != nil {
		return err
	}

	// Recreate the lifecycle channels for this run. They must be new channels
	// because Stop closes them and Go panics if a channel is closed twice.
	stopped := make(chan struct{})
	acceptDone := make(chan struct{})

	s.listener = listener
	s.stopped = stopped
	s.acceptDone = acceptDone

	// The accept loop owns listener lifetime from here on (Stop closes it).
	// Passing the channels as arguments avoids reading mutable fields from the
	// goroutine without the mutex.
	go s.acceptLoop(listener, stopped, acceptDone)

	return nil
}

// TODO 2: Return the listener address safely while allowing port 0 discovery.
// Address returns the address assigned to the running server.
//
// IMPLEMENTED:
//   - The mutex is held while reading s.listener because Address may be called
//     concurrently with Start or Stop.
//   - listener.Addr().String() reports the address the operating system actually
//     bound, which resolves ":0" to a concrete port.
//   - An empty string is returned before Start and after Stop, as required.
//
// Example:
//
//	server.Start("127.0.0.1:0")
//	fmt.Println(server.Address()) // e.g. "127.0.0.1:41237"
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
//
// IMPLEMENTED:
//   - If the server is not running, Stop returns nil immediately. This makes it
//     safe before Start and safe to call repeatedly.
//   - Lifecycle fields are cleared while holding mu, so a later Start sees a
//     clean, stopped server.
//   - close(stopped) signals the accept loop; listener.Close() unblocks a
//     goroutine currently blocked in Accept.
//   - Wait for acceptDone before workers.Wait: once the accept loop has exited,
//     no new worker can be added, so Wait cannot race with a later Add.
//   - workers.Wait() lets an in-flight call finish instead of cutting it off.
//
// Example:
//
//	defer server.Stop() // runs at the end of main, even if startup succeeded
func (s *Server) Stop() error {
	s.mu.Lock()

	// Not running (never started, already stopped, or still stopping). Stop is
	// intentionally idempotent.
	if s.listener == nil {
		s.mu.Unlock()
		return nil
	}

	listener := s.listener
	stopped := s.stopped
	acceptDone := s.acceptDone

	// Clear the visible state first. Address now returns "", and a concurrent
	// Stop sees nil and returns.
	s.listener = nil
	s.stopped = nil
	s.acceptDone = nil

	// Wake the accept loop. Closing is safe exactly once because Start creates a
	// new channel for every run.
	close(stopped)
	s.mu.Unlock()

	// Closing the listener makes a blocked Accept return an error, which ends
	// the loop. The error is expected during shutdown, so it is ignored.
	_ = listener.Close()

	// First wait for the accept loop to finish (no new handlers can start),
	// then wait for handlers that are already running.
	<-acceptDone
	s.workers.Wait()

	return nil
}

// acceptLoop runs in a background goroutine for the lifetime of one server run.
//
// It accepts connections until the listener is closed. Each connection gets its
// own goroutine and is counted in s.workers so Stop can wait for it.
func (s *Server) acceptLoop(listener net.Listener, stopped <-chan struct{}, done chan<- struct{}) {
	// Closing done is the signal Stop waits for ("the accept loop has exited").
	defer close(done)

	for {
		connection, err := listener.Accept()
		if err != nil {
			// The listener was closed by Stop (or failed). Either way there is
			// nothing more to accept.
			return
		}

		// If Stop started between Accept returning and this check, do not begin
		// new work. Closing the connection gives the client a prompt failure
		// instead of leaving it waiting.
		select {
		case <-stopped:
			_ = connection.Close()
			return
		default:
		}

		// Add must happen before the goroutine starts and before Stop can call
		// Wait. Because Stop waits for acceptDone first, all Add calls are
		// guaranteed to have happened before Wait begins.
		s.workers.Add(1)
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
//
// IMPLEMENTED / EXPLAINED:
//   - Exactly one request is decoded per connection, matching the protocol.
//   - io.LimitReader caps the number of bytes the decoder may consume. The +1
//     lets an over-limit body fail during decoding instead of being silently
//     truncated. validateRequest enforces the finer limits afterwards.
//   - Malformed JSON, validation failures, invocation errors and encoding
//     failures all produce {"ok":false,"error":...}; the client never hangs.
//   - The deferred recover is a last-resort safety net. Method panics are
//     normally caught earlier in callMethodSafely; this catches anything else
//     that escapes, so one connection cannot crash the whole process.
//   - The connection is always closed by the deferred Close.
func (s *Server) handle(connection net.Conn) {
	defer connection.Close()
	encoder := json.NewEncoder(connection)
	defer func() {
		if recovered := recover(); recovered != nil {
			_ = encoder.Encode(response{Error: fmt.Sprintf("remote method panicked: %v", recovered)})
		}
	}()

	// Limit the total bytes read for one request. A client that sends more than
	// maxRequestBytes+1 bytes causes a decode error rather than unbounded memory
	// use.
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
	// Marshal the Go result to raw JSON first so the response encoder can embed
	// it as a JSON value (not as a quoted string).
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
//
// IMPLEMENTED:
//   - The method name must be non-empty and must not contain whitespace or
//     control characters. This keeps logs safe and prevents names that could be
//     used to smuggle text into error messages.
//   - The argument count is capped at maxArguments.
//   - Each encoded argument is capped at maxArgumentBytes.
//   - The combined encoded arguments are capped at maxRequestBytes.
//
// Validation runs before reflection and before application code, so hostile
// input is rejected without allocating parameter values or invoking methods.
//
// Example:
//
//	validateRequest(request{Method: "Add", Args: []json.RawMessage{[]byte("1")}})
//	// error: method "Add" expects 2 arguments, got 1  (detected later, in invoke)
func validateRequest(req request) error {
	if req.Method == "" {
		return errors.New("method cannot be empty")
	}

	// Reject whitespace (spaces, tabs, newlines) and control characters in the
	// method name. This is both a protocol rule and a log-injection defence.
	for _, character := range req.Method {
		if unicode.IsSpace(character) || unicode.IsControl(character) {
			return fmt.Errorf("method %q cannot contain whitespace or control characters", req.Method)
		}
	}

	if len(req.Args) > maxArguments {
		return fmt.Errorf("too many arguments: %d (maximum %d)", len(req.Args), maxArguments)
	}

	total := 0
	for index, argument := range req.Args {
		if len(argument) > maxArgumentBytes {
			return fmt.Errorf("argument %d is larger than %d bytes", index, maxArgumentBytes)
		}
		total += len(argument)
	}
	if total > maxRequestBytes {
		return fmt.Errorf("request arguments are larger than %d bytes", maxRequestBytes)
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
//
// IMPLEMENTED / EXPLAINED:
//   - reflect.ValueOf(receiver).MethodByName only finds exported methods, so
//     private helpers stay private automatically.
//   - NumIn() on a bound method value excludes the receiver: it is exactly the
//     number of declared parameters.
//   - For argument i, reflect.New(paramType) allocates a *T; json.Unmarshal fills
//     it, then Elem() gives the T value that Call expects.
//   - callMethodSafely converts a panic into an ordinary error.
//   - Return handling covers every required shape:
//     no outputs, one value, one error, value+error.
//
// Supported receivers look like:
//
//	func (*Service) NoArgs() string
//	func (*Service) Add(left, right int) int
//	func (*Service) Echo(value string) string
//	func (*Service) OnlyError(shouldFail bool) error
//	func (*Service) Ping(id int) (string, error)
func (s *Server) invoke(req request) (any, error) {
	method := reflect.ValueOf(s.receiver).MethodByName(req.Method)
	if !method.IsValid() {
		return nil, fmt.Errorf("unknown method %q", req.Method)
	}

	typeOfMethod := method.Type()
	if typeOfMethod.NumIn() != len(req.Args) {
		return nil, fmt.Errorf("method %q expects %d arguments, got %d", req.Method, typeOfMethod.NumIn(), len(req.Args))
	}

	// Decode each JSON argument into the method's declared parameter type.
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
		// Method declared no return values (for example func NoResult()).
		return nil, nil
	}

	// If the final return value is an error, use it as a remote error when it is
	// non-nil, and drop it when it is nil. This handles both (value, error) and
	// (error) shapes.
	errorType := reflect.TypeOf((*error)(nil)).Elem()
	if last := outputs[len(outputs)-1]; last.Type().Implements(errorType) {
		if !isNilValue(last) {
			return nil, last.Interface().(error)
		}
		outputs = outputs[:len(outputs)-1]
	}

	if len(outputs) == 0 {
		// The only return value was a nil error (for example OnlyError(false)).
		return nil, nil
	}

	// At most one non-error value is supported, matching the assignment rules.
	return outputs[0].Interface(), nil
}

// isNilValue reports whether a reflect.Value holds a nil value.
//
// Only nilable kinds (channels, functions, interfaces, maps, pointers and
// slices) can be nil; calling IsNil on other kinds panics, hence the switch.
func isNilValue(value reflect.Value) bool {
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

// callMethodSafely invokes method and converts a panic into an error.
//
// Reflection calls a real Go function; if that function panics, the panic would
// otherwise unwind through the connection handler and kill the goroutine (and
// potentially the process). Recovering here turns it into a normal remote error.
func callMethodSafely(method reflect.Value, arguments []reflect.Value) (outputs []reflect.Value, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("remote method panicked: %v", recovered)
			outputs = nil
		}
	}()
	return method.Call(arguments), nil
}

// Client calls methods on a remote Server.
//
// Each Call uses one new TCP connection, so a Client is safe for concurrent use
// and a slow call cannot block other calls.
type Client struct {
	// Address is the server's host:port, usually obtained from Server.Address.
	Address string
	// Timeout bounds connection setup plus the write/read of one call. A value
	// <= 0 means defaultCallTimeout.
	Timeout time.Duration
}

// NewClient creates a client for a server address.
//
// Usage:
//
//	client, err := NewClient(server.Address())
//	var sum int
//	err = client.Call("Add", &sum, 20, 22)
func NewClient(address string) (*Client, error) {
	if address == "" {
		return nil, errors.New("address cannot be empty")
	}
	return &Client{Address: address, Timeout: defaultCallTimeout}, nil
}

// TODO 7: Open one connection, apply a deadline, encode the request, read one
// response, return remote errors, and decode the response into out.
// Call invokes method and decodes its result into out. Pass nil for no result.
//
// IMPLEMENTED:
//   - Empty method names are rejected before any network activity.
//   - Every argument is marshalled to json.RawMessage so the request matches the
//     protocol exactly (the server decodes each raw argument by parameter type).
//   - DialTimeout bounds connection setup; SetDeadline bounds the subsequent
//     write and read, so a slow or unresponsive server cannot block forever.
//   - A response with ok=false becomes an ordinary Go error containing the
//     server's message.
//   - A nil out means "the method has no result" and is accepted.
//   - Otherwise Value is unmarshalled into out, and decoding failures are
//     wrapped with the word "decode" for a clear diagnostic.
//   - The connection is always closed.
//
// Usage:
//
//	var result string
//	err := client.Call("Ping", &result, 7)
//	fmt.Println(result) // "Pong7"
//
//	err = client.Call("NoResult", nil) // no result: pass nil
func (c *Client) Call(method string, out any, args ...any) error {
	if method == "" {
		return errors.New("method cannot be empty")
	}

	// Marshal each argument separately. json.Marshal produces valid JSON for
	// the concrete type, and json.RawMessage embeds it without re-encoding.
	encodedArgs := make([]json.RawMessage, len(args))
	for index, argument := range args {
		raw, err := json.Marshal(argument)
		if err != nil {
			return fmt.Errorf("encode argument %d: %w", index, err)
		}
		encodedArgs[index] = raw
	}

	timeout := c.Timeout
	if timeout <= 0 {
		timeout = defaultCallTimeout
	}

	// One connection per call: simple framing, no shared mutable state, and
	// every call gets its own deadline.
	connection, err := net.DialTimeout("tcp", c.Address, timeout)
	if err != nil {
		return fmt.Errorf("connect to %s: %w", c.Address, err)
	}
	defer connection.Close()

	// SetDeadline applies to the whole connection (write + read). This is what
	// makes TestClientTimeout pass: the server may be busy, but the client stops
	// waiting after the timeout.
	if err := connection.SetDeadline(time.Now().Add(timeout)); err != nil {
		return fmt.Errorf("set deadline: %w", err)
	}

	encoder := json.NewEncoder(connection)
	if err := encoder.Encode(request{Method: method, Args: encodedArgs}); err != nil {
		return fmt.Errorf("encode request: %w", err)
	}

	decoder := json.NewDecoder(bufio.NewReader(connection))
	var remote response
	if err := decoder.Decode(&remote); err != nil {
		return fmt.Errorf("decode response: %w", err)
	}

	if !remote.OK {
		// The server reported a remote failure (unknown method, bad argument,
		// method error, panic, ...). Expose its message as a local error.
		return errors.New(remote.Error)
	}

	if out == nil {
		// The remote method has no result, or the caller does not want it.
		return nil
	}

	if err := json.Unmarshal(remote.Value, out); err != nil {
		return fmt.Errorf("decode response value: %w", err)
	}
	return nil
}
