# Lab 1 — Reflection-Based RPC System

## Complete Code, Design, and Test Documentation

| | |
|---|---|
| **Course** | 00304N: Distributed Processing |
| **Lab** | Lab 1 — Building a Reflection-Based RPC System |
| **Module** | `lab1` (Go 1.20+, verified with Go 1.23.4) |
| **Implementation** | `rpc/rpc.go` |
| **Tests** | `rpc/rpc_test.go` (provided suite, unchanged) |
| **Status** | **All 7 tasks implemented. 13/13 top-level tests pass. All 10 subtests pass. Race detector clean.** |

---

## Table of Contents

1. [What this project does](#1-what-this-project-does)
2. [Project layout](#2-project-layout)
3. [Wire protocol](#3-wire-protocol)
4. [Architecture and call flow](#4-architecture-and-call-flow)
5. [Detailed code walkthrough](#5-detailed-code-walkthrough)
   - 5.1 [Package documentation and imports](#51-package-documentation-and-imports)
   - 5.2 [Constants](#52-constants)
   - 5.3 [`request`](#53-request)
   - 5.4 [`response`](#54-response)
   - 5.5 [`Server`](#55-server)
   - 5.6 [`NewServer`](#56-newserver)
   - 5.7 [`Server.Start`](#57-serverstart)
   - 5.8 [`Server.acceptLoop`](#58-serveracceptloop)
   - 5.9 [`Server.Address`](#59-serveraddress)
   - 5.10 [`Server.Stop`](#510-serverstop)
   - 5.11 [`Server.handle`](#511-serverhandle)
   - 5.12 [`validateRequest`](#512-validaterequest)
   - 5.13 [`Server.invoke`](#513-serverinvoke)
   - 5.14 [`isNilValue`](#514-isnilvalue)
   - 5.15 [`callMethodSafely`](#515-callmethodsafely)
   - 5.16 [`Client`](#516-client)
   - 5.17 [`NewClient`](#517-newclient)
   - 5.18 [`Client.Call`](#518-clientcall)
6. [Concurrency and lifecycle design](#6-concurrency-and-lifecycle-design)
7. [Security and robustness](#7-security-and-robustness)
8. [Test documentation](#8-test-documentation)
9. [How to run and test](#9-how-to-run-and-test)
10. [Assignment requirements checklist](#10-assignment-requirements-checklist)
- [Appendix A — Error messages](#appendix-a--error-messages)
- [Appendix B — Function and line map](#appendix-b--function-and-line-map)
- [Appendix C — Full test output](#appendix-c--full-test-output)

---

## 1. What this project does

**Remote Procedure Call (RPC)** lets one program call a function that lives in another program, as if the function were local. The caller does not know whether the function body is in the same process or across a network.

This project implements a small RPC system in Go with three technologies:

1. **TCP** — the transport. The client and server exchange bytes over a reliable connection.
2. **JSON** — the message format. Requests and responses are human-readable JSON.
3. **Reflection** — the dispatcher. The server uses Go's `reflect` package to find and call a method by its name at runtime, without generated code.

The server is given a **receiver object**. Every exported method on that object becomes remotely callable:

```go
type Service struct{}

func (*Service) Add(left, right int) int        { return left + right }
func (*Service) Echo(value string) string       { return value }
func (*Service) Ping(id int) (string, error)    { return "Pong", nil }
```

A client can then call `Add`, `Echo`, or `Ping` by name:

```go
var sum int
err := client.Call("Add", &sum, 20, 22)   // sum == 42
```

### Supported method shapes

The assignment requires support for all of these:

| Shape | Example |
|---|---|
| Zero arguments, one value | `func (s *Service) NoArgs() string` |
| Typed arguments, one value | `func (s *Service) Add(left, right int) int` |
| One argument, one value | `func (s *Service) Echo(value string) string` |
| Only an error | `func (s *Service) OnlyError(shouldFail bool) error` |
| Value plus error | `func (s *Service) Ping(id int) (string, error)` |
| No return value | `func (s *Service) NoResult()` |

The implementation supports all of them automatically.

---

## 2. Project layout

```text
lab1/
├── go.mod                  module lab1, Go 1.20, standard library only
├── cmd/
│   └── ping/
│       └── main.go         runnable client/server demo
└── rpc/
    ├── rpc.go              RPC implementation (server, client, protocol, reflection)
    └── rpc_test.go         provided test suite (13 top-level tests)
```

| File | Role |
|---|---|
| `go.mod` | Declares `module lab1` and Go 1.20. No external dependencies. |
| `cmd/ping/main.go` | A `main` package that starts a real server, connects a client, and prints `Pong1`–`Pong4`. |
| `rpc/rpc.go` | The library: wire types, `Server`, `Client`, lifecycle, validation, reflection. |
| `rpc/rpc_test.go` | The specification: 13 tests and 10 subtests. |

The library does not depend on `main`. The demo depends on the library. This is the standard Go separation between a reusable package and an executable.

---

## 3. Wire protocol

Each call uses **one TCP connection**, **one request**, and **one response**. The server closes the connection after writing the response.

### Request

```json
{
  "method": "Add",
  "args": [2, 3]
}
```

- `method` — the exported method name.
- `args` — an array of JSON-encoded arguments. It may be absent or empty for a method with no parameters.

### Successful response

```json
{
  "ok": true,
  "value": 5
}
```

- `ok` is `true`.
- `value` is the JSON-encoded result. It is omitted when the method has no result.

### Error response

```json
{
  "ok": false,
  "error": "unknown method \"Missing\""
}
```

- `ok` is `false`.
- `error` is a human-readable message.
- `value` is omitted.

### Protocol invariants

1. The client sends exactly one request and then reads exactly one response.
2. The server closes the connection after the response.
3. There is no length header: JSON is self-delimiting, and the decoder reads one complete value.
4. Every response contains `ok`; exactly one of `value` or `error` is meaningful.
5. A remote method error is not a transport error: the response still arrives normally, with `ok:false`.

---

## 4. Architecture and call flow

### Components

| Component | Responsibility |
|---|---|
| `Server` | Owns the receiver, listener, and lifecycle state. |
| `Server.Start` | Creates the listener, records state, starts the accept loop. |
| `Server.acceptLoop` | Accepts connections and starts one worker goroutine per connection. |
| `Server.handle` | Reads one request, validates it, invokes it, writes one response, closes. |
| `validateRequest` | Rejects unsafe or oversized requests before reflection. |
| `Server.invoke` | Finds the method, decodes arguments to Go types, calls it, normalises results. |
| `callMethodSafely` | Converts a panic in user code into a normal error. |
| `Client.Call` | Opens one connection, sends one request, reads one response, decodes the result. |
| `request` / `response` | The JSON wire types. `json.RawMessage` defers argument decoding. |

### End-to-end sequence

```text
Client.Call("Add", &sum, 20, 22)
   │
   ├─ 1. net.DialTimeout(Address, Timeout)        one fresh TCP connection
   ├─ 2. conn.SetDeadline(now + Timeout)          bounds connect + write + read
   ├─ 3. encode {"method":"Add","args":[20,22]}
   ▼
Server.acceptLoop (background goroutine)
   ├─ 4. listener.Accept()                        blocks until a client connects
   ├─ 5. workers.Add(1)                           register the in-flight call
   └─ 6. go handle(conn)                          one goroutine per connection
   ▼
Server.handle(conn)
   ├─ 7. LimitReader + json.Decode                exactly one request, size-capped
   ├─ 8. validateRequest(req)                     reject abuse before reflection
   ├─ 9. invoke(req)
   │       ├─ reflect.MethodByName("Add")         exported methods only
   │       ├─ check NumIn() == len(args)
   │       ├─ json.Unmarshal each raw arg         into the declared Go type
   │       ├─ callMethodSafely(...)               panic becomes an error
   │       └─ normalise return values             value / error / value+error / none
   ├─ 10. encode {"ok":true,"value":42}
   └─ 11. defer conn.Close()                      one call per connection
   ▼
Client.Call
   ├─ 12. decode response
   ├─ 13. ok == false  → return errors.New(server message)
   └─ 14. ok == true   → json.Unmarshal(value, out); nil out is accepted
```

---

## 5. Detailed code walkthrough

File: `rpc/rpc.go`.

The walkthrough follows the file order. For each block, it explains **what the code does** and **why it is written that way**.

### 5.1 Package documentation and imports

**Lines 1–43** contain the package documentation. It states what RPC is, shows the protocol, and summarises the design. This is useful because `go doc lab1/rpc` displays it.

**Lines 45–56** import the standard library:

| Import | Used for | Why it is needed |
|---|---|---|
| `bufio` | `bufio.NewReader` | Buffers socket reads, reducing the number of system calls. |
| `encoding/json` | `Encoder`, `Decoder`, `Marshal`, `Unmarshal`, `RawMessage` | The wire format. |
| `errors` | `errors.New` | Simple sentinel errors such as "server already started". |
| `fmt` | `fmt.Errorf`, `fmt.Sprintf` | Formatted error messages with context. |
| `io` | `io.LimitReader` | Caps the number of request bytes read from a connection. |
| `net` | `net.Listener`, `net.Conn`, `net.Listen`, `net.DialTimeout` | TCP transport. |
| `reflect` | `reflect.Value`, `MethodByName`, `New`, `Call` | Runtime method lookup and invocation. |
| `sync` | `sync.Mutex`, `sync.WaitGroup` | Protect shared state and wait for handlers. |
| `time` | `time.Duration`, `time.Now` | Deadlines and timeouts. |
| `unicode` | `unicode.IsSpace`, `unicode.IsControl` | Validate method names by Unicode category. |

No third-party modules are used.

### 5.2 Constants

**Lines 58–69:**

```go
const (
    maxArguments       = 8
    maxArgumentBytes   = 1 << 20 // 1 MiB
    maxRequestBytes    = 4 << 20 // 4 MiB
    defaultCallTimeout = 5 * time.Second
)
```

| Constant | Value | What it does | Why this value / why it exists |
|---|---|---|---|
| `maxArguments` | 8 | Caps the number of parameters per call. | Prevents a request from forcing the server to allocate an unreasonable number of `reflect.Value`s. Normal RPC methods need only a few arguments. |
| `maxArgumentBytes` | 1 MiB | Caps one encoded argument. | Stops one huge string or blob from consuming memory before validation can reject it. |
| `maxRequestBytes` | 4 MiB | Caps the total encoded size of all arguments. | Stops many medium arguments from adding up to a memory exhaustion attack. |
| `defaultCallTimeout` | 5 s | Default client timeout. | A client should not wait forever for an unresponsive server. `NewClient` installs it, and `Call` falls back to it when `Timeout <= 0`. |

These limits are part of the security design: they are checked **before** reflection and before application code runs.

### 5.3 `request`

**Lines 71–79:**

```go
type request struct {
    Method string            `json:"method"`
    Args   []json.RawMessage `json:"args"`
}
```

| Part | What it does | Why |
|---|---|---|
| `Method string` | Holds the remote method name. | The JSON field must be exactly `method`. |
| `Args []json.RawMessage` | Holds each argument as **raw, undecoded JSON bytes**. | The server cannot decode an argument until it knows the method's parameter type. `json.RawMessage` delays decoding until `invoke` has looked up the method with reflection. |
| `json:"method"` / `json:"args"` | Struct tags. | Fix the wire names. The assignment forbids changing them. |

If `args` is missing, the slice is `nil`, length 0, which is correct for a zero-argument method.

### 5.4 `response`

**Lines 81–90:**

```go
type response struct {
    OK    bool            `json:"ok"`
    Value json.RawMessage `json:"value,omitempty"`
    Error string          `json:"error,omitempty"`
}
```

| Part | What it does | Why |
|---|---|---|
| `OK bool` | Success flag, always serialized. | The client checks `ok` before reading `value`. |
| `Value json.RawMessage` | The JSON-encoded result. | The result may be any type (`int`, `string`, struct, ...). Keeping it raw lets the server embed it as a JSON value rather than a quoted string. |
| `Error string` | The error message. | Remote errors must reach the client as text. |
| `omitempty` on `Value` and `Error` | Omits empty fields. | A success response does not include `"error":""`; an error response does not include `"value":null` (unless the result really is `null`). |
| `json:"ok"`, `json:"value"`, `json:"error"` | Wire names. | Fixed by the protocol. |

Note: if the Go result is `nil` (a method with no result), `json.Marshal(nil)` returns `null`, and `json.RawMessage("null")` has length 4, so `omitempty` does **not** omit it. The client accepts this; when `out` is `nil`, it ignores the value entirely.

### 5.5 `Server`

**Lines 92–127:**

```go
type Server struct {
    receiver   any
    listener   net.Listener
    stopped    chan struct{}
    acceptDone chan struct{}
    workers    sync.WaitGroup
    mu         sync.Mutex
}
```

| Field | What it stores | Why |
|---|---|---|
| `receiver any` | The object whose methods are exposed. | `invoke` uses `reflect.ValueOf(s.receiver).MethodByName(...)`. |
| `listener net.Listener` | The active TCP listener, or `nil` when stopped. | It is the source of `Accept` and `Addr()`. `nil` is the single source of truth for "running". |
| `stopped chan struct{}` | Closed by `Stop` to signal the accept loop. | Channels are the idiomatic Go way to broadcast a shutdown signal without polling. |
| `acceptDone chan struct{}` | Closed by the accept loop when it exits. | `Stop` waits on it to know that no new handlers can be added. |
| `workers sync.WaitGroup` | Counts active connection handlers. | `Stop` waits for in-flight calls instead of abandoning them. |
| `mu sync.Mutex` | Protects the lifecycle fields. | `Start`, `Stop`, and `Address` may run in different goroutines. |

The mutex is deliberately held only for short bookkeeping. It is **not** held while waiting for handlers, so a slow call does not block `Address`.

### 5.6 `NewServer`

**Lines 129–150:**

```go
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
```

| Step | What it does | Why |
|---|---|---|
| `receiver == nil` | Rejects an untyped nil. | `reflect.ValueOf(nil)` is invalid; calling a method on it would panic. |
| `reflect.ValueOf(receiver)` | Gets the reflection handle. | Needed to inspect the kind and nil-ness. |
| `Kind() != reflect.Pointer` | Requires a pointer receiver. | Methods with pointer receivers are the normal way to expose a service; a value receiver would expose a copy, and mutation would be lost. |
| `value.IsNil()` | Rejects a typed nil pointer such as `(*Service)(nil)`. | It is not `nil` as an `any`, but calling a method on it would panic when the method dereferences the receiver. |
| Return `&Server{receiver: receiver}` | Creates the server with no listener. | The server is not running until `Start`. |

### 5.7 `Server.Start`

**Lines 152–200:**

```go
func (s *Server) Start(address string) error {
    s.mu.Lock()
    defer s.mu.Unlock()

    if s.listener != nil {
        return errors.New("server already started")
    }

    listener, err := net.Listen("tcp", address)
    if err != nil {
        return err
    }

    stopped := make(chan struct{})
    acceptDone := make(chan struct{})

    s.listener = listener
    s.stopped = stopped
    s.acceptDone = acceptDone

    go s.acceptLoop(listener, stopped, acceptDone)
    return nil
}
```

| Step | What it does | Why |
|---|---|---|
| `s.mu.Lock()` + `defer Unlock` | Makes the "check + create + publish" sequence atomic. | Two concurrent `Start` calls must not both succeed. |
| `s.listener != nil` | Detects a server that is already running. | A second listener would leak and two accept loops would compete. |
| `net.Listen("tcp", address)` | Creates the TCP listening socket. | Supports `host:port`, `:port`, and `host:0`. Port `0` asks the OS for a free port. |
| `if err != nil { return err }` | Returns the real startup error. | Invalid address, permission denied, port in use, etc. must be reported to the caller. State is unchanged, so a retry can work. |
| `make(chan struct{})` twice | Creates fresh lifecycle channels. | `Stop` closes them. Closing a channel twice panics, so a restart **must** use new channels. |
| Assign fields | Publishes the listener and channels. | `Address` and `Stop` can now see a running server. |
| `go s.acceptLoop(...)` | Starts accepting in the background. | `Start` must return without waiting for a client. |
| `return nil` | Reports success. | The server is ready to accept connections. |

**Why pass the listener and channels as arguments?** The accept loop goroutine would otherwise read `s.listener`, `s.stopped`, and `s.acceptDone` while `Stop` mutates them. Passing values captures them safely without holding the mutex for the lifetime of the loop.

**Port 0:** `net.Listen` binds a free port chosen by the kernel. `Address()` later reports the concrete address.

### 5.8 `Server.acceptLoop`

**Lines 281–316:**

```go
func (s *Server) acceptLoop(listener net.Listener, stopped <-chan struct{}, done chan<- struct{}) {
    defer close(done)

    for {
        connection, err := listener.Accept()
        if err != nil {
            return
        }

        select {
        case <-stopped:
            _ = connection.Close()
            return
        default:
        }

        s.workers.Add(1)
        go func() {
            defer s.workers.Done()
            s.handle(connection)
        }()
    }
}
```

| Step | What it does | Why |
|---|---|---|
| `defer close(done)` | Signals "the accept loop has exited". | `Stop` waits on `done` before waiting for workers. |
| `listener.Accept()` | Blocks until a client connects. | This is the core of a TCP server. |
| `if err != nil { return }` | Exits when the listener is closed. | `Stop` closes the listener, so `Accept` returns an error; that is expected. Other listener failures also end the loop. |
| `select { case <-stopped: ... }` | Checks whether shutdown began after `Accept` returned. | A connection accepted at the same moment as `Stop` is closed instead of starting new work. |
| `s.workers.Add(1)` | Registers the handler **before** the goroutine starts. | If the goroutine started first, `Stop` could call `Wait` before `Add`, which is a WaitGroup misuse. |
| `go func(){ defer s.workers.Done(); s.handle(connection) }()` | Handles the connection concurrently. | One slow client must not block other clients. |
| `defer Done()` | Unregisters the handler when `handle` returns. | Lets `Stop` know the in-flight call is finished. |

**Why is `Add` safe with `Wait`?** `Stop` waits for `acceptDone` first. Once the accept loop has exited, no more `Add` calls can happen, so `workers.Wait()` cannot race with a later `Add`.

### 5.9 `Server.Address`

**Lines 202–224:**

```go
func (s *Server) Address() string {
    s.mu.Lock()
    defer s.mu.Unlock()

    if s.listener == nil {
        return ""
    }
    return s.listener.Addr().String()
}
```

| Step | What it does | Why |
|---|---|---|
| `s.mu.Lock()` | Reads lifecycle state safely. | `Address` may run concurrently with `Start` or `Stop`. |
| `s.listener == nil` | Detects "not running". | Returns `""` before `Start` and after `Stop`, as required. |
| `listener.Addr().String()` | Returns the **actual** bound address. | For `127.0.0.1:0`, this is `127.0.0.1:<real port>`. For `:0`, it includes the real port too. |

### 5.10 `Server.Stop`

**Lines 226–279:**

```go
func (s *Server) Stop() error {
    s.mu.Lock()
    if s.listener == nil {
        s.mu.Unlock()
        return nil
    }

    listener := s.listener
    stopped := s.stopped
    acceptDone := s.acceptDone

    s.listener = nil
    s.stopped = nil
    s.acceptDone = nil
    close(stopped)
    s.mu.Unlock()

    _ = listener.Close()
    <-acceptDone
    s.workers.Wait()
    return nil
}
```

| Step | What it does | Why |
|---|---|---|
| `s.listener == nil` → return `nil` | Handles "never started", "already stopped", and "currently stopping". | `Stop` must be safe before `Start` and safe to call more than once. |
| Snapshot the fields | Captures the current run's listener and channels. | The fields are about to be cleared. |
| Clear `listener`, `stopped`, `acceptDone` | Marks the server as stopped immediately. | `Address()` now returns `""`, and a concurrent `Stop` sees `nil` and returns. |
| `close(stopped)` | Signals the accept loop. | Closing broadcasts to any goroutine selecting on it. |
| `s.mu.Unlock()` | Releases the lock before waiting. | Handlers do not need the lock; holding it would block `Address` for the whole shutdown. |
| `listener.Close()` | Unblocks a goroutine sleeping in `Accept`. | `Accept` returns an error, so the loop exits. The error is expected and ignored. |
| `<-acceptDone` | Waits for the accept loop to finish. | After this point, no new handler can be registered. |
| `s.workers.Wait()` | Waits for active handlers. | An in-flight call finishes and receives its response. |
| `return nil` | Reports successful shutdown. | The server is restartable. |

**Why this exact order?**

```text
close(stopped)  →  listener.Close()  →  wait acceptDone  →  workers.Wait()
```

- `acceptDone` must be awaited **before** `workers.Wait`, otherwise the accept loop could call `Add` while `Wait` is running.
- `workers.Wait` comes last so existing calls can finish.
- Clearing the state first makes a second `Stop` a no-op.

**Caveat:** calling `Start` concurrently with an in-progress `Stop` is outside the assignment's tested contract. The required pattern is sequential `Stop` then `Start`, which is fully supported. This is documented as a trade-off rather than hidden.

### 5.11 `Server.handle`

**Lines 318–371:**

```go
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
```

| Step | What it does | Why |
|---|---|---|
| `defer connection.Close()` | Closes the socket when handling ends. | The protocol is one request per connection; resources must always be released. |
| `json.NewEncoder(connection)` | Creates a writer for JSON values. | It writes a JSON value plus a newline. Both sides use `Decoder`, which does not require the newline, but it is conventional. |
| `defer recover` | Catches a panic that escapes other layers. | A bad request must not crash the server. Method panics are normally caught earlier in `callMethodSafely`; this is defense in depth. |
| `io.LimitReader(connection, maxRequestBytes+1)` | Caps the bytes the decoder may read. | The `+1` lets an over-limit request fail during decoding instead of being silently truncated. |
| `bufio.NewReader(...)` | Buffers socket reads. | Fewer syscalls and smoother decoding. |
| `decoder.Decode(&req)` | Reads exactly one JSON request. | Matches the one-request-per-connection protocol. |
| Decode error → encode error → return | Reports malformed JSON. | The client must receive an answer; it must not wait forever. |
| `validateRequest(req)` | Checks protocol limits. | Reject abuse before reflection or application code. |
| `s.invoke(req)` | Performs the remote call. | All reflection and method errors come back as `error`. |
| `json.Marshal(result)` | Converts the Go result to JSON bytes. | The response struct stores a `json.RawMessage`; marshalling first lets the result be embedded as a JSON value rather than a quoted string. |
| `encoder.Encode(response{OK: true, Value: payload})` | Writes the success response. | The client sees `ok:true` and decodes `value` into its output variable. |

**Error responses:** the `response` struct's zero value is `OK:false`, so `response{Error: ...}` serializes as `{"ok":false,"error":"..."}`.

**Deferred order:** defers run last-in-first-out. The recover closure is deferred **after** `Close`, so on a panic the recover runs first (writes the error response) and then `Close` runs.

**Encoding errors:** writes are ignored with `_ =`. If the client has already disconnected, there is nothing useful to do; the handler still exits and closes the connection.

### 5.12 `validateRequest`

**Lines 373–421:**

```go
func validateRequest(req request) error {
    if req.Method == "" {
        return errors.New("method cannot be empty")
    }

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
```

| Check | What it rejects | Why |
|---|---|---|
| `req.Method == ""` | Empty method name. | An empty name cannot match any method and is not a meaningful request. |
| Rune loop with `unicode.IsSpace` / `IsControl` | Spaces, tabs, newlines, and control characters. | Prevents malformed method names and log injection. Iterating runes (not bytes) handles non-ASCII input correctly. |
| `len(req.Args) > maxArguments` | More than 8 arguments. | Bounds the number of reflection values and prevents parameter-count abuse. |
| `len(argument) > maxArgumentBytes` | One argument over 1 MiB. | Bounds per-argument memory. |
| `total > maxRequestBytes` | All arguments together over 4 MiB. | Bounds total memory even when each argument is individually legal. |
| `return nil` | Nothing. | The request is valid and may proceed. |

**Why validate before reflection?** Reflection allocates `reflect.Value`s and decodes arguments. A hostile client could force a lot of work or memory before the method is even called. Validation is cheap and runs first.

**What it does not do:** it does not check whether the method exists, whether the argument count matches, or whether each argument's JSON type matches. Those need the method's signature, so they are handled in `invoke`.

### 5.13 `Server.invoke`

**Lines 423–498:**

```go
func (s *Server) invoke(req request) (any, error) {
    method := reflect.ValueOf(s.receiver).MethodByName(req.Method)
    if !method.IsValid() {
        return nil, fmt.Errorf("unknown method %q", req.Method)
    }

    typeOfMethod := method.Type()
    if typeOfMethod.NumIn() != len(req.Args) {
        return nil, fmt.Errorf("method %q expects %d arguments, got %d",
            req.Method, typeOfMethod.NumIn(), len(req.Args))
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

    errorType := reflect.TypeOf((*error)(nil)).Elem()
    if last := outputs[len(outputs)-1]; last.Type().Implements(errorType) {
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
```

| Step | What it does | Why |
|---|---|---|
| `reflect.ValueOf(s.receiver).MethodByName(req.Method)` | Looks up the method by name. | This is the reflection bridge. It finds **exported** methods on the receiver. |
| `!method.IsValid()` | Detects a missing or unexported method. | Returns `unknown method "X"` instead of panicking. |
| `method.Type()` | Gets the method's signature. | Needed for parameter count and parameter types. |
| `NumIn() != len(req.Args)` | Checks argument count. | A mismatch must be a clear remote error. |
| `reflect.New(typeOfMethod.In(index))` | Allocates a `*T` for parameter type `T`. | `json.Unmarshal` needs a pointer so it can fill the value. |
| `json.Unmarshal(raw, argument.Interface())` | Decodes the raw JSON into `T`. | JSON types are checked here: a string sent for an `int` parameter becomes an error. |
| `argument.Elem()` | Converts `*T` to `T`. | `reflect.Value.Call` expects values of the declared parameter type. |
| `callMethodSafely(method, arguments)` | Calls the method with panic recovery. | A buggy method must not kill the server. |
| `len(outputs) == 0` | Handles methods with no return value. | Returns `nil, nil`. |
| `last.Type().Implements(errorType)` | Detects a final error return. | Supports both `(value, error)` and `(error)` shapes. |
| `!isNilValue(last)` | Checks whether the error is non-nil. | A nil error is not a failure; a non-nil error is a remote error. |
| `outputs = outputs[:len(outputs)-1]` | Drops a nil error. | The remaining value, if any, is the result. |
| `outputs[0].Interface()` | Returns the first non-error value. | Matches the assignment's one-value rule. |

**Method lookup and exported methods:** `MethodByName` uses the method set of the concrete type. Unexported methods are not in that set, so they cannot be called remotely. That is a security feature, not an accident.

**Argument decoding example:**

```text
request arg:  "left"        parameter type: int
reflect.New(int) → *int
json.Unmarshal([]byte(`"left"`), *int) → error
→ "argument 0: json: cannot unmarshal string into Go value of type int"
```

**Return shapes:**

| Method signature | `outputs` | Result |
|---|---|---|
| `func NoResult()` | `[]` | `nil, nil` |
| `func NoArgs() string` | `["ready"]` | `"ready", nil` |
| `func Add(int,int) int` | `[42]` | `42, nil` |
| `func OnlyError(bool) error` | `[nil]` or `[error]` | `nil, nil` or `nil, error` |
| `func Ping(int) (string, error)` | `["Pong4", nil]` | `"Pong4", nil` |
| `func Ping(int) (string, error)` with `id < 0` | `["", error]` | `nil, error` |

**Why the `isNilValue` check is necessary:** a typed nil pointer stored in an `error` interface is not a nil interface. Calling `Interface().(error)` on it would produce a non-nil error value even though the method returned "no error". `isNilValue` checks the underlying kind before deciding.

### 5.14 `isNilValue`

**Lines 500–511:**

```go
func isNilValue(value reflect.Value) bool {
    switch value.Kind() {
    case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map,
        reflect.Pointer, reflect.Slice:
        return value.IsNil()
    default:
        return false
    }
}
```

| Part | What it does | Why |
|---|---|---|
| `switch value.Kind()` | Handles only nilable kinds. | `reflect.Value.IsNil` panics for ints, strings, structs, etc. |
| `Chan, Func, Interface, Map, Pointer, Slice` | The nilable kinds. | These are the only kinds for which `IsNil` is legal. |
| `value.IsNil()` | Reports whether the value is nil. | Used for the final error return. |
| `default: return false` | Non-nilable values are never nil. | An `int` or `string` cannot be nil. |

### 5.15 `callMethodSafely`

**Lines 513–526:**

```go
func callMethodSafely(method reflect.Value, arguments []reflect.Value) (outputs []reflect.Value, err error) {
    defer func() {
        if recovered := recover(); recovered != nil {
            err = fmt.Errorf("remote method panicked: %v", recovered)
            outputs = nil
        }
    }()
    return method.Call(arguments), nil
}
```

| Part | What it does | Why |
|---|---|---|
| Named return values `outputs, err` | Allows the deferred function to modify the results. | `recover` must be able to set `err` after a panic. |
| `defer func(){ recover() ... }` | Catches a panic from `method.Call`. | A remote method may panic intentionally or because of a bug. |
| `err = fmt.Errorf("remote method panicked: %v", ...)` | Converts the panic value into a normal error. | The client receives a useful error instead of a dropped connection. |
| `outputs = nil` | Clears partial results. | A panicking call has no valid result. |
| `method.Call(arguments)` | Performs the actual call. | This is the reflection invocation. |

**What would happen without this function?** A panic in a remote method would unwind through `handle`, through the worker goroutine, and crash the whole process (an unrecovered panic in any goroutine terminates the program). Recovering turns it into a normal error response.

### 5.16 `Client`

**Lines 528–538:**

```go
type Client struct {
    Address string
    Timeout time.Duration
}
```

| Field | What it stores | Why |
|---|---|---|
| `Address` | The server's `host:port`. | Usually obtained from `Server.Address()`. |
| `Timeout` | The maximum duration for one call. | Bounds connection setup and the subsequent I/O. A value `<= 0` falls back to `defaultCallTimeout`. |

A `Client` has no mutable per-call state. Every call opens its own connection, so one `Client` can be shared by many goroutines safely.

### 5.17 `NewClient`

**Lines 540–552:**

```go
func NewClient(address string) (*Client, error) {
    if address == "" {
        return nil, errors.New("address cannot be empty")
    }
    return &Client{Address: address, Timeout: defaultCallTimeout}, nil
}
```

| Step | What it does | Why |
|---|---|---|
| Empty address check | Rejects a missing address early. | `net.Dial` would fail later with a less specific error. |
| Return `&Client{...}` | Creates a client with a default timeout. | Callers can override `Timeout` afterwards. |

### 5.18 `Client.Call`

**Lines 554–640:**

```go
func (c *Client) Call(method string, out any, args ...any) error {
    if method == "" {
        return errors.New("method cannot be empty")
    }

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

    decoder := json.NewDecoder(bufio.NewReader(connection))
    var remote response
    if err := decoder.Decode(&remote); err != nil {
        return fmt.Errorf("decode response: %w", err)
    }

    if !remote.OK {
        return errors.New(remote.Error)
    }

    if out == nil {
        return nil
    }

    if err := json.Unmarshal(remote.Value, out); err != nil {
        return fmt.Errorf("decode response value: %w", err)
    }
    return nil
}
```

| Step | What it does | Why |
|---|---|---|
| Empty method check | Rejects invalid input before networking. | No connection is opened for a call that cannot succeed. |
| Marshal each argument | Converts `any` to raw JSON bytes. | The request must carry `[]json.RawMessage`; the server needs raw bytes to decode by parameter type. |
| `json.Marshal` error | Handles unsupported values. | Channels, functions, and cyclic values cannot be encoded; the error names the argument index. |
| Timeout fallback | Uses 5 s when `Timeout <= 0`. | Avoids an accidental infinite wait. |
| `net.DialTimeout("tcp", c.Address, timeout)` | Connects to the server with a bound. | An unavailable server must fail promptly. |
| `defer connection.Close()` | Releases the socket on every return path. | No connection leaks, even on errors. |
| `SetDeadline(now + timeout)` | Bounds the write and read. | `DialTimeout` only bounds connection setup; the server may then be slow. |
| `json.NewEncoder(...).Encode(request{...})` | Sends exactly one request. | Matches the protocol. |
| `json.NewDecoder(...).Decode(&remote)` | Reads exactly one response. | Blocks until the server answers or the deadline expires. |
| `!remote.OK` | Converts a remote error into a local error. | The caller sees `err != nil` with the server's message. |
| `out == nil` | Accepts calls whose method has no result. | The caller passes `nil` when it does not want a value. |
| `json.Unmarshal(remote.Value, out)` | Decodes the result into the caller's variable. | `out` must be a pointer so it can be written. |
| Wrap with `"decode response value"` | Distinguishes output decoding from network errors. | The tests require the word `decode` in this failure. |

**Why one connection per call?** It makes framing trivial (exactly one JSON value each way), gives every call its own deadline, and removes shared mutable state. The trade-off is TCP handshake overhead per call.

---

## 6. Concurrency and lifecycle design

Three primitives cooperate:

| Primitive | Where | Purpose |
|---|---|---|
| `sync.Mutex` | `Server.mu` | Protects `listener`, `stopped`, and `acceptDone` during `Start`, `Stop`, and `Address`. |
| `sync.WaitGroup` | `Server.workers` | Counts active handlers so `Stop` can wait for them. |
| Channels | `stopped`, `acceptDone` | Signal shutdown and confirm that the accept loop exited. |

### Shutdown order

1. Lock `mu`, verify the server is running, and snapshot the listener and channels.
2. Clear the lifecycle fields. `Address()` now returns `""`; a second `Stop` returns immediately.
3. `close(stopped)` to signal the accept loop.
4. Unlock `mu`.
5. `listener.Close()` to unblock a goroutine in `Accept`.
6. Receive from `acceptDone` — the accept loop has exited, so no new handler can be added.
7. `workers.Wait()` — in-flight handlers finish.
8. Return `nil`.

### Why the order matters

If `workers.Wait()` ran before `<-acceptDone`, the accept loop could still call `workers.Add` while `Wait` is active. Go's `WaitGroup` does not allow adding to a group that is being waited on when the counter is zero. Waiting for `acceptDone` first establishes the invariant that all `Add` calls have happened.

### Restart safety

`Start` creates a new listener and **new** channels on every run. `Stop` clears the fields. The only long-lived state is the receiver and the `WaitGroup`, whose counter is zero after a successful `Stop`. `TestStopAndRestart` verifies this.

### Race detector

```bash
go test -race ./...
```

Result: **PASS**, no data races reported. The mutex covers every read and write of the lifecycle fields; the accept loop receives its listener and channels as immutable arguments.

---

## 7. Security and robustness

| Failure / attack | Mechanism | Result |
|---|---|---|
| Malformed JSON | Decode error path in `handle` | JSON error response; connection closed. |
| Empty or unsafe method name | `validateRequest` | Error response before reflection. |
| Too many arguments | `maxArguments` | Error response; no reflection allocation. |
| Oversized argument | `maxArgumentBytes` | Error response; memory bounded. |
| Oversized total request | `maxRequestBytes` + `LimitReader` | Error response; memory bounded. |
| Wrong JSON type for a parameter | `json.Unmarshal` in `invoke` | Error naming the argument index. |
| Unknown or unexported method | `MethodByName` validity check | `unknown method` error. |
| Method returns an error | Final-error handling in `invoke` | Client receives the error text. |
| Method panics | `callMethodSafely` recover | Error response; server stays alive. |
| Panic outside the method | Deferred `recover` in `handle` | Last-resort error response. |
| Slow or stuck method | Client `SetDeadline` | Client times out. |
| Duplicate `Start` | State check under `mu` | `server already started` error. |
| `Stop` before `Start` | Nil check | `nil` (success). |
| Repeated `Stop` | Nil check | `nil` (success). |
| Shutdown during a call | `workers.Wait` | In-flight call finishes before `Stop` returns. |

### Defence in depth

- `LimitReader` bounds the bytes the decoder can consume regardless of how the request is shaped.
- `validateRequest` produces precise, user-facing errors for limit violations.
- `callMethodSafely` handles method panics.
- The deferred `recover` in `handle` handles anything that escapes.
- The client deadline prevents a caller from waiting forever.

---

## 8. Test documentation

File: `rpc/rpc_test.go` (provided, unchanged).

The tests use the **public API** (`NewServer`, `Start`, `Address`, `Stop`, `NewClient`, `Call`) and, for the malformed/oversized cases, raw TCP. They assert observable behavior, not internal implementation. Error-message substrings are part of the contract because the tests use `strings.Contains`.

### 8.1 The test service

**Lines 13–47:**

```go
type testService struct {
    slowStarted chan struct{}
}
```

| Method | Code | Why it exists |
|---|---|---|
| `Ping(id int) (string, error)` | Returns `"Pong"+digit`; error for `id < 0`. | Tests value+error shape and remote error propagation. |
| `Add(left, right int) int` | Returns the sum. | Tests typed arguments and one value. |
| `Panic() (string, error)` | `panic("test panic")`. | Tests panic recovery. |
| `Slow() string` | Closes `slowStarted`, sleeps 150 ms. | Tests shutdown waiting and client timeout. |
| `NoArgs() string` | Returns `"ready"`. | Tests zero arguments. |
| `NoResult()` | Returns nothing. | Tests the no-result shape. |
| `Echo(value string) string` | Returns its argument. | Tests a string round trip. |
| `OnlyError(shouldFail bool) error` | Returns an error when `shouldFail` is true; otherwise `nil`. | Tests error-only shape in both directions. |

`slowStarted` is a channel used only by `TestStopWaitsForActiveHandler` to know that `Slow` has actually begun before calling `Stop`.

### 8.2 Helpers

**Lines 344–355 — `startTestServer`:**

```go
func startTestServer(t *testing.T) *Server {
    t.Helper()
    server, err := NewServer(&testService{})
    if err != nil { t.Fatal(err) }
    if err := server.Start("127.0.0.1:0"); err != nil { t.Fatal(err) }
    t.Cleanup(func() { _ = server.Stop() })
    return server
}
```

- `t.Helper()` makes failures point at the caller, not the helper.
- Port `0` lets the OS choose a free port, so tests never conflict.
- `t.Cleanup` guarantees `Stop` even if the test fails.

**Lines 357–364 — `testClient`:**

```go
func testClient(t *testing.T, server *Server) *Client {
    t.Helper()
    client, err := NewClient(server.Address())
    if err != nil { t.Fatal(err) }
    return client
}
```

- Uses `server.Address()` so it always connects to the real port.

### 8.3 Test-by-test

For each test: purpose, setup, assertions, why it matters, code path, and result.

---

#### 8.3.1 `TestValidateRequest`

**File line:** 49

**Purpose:** verify every validation rule and make sure valid requests are not rejected.

**Inputs:**

| # | Request | Expected error contains | What it tests |
|---|---|---|---|
| 1 | `request{}` | `method cannot be empty` | Empty method name. |
| 2 | `Method: "Bad Method"` | `whitespace` | Space in method name. |
| 3 | `Method: "Bad\nMethod"` | `whitespace` | Control character (newline). |
| 4 | `Args: make([]json.RawMessage, maxArguments+1)` | `too many arguments` | Argument count > 8. |
| 5 | One `json.RawMessage` of `maxArgumentBytes+1` | `larger` | One argument > 1 MiB. |
| 6 | Five arguments of `maxArgumentBytes-1` each | `larger` | Total > 4 MiB while each argument is legal. |

**Valid requests that must pass:**

```go
request{Method: "Ping"}
request{Method: "Add", Args: []json.RawMessage{[]byte("1"), []byte("2")}}
```

**Assertion:** if `err == nil` or the error text does not contain the expected substring, `t.Fatalf`.

**Why it matters:** validation is the first line of defence. It must reject abuse before reflection, and it must not reject normal calls. The exact substrings are the contract.

**Code path:** `validateRequest` (lines 392–421).

**Result:** **PASS** — parent and all 6 subtests, 0.00s.

---

#### 8.3.2 `TestServerClient`

**File line:** 78

**Purpose:** basic end-to-end success and a remote error.

**Flow:**

1. `NewServer(&testService{})`
2. `Start("127.0.0.1:0")`
3. `NewClient(server.Address())`
4. `Call("Add", &sum, 20, 22)` → expect `sum == 42`
5. `Call("Ping", &pong, 4)` → expect `"Pong4"`
6. `Call("Ping", &pong, -1)` → expect a non-nil error

**Why it matters:** proves the whole path works: TCP, JSON, validation, reflection, typed decoding, result encoding, and remote error propagation.

**Code path:** `Start` → `acceptLoop` → `handle` → `validateRequest` → `invoke` → `callMethodSafely`; client `Call`.

**Result:** **PASS** 0.00s.

---

#### 8.3.3 `TestServerClientMethodShapes`

**File line:** 111

**Purpose:** cover every supported method shape.

| Call | Expected |
|---|---|
| `Call("NoArgs", &noArgs)` | `"ready"` |
| `Call("NoResult", nil)` | `nil` error |
| `Call("Echo", &echoed, "hello")` | `"hello"` |
| `Call("OnlyError", nil, false)` | `nil` error |
| `Call("OnlyError", nil, true)` | error containing `"requested failure"` |

**Why it matters:** the reflection bridge must handle zero outputs, one value, one error, and value+error. This test is the direct check of those shapes.

**Code path:** `invoke` return-shape branch (lines 475–497).

**Result:** **PASS** 0.00s.

---

#### 8.3.4 `TestInvalidRemoteCallsBecomeErrors`

**File line:** 143

**Purpose:** invalid calls must become errors, never panic or hang.

| Case | Call | Expected error contains | Why |
|---|---|---|---|
| Unknown method | `Missing` | `unknown method` | `MethodByName` returns an invalid value. |
| Wrong argument count | `Add` with one arg | `expects 2 arguments` | `NumIn()` mismatch. |
| Wrong JSON type | `Add("left", 2)` | `argument 0` | `json.Unmarshal` into `int` fails. |
| Invalid output type | `Add(1, 2)` into `*string` | `decode` | The server succeeds with `5`, but the client cannot unmarshal a number into `string`. |

**Assertion:** error must be non-nil and contain the expected text.

**Why it matters:** malformed client input must not crash the server, and the client must produce a useful diagnostic. The `decode` case checks that output-type failures are distinguishable.

**Code path:** `invoke` lines 450–468; `Client.Call` lines 636–638.

**Result:** **PASS** — parent and all 4 subtests, 0.00s.

---

#### 8.3.5 `TestRemotePanicBecomesError`

**File line:** 169

**Purpose:** a panic in a remote method must become an error response.

**Flow:** `Call("Panic", &result)`; expect `err != nil` and `strings.Contains(err.Error(), "panicked")`.

**Why it matters:** an unrecovered panic in a goroutine terminates the whole process. `callMethodSafely` recovers it and returns `remote method panicked: test panic`. The server stays alive and other clients are unaffected.

**Code path:** `callMethodSafely` (lines 518–526), `invoke` error check.

**Result:** **PASS** 0.00s.

---

#### 8.3.6 `TestConcurrentCalls`

**File line:** 179

**Purpose:** many simultaneous calls must all succeed.

**Flow:** 20 goroutines share one `Client`, each calls `Add(value, value)` into a local `int`; results are collected through an `errors` channel. Any error fails the test.

**Why it matters:**

- Each call opens its own TCP connection, so there is no shared connection state.
- The server starts one goroutine per connection.
- `Client` is safe for concurrent use.

**Code path:** `acceptLoop` (goroutine per connection), `handle`, `invoke`; `Client.Call`.

**Result:** **PASS** 0.00s.

---

#### 8.3.7 `TestStopAndRestart`

**File line:** 196

**Purpose:** a stopped server must be restartable.

**Flow:**

1. `Start("127.0.0.1:0")`
2. `Stop()`
3. `Address()` must be `""`
4. `Start("127.0.0.1:0")` again
5. `Address()` must be non-empty
6. `defer Stop()`

**Why it matters:** `Stop` clears `listener`, `stopped`, and `acceptDone`; `Start` creates fresh channels. Reusing closed channels would panic.

**Code path:** `Start` (lines 185–197), `Stop` (lines 258–276), `Address`.

**Result:** **PASS** 0.00s.

---

#### 8.3.8 `TestServerLifecycleErrors`

**File line:** 219

**Purpose:** lifecycle operations must be idempotent and reject duplicate starts.

**Assertions:**

| Step | Expected |
|---|---|
| `Address()` before `Start` | `""` |
| `Stop()` before `Start` | `nil` error |
| `Start(...)` | success |
| `Start(...)` again | non-nil error |
| `Stop()` | success |
| `Address()` after `Stop` | `""` |
| `Stop()` again | `nil` error |

**Why it matters:** the server must not panic on `close(nil)` or double-close a channel. A duplicate `Start` must not leak a listener.

**Code path:** `Start` duplicate check; `Stop` nil check.

**Result:** **PASS** 0.00s.

---

#### 8.3.9 `TestStopWaitsForActiveHandler`

**File line:** 247

**Purpose:** `Stop` must wait for an in-flight call to finish.

**Flow:**

1. Create `testService{slowStarted: make(chan struct{})}`.
2. Start the server and create a client.
3. In a goroutine, call `Slow` (sleeps 150 ms).
4. Wait for `slowStarted`, so the handler has definitely begun.
5. Record the time, call `Stop`.
6. Assert `elapsed >= 100ms`.
7. Assert the in-flight call eventually returns `nil` error.

**Why it matters:** a graceful shutdown must not cut off a client mid-call. The test also proves the shutdown order: the accept loop exits, then `workers.Wait` waits for the slow handler.

**Code path:** `Stop` lines 271–276; `acceptLoop` `workers.Add/Done`; `handle`.

**Result:** **PASS** 0.15s.

---

#### 8.3.10 `TestClientTimeout`

**File line:** 281

**Purpose:** a slow server must not block the client forever.

**Flow:** start the server, create a client, set `client.Timeout = 20 * time.Millisecond`, call `Slow` (150 ms). Expect a non-nil error.

**Why it matters:** `DialTimeout` only covers connection setup. `SetDeadline` covers the write and read. Without it, this call would wait for the full 150 ms (or forever for a stuck method).

**Code path:** `Client.Call` lines 607–612 (deadline) and 619–623 (decode error).

**Result:** **PASS** 0.15s.

---

#### 8.3.11 `TestClientConnectionFailures`

**File line:** 291

**Purpose:** an unavailable server must produce an error.

**Flow:** `NewClient("127.0.0.1:1")`, `Timeout = 50ms`, `Call("NoArgs", &result)`. Expect a non-nil error.

**Why it matters:** connection failures must be reported, not ignored or allowed to hang. Port 1 is a reserved port that is normally not listening.

**Code path:** `Client.Call` lines 601–604 (`net.DialTimeout` error).

**Result:** **PASS** 0.00s.

---

#### 8.3.12 `TestMalformedRequestGetsErrorResponse`

**File line:** 303

**Purpose:** malformed JSON must receive an error response, and the server must not crash.

**Flow:** open a raw TCP connection, write `not json\n`, decode one `response`, assert `result.Error != ""`.

**Why it matters:** this bypasses `Client.Call`, so it tests the server's raw protocol behavior. The client must not be left waiting forever.

**Code path:** `handle` lines 348–353 (decode error → error response).

**Result:** **PASS** 0.00s.

---

#### 8.3.13 `TestOversizedRequestGetsErrorResponse`

**File line:** 322

**Purpose:** an oversized argument must be rejected with an error response.

**Flow:** open a raw TCP connection, write a JSON request containing `strings.Repeat("x", maxArgumentBytes)` (1 MiB) as the `Echo` argument, decode one response, assert `result.Error != ""`.

**Why it matters:** the encoded JSON string includes two quote bytes, so its `json.RawMessage` length is `maxArgumentBytes + 2`, which exceeds `maxArgumentBytes`. `validateRequest` rejects it. The test also exercises the `LimitReader` path.

**Code path:** `handle` lines 345–357; `validateRequest` lines 409–415.

**Result:** **PASS** 0.01s.

---

### 8.4 Subtests

`TestValidateRequest` contains 6 subtests and `TestInvalidRemoteCallsBecomeErrors` contains 4. All 10 pass:

| Subtest | Result |
|---|---|
| `TestValidateRequest/empty_method` | PASS |
| `TestValidateRequest/space_in_method` | PASS |
| `TestValidateRequest/control_character` | PASS |
| `TestValidateRequest/too_many_arguments` | PASS |
| `TestValidateRequest/large_argument` | PASS |
| `TestValidateRequest/large_request` | PASS |
| `TestInvalidRemoteCallsBecomeErrors/unknown_method` | PASS |
| `TestInvalidRemoteCallsBecomeErrors/wrong_argument_count` | PASS |
| `TestInvalidRemoteCallsBecomeErrors/invalid_argument_JSON_type` | PASS |
| `TestInvalidRemoteCallsBecomeErrors/invalid_output_type` | PASS |

### 8.5 Test-to-requirement matrix

| Assignment testing requirement | Test(s) | Result |
|---|---|---|
| Starting a server on a normal address | `TestServerClient`, helpers | PASS |
| Starting with port 0 | all server tests | PASS |
| Returning the actual listening address | `TestServerClient`, `TestStopAndRestart` | PASS |
| Rejecting duplicate starts | `TestServerLifecycleErrors` | PASS |
| Stopping before startup | `TestServerLifecycleErrors` | PASS |
| Stopping more than once | `TestServerLifecycleErrors` | PASS |
| Restarting after a stop | `TestStopAndRestart` | PASS |
| Waiting for active handlers during shutdown | `TestStopWaitsForActiveHandler` | PASS |
| Calling methods successfully | `TestServerClient`, `TestServerClientMethodShapes` | PASS |
| Zero-argument methods | `TestServerClientMethodShapes` | PASS |
| Typed arguments | `TestServerClient` (`Add`), `TestServerClientMethodShapes` (`Echo`) | PASS |
| No-result methods | `TestServerClientMethodShapes` | PASS |
| Error-only methods | `TestServerClientMethodShapes` | PASS |
| Unknown method | `TestInvalidRemoteCallsBecomeErrors` | PASS |
| Wrong number of arguments | `TestInvalidRemoteCallsBecomeErrors` | PASS |
| Wrong JSON argument type | `TestInvalidRemoteCallsBecomeErrors` | PASS |
| Result that cannot decode into `out` | `TestInvalidRemoteCallsBecomeErrors` | PASS |
| Method returns an error | `TestServerClient` (`Ping(-1)`), `TestServerClientMethodShapes` (`OnlyError(true)`) | PASS |
| Method panics | `TestRemotePanicBecomesError` | PASS |
| Malformed JSON | `TestMalformedRequestGetsErrorResponse` | PASS |
| Oversized input | `TestOversizedRequestGetsErrorResponse`, `TestValidateRequest` | PASS |
| Concurrent calls | `TestConcurrentCalls` | PASS |
| Client timeout | `TestClientTimeout` | PASS |
| Unavailable server | `TestClientConnectionFailures` | PASS |

### 8.6 Actual results

Full suite:

```text
$ go test -v -count=1 ./rpc
...
PASS
ok      lab1/rpc    0.316s
```

Race detector:

```text
$ go test -race -v -count=1 ./...
...
PASS
ok      lab1/rpc    1.425s
```

No data races were reported. The full output is in [Appendix C](#appendix-c--full-test-output).

**Conclusion:** every test succeeded.

---

## 9. How to run and test

### Prerequisites

Go 1.20 or newer on the `PATH`:

```bash
go version
```

This machine has Go 1.23.4 at `/home/atta/.local/bin/go`.

### Run all tests

```bash
cd ~/RPC/lab1
go test -v -count=1 ./rpc
```

### Run with the race detector

```bash
cd ~/RPC/lab1
go test -race -v -count=1 ./rpc
```

### Run one test

Use the **real** test name, not the placeholder `TestName`:

```bash
cd ~/RPC/lab1
go test -run '^TestValidateRequest$' -v -count=1 ./rpc
go test -run '^TestStopAndRestart$' -v -count=1 ./rpc
go test -run '^TestClientTimeout$' -v -count=1 ./rpc
```

### Run every test individually

```bash
cd ~/RPC/lab1
for t in $(go test -list '^Test' ./rpc | grep '^Test'); do
  echo "===== $t ====="
  go test -run "^${t}$" -v -count=1 ./rpc
done
```

### List test names

```bash
go test -list '^Test' ./rpc
```

### Run the demo

```bash
cd ~/RPC/lab1
go run ./cmd/ping
```

Expected output:

```text
Pong1
Pong2
Pong3
Pong4
```

### Other useful commands

```bash
go vet ./...          # static analysis
gofmt -l .            # files needing formatting (empty = clean)
go build ./...        # compile everything
go test -cover ./...  # coverage
```

### Common mistake

```bash
go test -run '^TestName$' ...
```

`TestName` is a placeholder, not a real test. It produces:

```text
testing: warning: no tests to run
PASS
ok      lab1/rpc    [no tests to run]
```

Use a real name such as `^TestValidateRequest$`, or omit `-run` to run everything.

---

## 10. Assignment requirements checklist

### Tasks

| Task | Function | Status |
|---|---|---|
| 1. Start the server | `Server.Start` | **Done** |
| 2. Return the address | `Server.Address` | **Done** |
| 3. Stop the server | `Server.Stop` | **Done** |
| 4. Validate requests | `validateRequest` | **Done** |
| 5. Handle RPC calls | `Server.handle` | **Done** |
| 6. Invoke methods | `Server.invoke` | **Done** |
| 7. Implement the client | `Client.Call` | **Done** |

### Important rules

| Rule | Status |
|---|---|
| Do not delete existing TODO comments | **Preserved** (all 7 remain) |
| Keep existing function signatures | **Unchanged** |
| Do not change JSON field names | **Unchanged** (`method`, `args`, `ok`, `value`, `error`) |
| Invalid input returns an error response | **Yes** |
| Invalid input does not crash the server | **Yes** (validation + recover) |
| Network resources are closed | **Yes** (`defer Close`, listener `Close`) |
| Server is safe to stop and restart | **Yes** (fresh channels, cleared state) |
| Supports all method shapes | **Yes** |
| Remote errors reach the client | **Yes** |
| Client timeout | **Yes** |
| Concurrent clients | **Yes** (one goroutine per connection) |

### Test suite

| Metric | Result |
|---|---|
| Top-level tests | 13 / 13 pass |
| Subtests | 10 / 10 pass |
| Total test cases | 23 / 23 pass |
| Race detector | Clean |
| Demo | `Pong1`–`Pong4` |

---

## Appendix A — Error messages

These strings are observable behavior. The tests match several of them with `strings.Contains`.

### Server and validation

| Message | Source |
|---|---|
| `receiver cannot be nil` | `NewServer` |
| `receiver must be a non-nil pointer` | `NewServer` |
| `server already started` | `Start` |
| `method cannot be empty` | `validateRequest` |
| `method %q cannot contain whitespace or control characters` | `validateRequest` |
| `too many arguments: %d (maximum %d)` | `validateRequest` |
| `argument %d is larger than %d bytes` | `validateRequest` |
| `request arguments are larger than %d bytes` | `validateRequest` |
| `invalid request: %v` | `handle` (JSON decode) |
| `unknown method %q` | `invoke` |
| `method %q expects %d arguments, got %d` | `invoke` |
| `argument %d: %v` | `invoke` (JSON decode) |
| `remote method panicked: %v` | `callMethodSafely` / `handle` recover |
| `encode result: %v` | `handle` |

### Client

| Message | Source |
|---|---|
| `address cannot be empty` | `NewClient` |
| `method cannot be empty` | `Call` |
| `encode argument %d: %w` | `Call` |
| `connect to %s: %w` | `Call` |
| `set deadline: %w` | `Call` |
| `encode request: %w` | `Call` |
| `decode response: %w` | `Call` |
| `decode response value: %w` | `Call` |

Remote method errors are returned verbatim via `errors.New(remote.Error)`.

---

## Appendix B — Function and line map

### `rpc/rpc.go`

| Symbol | Lines |
|---|---|
| Package documentation | 1–43 |
| Imports | 45–56 |
| Constants | 60–69 |
| `request` | 76–79 |
| `response` | 86–90 |
| `Server` | 104–127 |
| `NewServer` | 141–150 |
| `Server.Start` | 169–200 |
| `Server.Address` | 216–224 |
| `Server.Stop` | 244–279 |
| `Server.acceptLoop` | 285–316 |
| `Server.handle` | 336–371 |
| `validateRequest` | 392–421 |
| `Server.invoke` | 449–498 |
| `isNilValue` | 504–511 |
| `callMethodSafely` | 518–526 |
| `Client` | 532–538 |
| `NewClient` | 547–552 |
| `Client.Call` | 578–640 |

### `rpc/rpc_test.go`

| Symbol | Lines |
|---|---|
| `testService` | 13–47 |
| `TestValidateRequest` | 49–76 |
| `TestServerClient` | 78–109 |
| `TestServerClientMethodShapes` | 111–141 |
| `TestInvalidRemoteCallsBecomeErrors` | 143–167 |
| `TestRemotePanicBecomesError` | 169–177 |
| `TestConcurrentCalls` | 179–194 |
| `TestStopAndRestart` | 196–217 |
| `TestServerLifecycleErrors` | 219–245 |
| `TestStopWaitsForActiveHandler` | 247–279 |
| `TestClientTimeout` | 281–289 |
| `TestClientConnectionFailures` | 291–301 |
| `TestMalformedRequestGetsErrorResponse` | 303–320 |
| `TestOversizedRequestGetsErrorResponse` | 322–342 |
| `startTestServer` | 344–355 |
| `testClient` | 357–364 |

---

## Appendix C — Full test output

```text
$ go test -v -count=1 ./rpc
=== RUN   TestValidateRequest
=== RUN   TestValidateRequest/empty_method
=== RUN   TestValidateRequest/space_in_method
=== RUN   TestValidateRequest/control_character
=== RUN   TestValidateRequest/too_many_arguments
=== RUN   TestValidateRequest/large_argument
=== RUN   TestValidateRequest/large_request
--- PASS: TestValidateRequest (0.00s)
    --- PASS: TestValidateRequest/empty_method (0.00s)
    --- PASS: TestValidateRequest/space_in_method (0.00s)
    --- PASS: TestValidateRequest/control_character (0.00s)
    --- PASS: TestValidateRequest/too_many_arguments (0.00s)
    --- PASS: TestValidateRequest/large_argument (0.00s)
    --- PASS: TestValidateRequest/large_request (0.00s)
=== RUN   TestServerClient
--- PASS: TestServerClient (0.00s)
=== RUN   TestServerClientMethodShapes
--- PASS: TestServerClientMethodShapes (0.00s)
=== RUN   TestInvalidRemoteCallsBecomeErrors
=== RUN   TestInvalidRemoteCallsBecomeErrors/unknown_method
=== RUN   TestInvalidRemoteCallsBecomeErrors/wrong_argument_count
=== RUN   TestInvalidRemoteCallsBecomeErrors/invalid_argument_JSON_type
=== RUN   TestInvalidRemoteCallsBecomeErrors/invalid_output_type
--- PASS: TestInvalidRemoteCallsBecomeErrors (0.00s)
    --- PASS: TestInvalidRemoteCallsBecomeErrors/unknown_method (0.00s)
    --- PASS: TestInvalidRemoteCallsBecomeErrors/wrong_argument_count (0.00s)
    --- PASS: TestInvalidRemoteCallsBecomeErrors/invalid_argument_JSON_type (0.00s)
    --- PASS: TestInvalidRemoteCallsBecomeErrors/invalid_output_type (0.00s)
=== RUN   TestRemotePanicBecomesError
--- PASS: TestRemotePanicBecomesError (0.00s)
=== RUN   TestConcurrentCalls
--- PASS: TestConcurrentCalls (0.00s)
=== RUN   TestStopAndRestart
--- PASS: TestStopAndRestart (0.00s)
=== RUN   TestServerLifecycleErrors
--- PASS: TestServerLifecycleErrors (0.00s)
=== RUN   TestStopWaitsForActiveHandler
--- PASS: TestStopWaitsForActiveHandler (0.15s)
=== RUN   TestClientTimeout
--- PASS: TestClientTimeout (0.15s)
=== RUN   TestClientConnectionFailures
--- PASS: TestClientConnectionFailures (0.00s)
=== RUN   TestMalformedRequestGetsErrorResponse
--- PASS: TestMalformedRequestGetsErrorResponse (0.00s)
=== RUN   TestOversizedRequestGetsErrorResponse
--- PASS: TestOversizedRequestGetsErrorResponse (0.01s)
PASS
ok      lab1/rpc    0.316s
```

### Race detector

```text
$ go test -race -v -count=1 ./...
...
=== RUN   TestStopWaitsForActiveHandler
--- PASS: TestStopWaitsForActiveHandler (0.15s)
=== RUN   TestClientTimeout
--- PASS: TestClientTimeout (0.15s)
=== RUN   TestClientConnectionFailures
--- PASS: TestClientConnectionFailures (0.00s)
=== RUN   TestMalformedRequestGetsErrorResponse
--- PASS: TestMalformedRequestGetsErrorResponse (0.00s)
=== RUN   TestOversizedRequestGetsErrorResponse
--- PASS: TestOversizedRequestGetsErrorResponse (0.10s)
PASS
ok      lab1/rpc    1.425s
```

No data races were detected.

---

*End of documentation.*
