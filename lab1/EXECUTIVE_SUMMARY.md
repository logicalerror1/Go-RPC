# Lab 1 — Executive Summary & Complete Code Explanation

## 1. Executive summary

**What the lab is.** Lab 1 asks you to finish a small **Remote Procedure Call (RPC)** system in Go. RPC lets a client call a function on another process as though it were local. The client and server talk over **TCP**, encode messages as **JSON**, and the server discovers which method to run at runtime using **reflection**. The starter archive contained a module (`lab1/go.mod`), a demo (`cmd/ping/main.go`), a partly implemented server/client (`rpc/rpc.go`) with seven TODOs, and a full test suite (`rpc/rpc_test.go`).

**What we built.** All seven TODOs were implemented without changing any signature, JSON field name, or protocol structure:

| TODO | Function | What it does |
|---|---|---|
| 1 | `Server.Start` | Opens the TCP listener, rejects duplicate starts, saves lifecycle state, launches the accept loop |
| 2 | `Server.Address` | Returns the real listener address (including OS-chosen port when port `0` was requested), safely |
| 3 | `Server.Stop` | Signals shutdown, closes the listener, waits for the accept loop and all active handlers, and permits restart |
| 4 | `Server.handle` | Reads one JSON request, returns error responses for bad input, writes one JSON response, closes the connection |
| 5 | `validateRequest` | Enforces method-name and size limits before reflection runs |
| 6 | `Server.invoke` | Reflection bridge: converts JSON arguments to Go types, calls the method, normalizes results/errors/panics |
| 7 | `Client.Call` | One connection per call, deadline, request encoding, remote error handling, result decoding |

**Why it matters.** These are the mechanics behind nearly every distributed system: a wire protocol, careful validation at a trust boundary, concurrency, graceful shutdown, and translating local failures into remote errors without crashing the server.

**Verification.** From `lab1/`:

```sh
go test ./...        # PASS — all 12 supplied tests
go test -race ./...  # PASS — no data races
go run ./cmd/ping    # prints Pong1, Pong2, Pong3, Pong4
```

---

## 2. Wire protocol and supported method shapes

A **request** is one JSON object with a method name and an array of JSON-encoded arguments:

```json
{ "method": "Add", "args": [2, 3] }
```

A **successful response** carries `ok: true` and the JSON result:

```json
{ "ok": true, "value": 5 }
```

An **error response** carries `ok: false` and a message:

```json
{ "ok": false, "error": "unknown method \"Missing\"" }
```

Rules from the sheet:

- Exactly **one request and one response per TCP connection**; the server closes the connection afterwards.
- Methods may have zero or more typed arguments and may return: nothing, one value, one value + `error`, or only `error`.
- A non-nil method `error` becomes a remote error.
- Methods must be **exported** (Go reflection only exposes exported methods by name).

Limits enforced before invoking user code:

| Constant | Value | Purpose |
|---|---|---|
| `maxArguments` | `8` | Maximum number of arguments per request |
| `maxArgumentBytes` | `1 << 20` (1 MiB) | Maximum encoded size of a single argument |
| `maxRequestBytes` | `4 << 20` (4 MiB) | Maximum combined encoded argument size / reader cap |
| `defaultCallTimeout` | `5 * time.Second` | Default connect + I/O deadline for a client call |

---

## 3. File map

| File | Role |
|---|---|
| `rpc/rpc.go` | All production code: protocol structs, server, reflection bridge, client |
| `rpc/rpc_test.go` | Supplied test suite (test service + 12 tests) |
| `cmd/ping/main.go` | Runnable end-to-end demo |
| `README.md` / this file | Build instructions and explanation |
| `go.mod` | Module named `lab1`, Go 1.20 |

---

## 4. `rpc/rpc.go` — line by line

### 4.1 Imports (lines 4–15)

| Package | Why it is needed |
|---|---|
| `bufio` | Buffers reads/writes on the connection for the JSON decoder/encoder |
| `encoding/json` | Marshals arguments/results and decodes requests/responses |
| `errors` | Constructs simple sentinel-style error messages |
| `fmt` | Formats detailed error messages (method names, sizes, argument indices) |
| `io` | `io.LimitReader` caps how many bytes a request may consume |
| `net` | `net.Listen`, `net.DialTimeout`, `net.Conn`, `net.Listener` |
| `reflect` | Finds and calls methods by name, inspects types, converts arguments |
| `sync` | `sync.Mutex` protects server state; `sync.WaitGroup` tracks active handlers |
| `time` | Call timeouts and deadlines |
| `unicode` | Detects whitespace/control characters in method names |

### 4.2 Protocol structs (lines 24–33)

- **`request`** — `Method string` and `Args []json.RawMessage`. `RawMessage` keeps each argument as raw JSON bytes so it can be unmarshalled later into the *actual parameter type* discovered by reflection. This is the key trick that allows typed arguments without a fixed schema.
- **`response`** — `OK bool` says success/failure; `Value json.RawMessage` holds the already-encoded result (`omitempty` omits it for failures); `Error string` holds the failure message (`omitempty` omits it on success).

### 4.3 `Server` struct (lines 35–43)

| Field | Meaning |
|---|---|
| `receiver any` | The object whose exported methods are exposed; required to be a non-nil pointer |
| `listener net.Listener` | Active TCP listener. **Also acts as the "is running" flag**: `nil` means stopped |
| `stopped chan struct{}` | Closed by `Stop` to signal the accept loop to exit |
| `acceptDone chan struct{}` | Closed by the accept loop so `Stop` can wait for it |
| `workers sync.WaitGroup` | Counts in-flight connection handlers so `Stop` can drain them |
| `mu sync.Mutex` | Guards `listener`, `stopped`, `acceptDone`, and worker registration |

### 4.4 `NewServer(receiver any) (*Server, error)` (lines 45–55)

Validates the receiver *before* any listener exists:

1. `receiver == nil` → error `"receiver cannot be nil"`.
2. Reflect on it: it must be a **pointer** and not nil (`reflect.Pointer`, `IsNil`), otherwise `"receiver must be a non-nil pointer"`. A pointer is required because the RPC methods are expected to have pointer receivers (so they can hold state and be mutated).
3. Returns `&Server{receiver: receiver}` with all lifecycle fields at their zero values — so a fresh server is "stopped" and `Address()` returns `""`.

### 4.5 `Start(address string) error` (lines 57–80) — TODO 1

```
mu.Lock() → reject if listener != nil → net.Listen("tcp", address)
→ store listener/stopped/acceptDone → go acceptLoop(...) → return nil
```

Key behaviours:

- **Duplicate start rejected:** if `s.listener != nil`, returns `"server already started"` — no second listener is created.
- **Startup errors propagate:** the exact error from `net.Listen` is returned (e.g., port in use).
- **Port 0:** `net.Listen("tcp", "127.0.0.1:0")` lets the OS choose a free port; the chosen address is read later from `listener.Addr()` by `Address()`.
- **Lifecycle state saved:** fresh `stopped` and `acceptDone` channels are allocated on every start. Passing them explicitly into `acceptLoop` means an old loop can never be signalled by a later `Start` (important for safe restart).
- **Non-blocking:** the accept loop runs in a goroutine; `Start` returns before any client connects.
- The whole function holds `mu` so two concurrent `Start` calls cannot both create listeners.

### 4.6 `Address() string` (lines 82–91) — TODO 2

- Locks `mu`, returns `""` if `listener == nil` (before start or after stop).
- Otherwise returns `listener.Addr().String()` — the **actual** bound address, which is how port 0 discovery works.
- Locking is required because `Start`/`Stop` mutate `listener` from other goroutines while the accept loop and clients may call `Address()` concurrently.

### 4.7 `Stop() error` (lines 93–116) — TODO 3

Shutdown has to be safe, complete, and repeatable. Steps:

1. Lock; if `listener == nil`, return `nil` immediately (safe before start and on repeated calls).
2. Snapshot `listener`, `stopped`, `acceptDone` into locals, and **set `s.listener = nil` while still holding the lock**. This is the crucial ordering: `acceptLoop` checks `s.listener == nil` under the same lock before registering a new worker, so after this point no new `WaitGroup.Add` can happen.
3. Unlock, then `close(stopped)` (signals the loop), `listener.Close()` (unblocks a blocked `Accept()`), `<-acceptDone` (wait for the loop to exit), `s.workers.Wait()` (wait for handlers already running).
4. Return the listener's close error (normally `nil`).

Because the channels are recreated in `Start`, after `Stop` returns the server is fully restartable.

### 4.8 `acceptLoop(listener, stopped, acceptDone)` (lines 118–148) — helper

- `defer close(acceptDone)` guarantees `Stop` never waits forever, even if the loop exits for another reason.
- Infinite `listener.Accept()`:
  - On error, it checks whether `stopped` was closed (it may have been a normal shutdown close) and returns either way.
  - On success, it locks `mu` and checks `s.listener == nil`. If `Stop` already ran, it closes the new connection and exits instead of starting a handler that `Stop` would not wait for.
  - Otherwise `workers.Add(1)` **under the lock**, unlock, and spawn `go s.handle(connection)` wrapped in `defer workers.Done()`.
- Each connection is therefore handled concurrently, and `Stop` drains them all.

### 4.9 `handle(connection net.Conn)` (lines 150–186) — TODO 4

One function = one connection = one request/response cycle.

1. `defer connection.Close()` — connection is always closed, including on malformed input and panics (the sheet's "one connection, one response" rule).
2. Creates the JSON encoder and installs a **deferred recover**: if anything panics outside the safer reflection call, it still writes `{"ok":false,"error":"remote method panicked: ..."}` instead of killing the process.
3. Decodes **one** JSON value through `io.LimitReader(connection, maxRequestBytes+1)`. The `+1` allows the decoder to see that the payload exceeded the cap rather than silently truncating a valid request. Decode failure → `"invalid request: ..."` error response.
4. Calls `validateRequest`; failure → error response with the validation message.
5. Calls `s.invoke`; failure (unknown method, bad argument count/type, returned error, or panic) → error response.
6. Marshals the result to `json.RawMessage`; marshalling failure → `"encode result: ..."` error response.
7. Writes `response{OK: true, Value: payload}`.

Every early return still writes exactly one response, so a client can never hang waiting forever.

### 4.10 `validateRequest(req request) error` (lines 188–215) — TODO 5

Protocol-limit gate that runs **before reflection or application code**:

1. Empty `Method` → `"method cannot be empty"`.
2. Any rune in `Method` for which `unicode.IsSpace` or `unicode.IsControl` is true (space, tab, newline, NUL, etc.) → `"method name contains whitespace or control characters"`. This blocks malformed/ambiguous method names and keeps error messages readable.
3. `len(req.Args) > maxArguments` → `"too many arguments: got N, limit is 8"`.
4. Any single argument longer than `maxArgumentBytes` → `"argument i is larger than 1048576 bytes"`.
5. Sum of all argument lengths greater than `maxRequestBytes` → `"encoded arguments are larger than 4194304 bytes"`.

Why before reflection: it bounds work and memory before unmarshalling or calling user code, and it guarantees that even a hostile request produces a clean error response instead of a crash or huge allocation.

### 4.11 `invoke(req request) (any, error)` (lines 217–258) — TODO 6

The reflection bridge. Step by step:

1. `reflect.ValueOf(s.receiver).MethodByName(req.Method)` — looks up the exported method. Invalid result → `unknown method "X"`.
2. `typeOfMethod.NumIn() != len(req.Args)` → `method "X" expects N arguments, got M`. For a bound method, `NumIn()` counts only the declared parameters (not the receiver).
3. For each argument:
   - `reflect.New(typeOfMethod.In(index))` allocates a **pointer to the parameter's exact Go type**.
   - `json.Unmarshal(raw, argument.Interface())` decodes the raw JSON into that typed value; failure → `argument i: <json error>`. This is what turns `[2, 3]` into `int(2)`, `int(3)`.
   - `arguments[index] = argument.Elem()` produces the value expected by `Call`.
4. `callMethodSafely(method, arguments)` performs the actual call (see below).
5. Result normalization:
   - No outputs → return `nil, nil` (method returns nothing).
   - If the **last** output implements `error`:
     - If it is non-nil → return it as the error → client sees a remote error.
     - If nil → drop it from the output list.
   - If nothing remains → `nil, nil` (error-only method that succeeded).
   - Otherwise return `outputs[0].Interface()` as the value.
6. This covers all required shapes: `NoArgs() string`, `Add(int,int) int`, `Echo(string) string`, `OnlyError(bool) error`, `Ping(int) (string, error)`, and `NoResult()`.

### 4.12 `isNilValue(value reflect.Value) bool` (lines 260–267)

`reflect.Value.IsNil()` panics for value kinds like `int` or `string`, so this helper only calls it for nil-able kinds: channel, func, interface, map, pointer, slice. Everything else is definitively non-nil. Used by `invoke` to decide whether the final `error` return is nil.

### 4.13 `callMethodSafely(...)` (lines 269–276)

Wraps `method.Call(arguments)` in a deferred `recover`. A panic inside the remote method (e.g., `Panic()` in the tests) becomes `error: "remote method panicked: test panic"` instead of crashing the server goroutine. Because the panic is recovered here, `handle` can still write a clean error response.

### 4.14 `Client` struct / `NewClient` (lines 278–290)

- `Client{Address string, Timeout time.Duration}` — the address of a server and a per-call timeout; `Timeout` is exported so tests can shorten it (e.g., 20 ms for the timeout test).
- `NewClient` rejects an empty address and sets `Timeout = defaultCallTimeout` (5 s).

### 4.15 `Client.Call(method string, out any, args ...any) error` (lines 292–344) — TODO 7

1. Empty method name → error immediately (no network traffic).
2. Encode each argument with `json.Marshal` into `[]json.RawMessage`, wrapping failures as `encode argument i: ...`.
3. Timeout: use `c.Timeout`, falling back to `defaultCallTimeout` if it is zero or negative.
4. `net.DialTimeout("tcp", c.Address, timeout)` → connect failures (e.g., no server on port 1) return `connect to ...: ...`.
5. `defer connection.Close()` — exactly one connection per call, always closed.
6. `connection.SetDeadline(time.Now().Add(timeout))` applies **one absolute deadline to both the write and the read**, so a slow method causes a read timeout error rather than an infinite wait.
7. Encode `request{Method, Args}` with `json.NewEncoder`.
8. Decode one `response` with `json.NewDecoder(bufio.NewReader(connection))`.
9. If `!reply.OK`, return `errors.New(reply.Error)` (or a generic error if the server sent an empty message) — this is how remote errors reach the caller.
10. If `out == nil` or the response has no value → success with nothing decoded (for methods with no result).
11. Otherwise `json.Unmarshal(reply.Value, out)`; a mismatch (e.g., result `3` into `*string`) returns `decode result: ...`, satisfying the test that looks for `"decode"`.
12. Return `nil` on full success.

---

## 5. `cmd/ping/main.go` — line by line

| Code | Explanation |
|---|---|
| `type pingServer struct{}` | Empty receiver type; the methods are the entire API. |
| `func (*pingServer) Ping(id int) (string, error)` | The one exposed method. Returns `"Pong%d"` — this is the method the client invokes remotely. Its `(string, error)` shape also exercises the "value + error" path in `invoke`. |
| `rpc.NewServer(&pingServer{})` | Creates a server for the receiver; `log.Fatal` on error. |
| `server.Start("127.0.0.1:0")` | Starts on loopback with an OS-assigned port. |
| `defer server.Stop()` | Ensures graceful shutdown when `main` returns. |
| `rpc.NewClient(server.Address())` | Discovers the chosen port through `Address()` and builds a client. |
| `for id := 1; id <= 4; ...` | Four sequential calls; each uses a fresh connection and prints the returned string. Output: `Pong1`…`Pong4`. |
| `time.Sleep(10 * time.Millisecond)` | Tiny pause before `defer Stop()` runs, so output is flushed cleanly. |

---

## 6. `rpc/rpc_test.go` — every test explained

### 6.1 The test service (lines 13–47)

A single receiver type exposing all required method shapes:

| Method | Shape / purpose |
|---|---|
| `Ping(id int) (string, error)` | Value + error; returns `"Pong"+digit`, errors on negative id → remote error test |
| `Add(left, right int) int` | Typed arguments and a single value result |
| `Panic() (string, error)` | Deliberately panics → panic-to-error test |
| `Slow() string` | Signals `slowStarted` (if provided) then sleeps 150 ms → shutdown-wait and client-timeout tests |
| `NoArgs() string` | Zero-argument method |
| `NoResult()` | No arguments, no result (`out = nil`) |
| `Echo(value string) string` | String round-trip; also used for the oversized-request test |
| `OnlyError(bool) error` | Error-only return: nil error on `false`, `"requested failure"` on `true` |

### 6.2 Tests

| Test | What it proves |
|---|---|
| `TestValidateRequest` | Table of invalid requests: empty method, space, control char, too many args, oversized single arg, oversized combined args — each must produce an error containing the expected text; two valid requests must be accepted. |
| `TestServerClient` | End-to-end: start on port 0, use `Address()`, call `Add(20,22) = 42`, call `Ping(4) = "Pong4"`, and receive a remote error for `Ping(-1)`. |
| `TestServerClientMethodShapes` | All return shapes: `NoArgs`, `NoResult` with `nil` output, `Echo`, `OnlyError(false)` success, `OnlyError(true)` error containing `"requested failure"`. |
| `TestInvalidRemoteCallsBecomeErrors` | Bad input becomes an error, never a crash: unknown method (`"unknown method"`), wrong argument count (`"expects 2 arguments"`), wrong JSON type (`"argument 0"`), and result that cannot be decoded into the output type (`"decode"`). |
| `TestRemotePanicBecomesError` | `Panic()` must yield an error containing `"panicked"`; the server must survive. |
| `TestConcurrentCalls` | 20 goroutines call `Add` at once; all results must be correct — proves per-connection goroutines and thread-safe state. |
| `TestStopAndRestart` | Stop, verify `Address() == ""`, start again, verify a new address exists. |
| `TestServerLifecycleErrors` | `Address()` before start is `""`; `Stop()` before start and a second `Stop()` are harmless; a second `Start()` fails; address is cleared after stop. |
| `TestStopWaitsForActiveHandler` | Starts `Slow()` in a goroutine, waits for `slowStarted`, calls `Stop()`, and asserts it took at least 100 ms and that the in-flight call still completed successfully — proves `workers.Wait()`. |
| `TestClientTimeout` | Sets `Timeout = 20 ms` and calls the 150 ms `Slow()`; the deadline must produce an error. |
| `TestClientConnectionFailures` | Calls a client pointed at `127.0.0.1:1`; dial failure must be reported. |
| `TestMalformedRequestGetsErrorResponse` | Raw TCP client writes `"not json\n"`; must receive a response with a non-empty `error` field. |
| `TestOversizedRequestGetsErrorResponse` | Raw TCP client writes an `Echo` request with a 1 MiB argument (raw JSON exceeds `maxArgumentBytes`); must receive an error response. |

### 6.3 Helpers

- `startTestServer(t)` — creates a server on port 0 and registers `server.Stop()` via `t.Cleanup`.
- `testClient(t, server)` — creates a client using the server's discovered address.

---

## 7. Design insights (why the code is written this way)

1. **`listener == nil` is the single source of truth** for "running". It makes duplicate start, empty address, idempotent stop, and restart all fall out of one invariant, and it lets `acceptLoop` participate in the shutdown handshake via the same mutex.
2. **No `WaitGroup.Add` after `Wait` begins.** `Stop` clears `listener` under the lock before calling `Wait`; `acceptLoop` only calls `Add` while holding that same lock and after checking `listener != nil`. This avoids a classic concurrent-Add/Wait race.
3. **Validation is a security boundary.** All size and name checks happen before JSON unmarshalling into reflection-discovered types and before user code runs.
4. **Errors travel, panics do not.** Both `callMethodSafely` and `handle` have recover blocks, so a broken remote method becomes a JSON error response; the server process keeps serving other clients.
5. **One connection per call + deadlines** gives simple request/response framing (the decoder reads exactly one JSON value) and guarantees no client can wait forever.

---

## 8. Quick reference: error messages produced

| Situation | Error text (fragment) |
|---|---|
| Empty method name (client or validation) | `method cannot be empty` |
| Unsafe method name | `whitespace` |
| Too many arguments | `too many arguments` |
| Oversized argument/request | `larger than` |
| Unknown method | `unknown method` |
| Wrong argument count | `expects N arguments` |
| Bad argument JSON type | `argument i` |
| Remote method returned error | the method's own message (e.g., `requested failure`) |
| Remote method panicked | `remote method panicked` |
| Malformed JSON | `invalid request` |
| Result cannot be decoded | `decode result` |
| Timeout / unreachable server | `read response` / `connect to ...` |
