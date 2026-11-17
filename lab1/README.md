# Lab 1 — Reflection-Based RPC System (Go)

A small RPC system where a client asks a server to invoke an exported Go method
over TCP. Requests and responses are JSON; the server discovers and calls
methods with reflection.

## Layout

- `rpc/rpc.go` — implementation: server lifecycle, request validation,
  reflection bridge, and client calls (the original TODO comments are kept).
- `rpc/rpc_test.go` — the supplied test suite.
- `cmd/ping/main.go` — runnable demo using port 0 and a `Ping` method.

## Run

```sh
cd lab1
go test ./...          # all tests
go test -race ./...    # concurrency check
go run ./cmd/ping      # demo: Pong1..Pong4
```

## What was implemented

- **Start / Address / Stop**: TCP listener creation (including OS-selected port
  0), duplicate-start rejection, thread-safe address lookup, graceful shutdown
  that closes the listener, signals the accept loop, and waits for every active
  handler before allowing a restart.
- **validateRequest**: rejects empty/unsafe method names and oversized
  arguments/requests before any reflection or application code runs.
- **handle / invoke** (provided skeleton): one JSON request per connection,
  typed argument decoding, all supported return shapes, remote errors and
  panics converted to error responses.
- **Client.Call**: one connection per call with connect/I/O deadlines, request
  encoding, remote error propagation, and result decoding into `out`.
