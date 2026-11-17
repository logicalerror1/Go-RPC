# Go-RPC

Distributed Processing course lab solutions in Go.

- **[Lab 1 — Reflection-Based RPC](lab1/README.md):** JSON-over-TCP RPC system
  that invokes exported Go methods via reflection, with safe server lifecycle,
  request validation, concurrency, and error handling.
- **[Lab 2 — Raft](lab2/README.md):** Raft consensus — leader election,
  heartbeats, replicated log, commitment, and a simulated RPC network that can
  drop, delay, and reorder messages.

## Run the tests

```sh
cd lab1 && go test ./... && go test -race ./...
cd ../lab2 && go test ./... && go test -race ./...
```
