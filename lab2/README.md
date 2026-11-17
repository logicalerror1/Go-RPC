# Lab 2 — Raft (Go)

An implementation of the core Raft consensus algorithm: leader election,
heartbeats, replicated log, commitment, and in-order delivery of committed
entries through `applyCh`.

## Layout

- `raft/raft.go` — the Raft implementation (3A + 3B).
- `labrpc/labrpc.go` — channel-based RPC simulator used for peer communication.
  It can drop, delay and reorder messages and simulate partitions/crashes.
- `raft/raft_test.go` — tests covering elections, re-election, agreement,
  log repair, leadership changes, concurrent commands, and unreliable networks.

No Go starter archive was included with the handout (only `Lab 2.pdf` and the
Raft paper), so `labrpc` and the tests were written from scratch against the
public interface described in the sheet:

```go
rf := Make(peers, me, applyCh)
index, term, isLeader := rf.Start(command)
term, isLeader := rf.GetState()
```

## Run

```sh
cd lab2
go test ./...                 # all tests
go test -race ./...           # race detector
go test -run 3A ./raft        # checkpoint: elections + heartbeats
go test -run 3B ./raft        # final: log replication
```

## Design notes

- State (term, vote, role, log, commit/applied indices, per-peer replication
  progress) is protected by one mutex; channel sends to `applyCh` and blocking
  RPCs happen outside the lock.
- Election timeouts are randomized (150–300 ms); candidates retry more slowly
  so delayed votes can still arrive. Heartbeats run every 120 ms.
- Replication uses `AppendEntries` with fast conflict repair
  (`ConflictTerm`/`ConflictIndex`) and a monotonic `matchIndex`; stale replies
  cannot move replication progress backwards. Failure retries target only the
  rejecting peer, keeping RPC counts low.
- Commitment follows the paper's current-term restriction; committed entries
  are applied exactly once, in log order, by a dedicated applier goroutine.
- Out of scope (as stated in the sheet): persistence/crash recovery, snapshots,
  log compaction, and membership changes. Snapshot-related `ApplyMsg` fields are
  retained but unused.
