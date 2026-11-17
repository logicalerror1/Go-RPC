# Lab 2 — Executive Summary & Complete Code Explanation

## 1. Executive summary

**What the lab is.** Lab 2 asks you to implement **Raft**, a consensus algorithm that lets a group of servers agree on an ordered sequence of commands even when some servers fail or the network misbehaves. The handout supplied `Lab 2.pdf` (the specification) and `raft.pdf` (the extended Raft paper) but **no Go starter code**, so the Go module, the `labrpc` network simulator, and the test suite were all written from scratch to match the public interface in the sheet.

The work is split into two milestones:

- **Checkpoint (3A):** leader election and heartbeats. A single leader must be elected when a majority can communicate, and a new leader must be elected when the old one fails.
- **Final (3B):** log replication and commitment. The leader accepts commands via `Start`, replicates them, commits them by majority rule, and each server delivers committed entries to the service through `applyCh` in order.

**What we built.**

| File | Contents |
|---|---|
| `raft/raft.go` | Full Raft state machine: roles, terms, votes, log, election timers, heartbeats, replication, commitment, apply loop |
| `labrpc/labrpc.go` | RPC/network simulator: endpoints, servers, services, gob encoding, packet loss, delays, reordering, partitions, crashes |
| `raft/raft_test.go` | 7 tests covering 3A and 3B, including concurrent commands and an unreliable network |

**Why it matters.** Replication gives availability, but replicas can diverge after failures. Raft forces them to agree on the same log so they compute identical state. It underpins real systems such as etcd, Consul, and Kubernetes' control plane.

**Verification.** From `lab2/`:

```sh
go test ./...                 # PASS — all 7 tests
go test -race ./...           # PASS — no data races
go test -run 3A ./raft        # PASS — checkpoint
go test -run 3B ./raft        # PASS — final milestone
```

The suite was also stress-run repeatedly (10× full suite, 5× race detector, 20× unreliable-network test) with no failures.

**Explicitly out of scope** (per the sheet): persistence/crash recovery from stable storage, cluster membership changes, snapshots, and log compaction. Snapshot fields in `ApplyMsg` exist only for interface compatibility and are ignored.

---

## 2. Public interface (must stay unchanged)

```go
rf := Make(peers, me, applyCh)          // create a replica
index, term, isLeader := rf.Start(cmd)  // begin replication (leader only)
term, isLeader := rf.GetState()         // observe current term and leadership
```

`peers` contains one `*labrpc.ClientEnd` per server **including this one**; `me` is this server's index; `applyCh` receives one `ApplyMsg` per committed entry. `Start` must return without waiting for replication. `Kill` stops background goroutines (standard harness convenience).

---

## 3. `raft/raft.go` — data model

### 3.1 Imports and constants (lines 6–35)

| Import | Use |
|---|---|
| `fmt` | Formatting debug log messages |
| `log` | Debug logging when `debug = true` |
| `math/rand` | Randomized election/retry timeouts |
| `sync` | Mutex, condition variable, shutdown channel |
| `time` | Heartbeats, election deadlines, random durations |
| `lab2/labrpc` | Peer-to-peer RPC endpoints |

Roles are `follower = 0`, `candidate = 1`, `leader = 2` (iota constants).

| Constant | Value | Reason |
|---|---|---|
| `heartbeatInterval` | 120 ms | ~8 heartbeats/second — below the paper's "not more than 10/s" limit and safely below election timeouts |
| `electionTimeoutMin/Max` | 150–300 ms | Paper's recommended randomized election timeout range |
| `candidateRetryMin/Max` | 600–900 ms | Candidates retry more slowly so delayed/reordered vote replies can still arrive before a new election starts |
| `tickerInterval` | 10 ms | Poll interval for detecting an expired election deadline |
| `debug` | `false` | Toggle for verbose diagnostics |

### 3.2 Types (lines 37–116)

- **`LogEntry{Term int, Command interface{}}`** — one replicated log slot. Commands are stored as `interface{}`, so the implementation does not assume integers.
- **`ApplyMsg`** — the message delivered to the service: `CommandValid`, `Command`, `CommandIndex`, plus `SnapshotValid`, `Snapshot`, `SnapshotIndex`, `SnapshotTerm`, `UseSnapshot` retained for interface compatibility but never set.
- **`RequestVoteArgs{Term, CandidateId, LastLogIndex, LastLogTerm}`** and **`RequestVoteReply{Term, VoteGranted}`** — candidate's vote request; the log freshness fields implement the election restriction.
- **`AppendEntriesArgs{Term, LeaderId, PrevLogIndex, PrevLogTerm, Entries, LeaderCommit}`** — heartbeat + replication RPC; empty `Entries` means a pure heartbeat.
- **`AppendEntriesReply{Term, Success, ConflictIndex, ConflictTerm}`** — the two conflict fields implement **fast log repair** (skip a whole conflicting term instead of backing up one entry at a time).
- **`Raft` struct** — every mutable field is protected by `mu`:

| Field group | Fields | Purpose |
|---|---|---|
| Identity | `peers`, `me`, `applyCh` | RPC endpoints, own index, delivery channel |
| Paper state | `currentTerm`, `votedFor`, `log` | Term, one vote per term, replicated log (index 0 is a dummy entry) |
| All servers | `state`, `commitIndex`, `lastApplied` | Role, highest committed index, highest applied index |
| Leaders only | `nextIndex[]`, `matchIndex[]` | Per-follower "next entry to send" and "highest replicated index" |
| Timing/shutdown | `electionDeadline`, `electionPending`, `random`, `done`, `killed` | Election scheduling, goroutine shutdown |
| Apply wakeup | `applyCond` | `sync.Cond` tied to `mu`, signalled when `commitIndex` advances |

---

## 4. `raft/raft.go` — every function

### 4.1 `logf(format, args...)` (lines 118–122)

Debug-only logger: prefixes the peer id and current term, and does nothing when `debug == false` (so no cost during normal tests).

### 4.2 `Make(peers, me, applyCh) *Raft` (lines 124–146)

Constructor. Initializes:

- `votedFor = -1` (no vote yet), `state = follower`, `currentTerm/commitIndex/lastApplied = 0`.
- `log = []LogEntry{{Term: 0, Command: 0}}` — the dummy entry at index 0 makes log indices 1-based, matching the paper and `ApplyMsg.CommandIndex`.
- `nextIndex` / `matchIndex` arrays sized to the cluster.
- A per-peer-seeded `rand.Rand` so different servers choose different timeouts (seeded with `UnixNano + me*7919`).
- A `done` channel for shutdown and `applyCond` bound to `&rf.mu`.
- Calls `resetElectionTimer()`, then starts three background goroutines: `ticker`, `applier`, `heartbeatLoop`.

### 4.3 `Start(command) (index, term, isLeader)` (lines 148–163)

- Rejects (returns `-1, term, false`) if killed or not the leader.
- Appends `LogEntry{Term: currentTerm, Command: command}` to the local log; index is the new last index.
- Launches `go rf.broadcastAppendEntries()` so replication begins **immediately**, not at the next heartbeat.
- Returns without waiting for any follower — as required by the interface.

### 4.4 `GetState() (term, isLeader)` (lines 165–170)

Thread-safe snapshot of `currentTerm` and `state == leader`, used by tests and by candidates/leaders to observe the world.

### 4.5 `Kill()` (lines 172–181)

Idempotent shutdown: sets `killed`, wakes the applier via `applyCond.Broadcast()`, and closes `done` so `ticker` and `heartbeatLoop` exit promptly. Guarded by `mu` and a `killed` check so closing `done` happens exactly once.

### 4.6 `RequestVote(args, reply)` (lines 183–208) — RPC handler

Voting rules from the paper:

1. A `defer` sets `reply.Term = rf.currentTerm` on every path, including after adopting a higher term.
2. `args.Term < currentTerm` → reject (stale candidate).
3. `args.Term > currentTerm` → `becomeFollower(args.Term, -1)` (adopt the new term, clear the old vote).
4. One vote per term: if `votedFor != -1 && votedFor != args.CandidateId` → reject.
5. **Election restriction:** `isUpToDate(args.LastLogTerm, args.LastLogIndex)` must be true; otherwise reject — this prevents a server with stale log from becoming leader.
6. Otherwise grant the vote, record `votedFor`, and **reset the election timer** (a granted vote is evidence of progress).

### 4.7 `AppendEntries(args, reply)` (lines 210–278) — RPC handler

Handles both heartbeats and log replication:

1. Reply defaults: `Success=false`, `ConflictIndex=-1`, `ConflictTerm=-1`, and a deferred `reply.Term = rf.currentTerm`.
2. `args.Term < currentTerm` → reject as stale.
3. `args.Term > currentTerm` **or this peer is not a follower** → `becomeFollower(args.Term, -1)`. A candidate that sees a valid leader in the same term steps down.
4. `resetElectionTimer()` — a valid leader's message prevents an election.
5. **Consistency check:** if `PrevLogIndex` is beyond the local log → reply `ConflictIndex = len(log)`. If `log[PrevLogIndex].Term != PrevLogTerm`, find the first index of that conflicting term and reply with `ConflictTerm` + `ConflictIndex` so the leader can skip the whole term.
6. On match, append entries: walk `args.Entries`; matching entries (same term) are skipped; at the first conflict the local suffix is truncated and the leader's remaining entries appended in one operation.
7. **Commit update:** if `LeaderCommit > commitIndex`, set `commitIndex = min(LeaderCommit, index of last new entry, len(log)-1)` and wake the applier with `applyCond.Broadcast()`.

### 4.8 State-transition helpers (lines 280–316)

- **`becomeFollower(term, votedFor)`** — sets role/term/vote, cancels any pending election (`electionPending = false`), and resets the election timer.
- **`becomeCandidate()`** — role = candidate, `currentTerm++`, vote for self, reset election timer.
- **`becomeLeader()`** — role = leader; initialize every peer's `nextIndex = len(log)` and `matchIndex = 0`.
- **`resetElectionTimer()`** — deadline = now + random 150–300 ms.
- **`resetCandidateTimer()`** — deadline = now + random 600–900 ms (gives delayed votes time).
- **`randomTimeout(min, max)`** — uniform random duration in `[min, max)`.

### 4.9 `isUpToDate(lastLogTerm, lastLogIndex)` (lines 318–324)

Compares the candidate's last log entry with this server's: higher last term wins; if terms are equal, the longer log wins (`>=`). This is Raft's election safety rule — a leader must contain all committed entries.

### 4.10 `advanceCommitIndex()` (lines 326–351)

Leader-only. Scans backward from the end of its log:

- Skips entries whose term != `currentTerm`. This is the **current-term commitment restriction**: only entries from the leader's own term may be committed by counting replicas (older entries get committed implicitly when a newer one commits).
- Counts how many peers have `matchIndex >= index` (leader counts itself).
- On a majority, sets `commitIndex = index`, broadcasts to the applier, and stops — applying in order.

### 4.11 `ticker()` (lines 353–379)

Every 10 ms (or exit on `done`/`killed`):

- If this peer is not the leader, no election is already pending, and the deadline has passed → set `electionPending = true` and spawn `go startElection()`.
- `electionPending` prevents many goroutines from starting elections for the same timeout; `becomeFollower` clears it when a legitimate leader/vote is seen.

### 4.12 `applier()` (lines 381–418)

Dedicated goroutine that delivers committed entries to the service:

- Waits on `applyCond` while `lastApplied >= commitIndex`.
- Clamps the target index to `len(log)-1` defensively.
- Copies `ApplyMsg`s for indices `lastApplied+1 … commitIndex` **while holding the lock** (so the log cannot change mid-copy).
- **Unlocks before sending to `applyCh`** — a slow/blocked service can never hold up RPC handlers or elections.
- Re-locks and advances `lastApplied` only after all messages were sent, guaranteeing **in-order, exactly-once** delivery.

### 4.13 `heartbeatLoop()` (lines 420–431)

Every 120 ms (or exit on `done`) calls `broadcastAppendEntries()`. This is how the leader maintains authority and how followers learn the current `commitIndex` even when there are no new commands.

### 4.14 `startElection()` (lines 433–487)

1. Under lock: bail out if killed, already leader, or no pending election; clear `electionPending`; `becomeCandidate()`; use the longer candidate retry timer; snapshot `term`, last log index/term; start with 1 vote (self).
2. Send `RequestVote` to every other peer in parallel goroutines.
3. Each reply handler re-locks and checks: RPC succeeded, still the same term, still a candidate, not killed — stale replies are ignored.
4. A reply with a higher term → `becomeFollower(reply.Term, -1)`.
5. A granted vote increments `votes` (all accesses under `mu`); on a majority → `becomeLeader()`, then unlock, send an immediate heartbeat broadcast, and re-lock (so the broadcast does not run while holding the lock).

### 4.15 `broadcastAppendEntries()` (lines 489–509)

Leader-only. Snapshots the list of other peers under the lock, unlocks, and starts one `go replicateTo(peer)` per peer. Using one goroutine per peer means a slow peer cannot block others, and failures are retried per peer (see below) rather than re-broadcasting to the whole cluster — important for RPC-count limits.

### 4.16 `replicateTo(peer)` (lines 511–601)

The workhorse of 3B:

1. **Build args under lock:** clamp `nextIndex` to `[1, len(log)]`; copy the log suffix from `nextIndex` into `Entries`; set `PrevLogIndex = next-1`, `PrevLogTerm = log[next-1].Term`, `LeaderCommit = commitIndex`; snapshot the term.
2. **Send outside the lock** via `sendAppendEntries`; this is essential — RPCs can be delayed for seconds.
3. Re-lock; ignore the reply if the RPC failed, the node was killed, leadership/term changed, or the term is stale.
4. A reply term higher than ours → `becomeFollower` (step down).
5. **Success:** compute `match = PrevLogIndex + len(Entries)`. Only if it exceeds `matchIndex[peer]` (monotonicity — a delayed success from an older attempt can never regress progress), raise `matchIndex`, raise `nextIndex`, and call `advanceCommitIndex()`.
6. **Failure:** ignore it if `nextIndex` no longer equals `PrevLogIndex+1` (the request was superseded). Otherwise compute a new `nextIndex`:
   - If `ConflictTerm != -1`, find the last entry with that term in the leader's log and continue after it; if absent, use the follower's `ConflictIndex`.
   - Otherwise use `ConflictIndex` directly (follower's log is shorter).
   - Clamp to `[1, len(log)]`; if it changed, retry **only this peer** with `go replicateTo(peer)`.

This combination (fast repair + monotonic progress + stale-reply guards) is what makes recovery from partitions and conflicting logs both correct and efficient.

### 4.17 `sendRequestVote` / `sendAppendEntries` (lines 603–611)

Thin wrappers around `rf.peers[server].Call("Raft.RequestVote", ...)` and `Call("Raft.AppendEntries", ...)`. They are the single place where the Raft state machine touches the network, returning `false` when labrpc drops the message.

---

## 5. `labrpc/labrpc.go` — every part

### 5.1 Purpose and message types

The package emulates Go's RPC over an unreliable network using channels and **gob** encoding (so values are copied, not shared by reference — peers cannot cheat by reading each other's memory).

- **`reqMsg`** — carries `endname` (which client link sent it), `svcMeth` (e.g. `"Raft.AppendEntries"`), `argsType`, gob-encoded `args`, and a per-call `replyCh`.
- **`replyMsg`** — `ok` (false = lost/dead server) and gob-encoded `reply`.

### 5.2 `ClientEnd` and `Call` (lines 45–82)

- `ClientEnd` holds the endpoint name, a channel into the network, and a `done` channel closed on cleanup.
- `Call(svcMeth, args, reply) bool`:
  1. Builds `reqMsg`, gob-encodes `args`; encoding failure → `false`.
  2. Sends the request to the network's central channel, or returns `false` if the network was cleaned up.
  3. Waits for the reply channel; `ok == false` means the request/reply was dropped or the server died.
  4. gob-decodes into `reply`; decode failure → `false`. Returns `true` on success.
- It is safe to have multiple concurrent `Call`s on one endpoint, and replies may arrive out of order — Raft is designed for that.

### 5.3 `Network` struct and `MakeNetwork` (lines 84–125)

Fields: a mutex; reliability flags (`reliable`, `longDelays`, `longReordering`); maps of `ends`, `enabled` links, `servers`, and `connections` (end → server); a central `endCh`; `done` + `cleanupOnce`; and atomic RPC/byte counters.

`MakeNetwork` initializes everything as reliable and starts one dispatcher goroutine that reads `endCh`, counts the RPC, and spawns `processReq` for each message.

### 5.4 Settings and lifecycle (lines 127–152)

- `Cleanup()` — closes `done` once, stopping the dispatcher.
- `Reliable(bool)` — toggles packet loss and short delays.
- `LongDelays(bool)` — makes calls on dead links fail after up to 7 s (used to prove Raft never blocks on RPCs).
- `LongReordering(bool)` — occasionally delays replies by 200–2200 ms.

### 5.5 `endpointInfo` / `serverDead` / `processReq` (lines 154–225)

- `endpointInfo` — under lock, snapshots whether the link is enabled, which server it points to, the server object, and the reliability flags.
- `serverDead` — true if the link was disabled or the server object was replaced/deleted (simulates a crash).
- `processReq` — the simulator core:
  - If link enabled and server alive: when unreliable, sleep 0–26 ms and drop 10% of requests; dispatch the handler in a goroutine while polling every 100 ms for server death; drop 10% of replies; optionally delay replies for reordering; otherwise deliver.
  - If link disabled / no server: schedule a failed reply after 0–100 ms (or up to 7 s with `LongDelays`).
  - This is why `Call` can return `false` at any time, just like a real network timeout.

### 5.6 Endpoint/server management (lines 227–304)

| Function | Behaviour |
|---|---|
| `MakeEnd(endname)` | Creates a client endpoint with a unique name; starts **disabled** until connected |
| `DeleteEnd(endname)` | Removes an endpoint and its link state |
| `AddServer(name, srv)` | Registers a server (future calls can reach it) |
| `DeleteServer(name)` | Sets the server to `nil` — in-flight and future calls to it fail (crash simulation) |
| `Connect(endname, servername)` | Points an endpoint at a server and enables it (verifies the server exists) |
| `Enable(endname, bool)` | Turns a link on/off — the primitive used to create network partitions |
| `Disconnect(...)` | Convenience alias for `Enable(endname, false)` |
| `GetCount(name)` / `GetTotalCount()` / `GetTotalBytes()` | RPC statistics for tests |

### 5.7 `Server`, `Service`, and dispatch (lines 306–407)

- **`Server`** — a dispatcher owning a map of named services and an incoming-RPC count.
- `MakeServer()` / `AddService(svc)` / `Register(rcvr)` — construct a server and attach services; `Register` is shorthand for `AddService(MakeService(rcvr))`.
- `Server.dispatch(req)` — splits `"Raft.AppendEntries"` at the last dot into service + method, increments the count under lock, and delegates.
- **`Service`** — holds the receiver value and the set of callable methods.
- `MakeService(rcvr)` — uses reflection to keep only **exported** methods with exactly the handler shape `func (*T) Method(args *Args, reply *Reply)` (3 inputs including receiver, pointer reply, no return values). For our `Raft` type that keeps `RequestVote` and `AppendEntries`; the service name becomes `"Raft"`, matching the RPC method strings.
- `Service.dispatch` — allocates and gob-decodes the argument value, allocates the reply value, calls the method reflectively, gob-encodes the reply, and returns it. Any failure returns `ok: false`, which `ClientEnd.Call` reports as a network failure.

---

## 6. `raft/raft_test.go` — every part

### 6.1 Helpers (lines 12–192)

| Helper | Purpose |
|---|---|
| `endName(from,to)` / `serverName(peer)` | Unique names for each link and each Raft server |
| `testConfig` | Holds the network, all Raft peers, their apply channels, and a mutex-protected log of applied messages |
| `makeTestConfig(t, n)` | Creates n endpoints per peer, registers each `Raft` as a `labrpc` service, connects all links, starts a collector goroutine per `applyCh`, and registers cleanup (`Kill` + `Cleanup`) |
| `disconnect` / `connect` | Toggle a single directed link |
| `isolate` / `rejoin` | Cut/restore all links to one peer (partition simulation) |
| `leaders()` | List peers that currently believe they are leader |
| `singleLeader(timeout)` | Wait until exactly one leader exists; return it |
| `singleLeaderAmong(peers, timeout)` | Same, but only among a connected subset (an isolated old leader still thinks it is leader) |
| `termsAgree()` | Check all peers report the same term |
| `appliedSnapshot(peer)` | Thread-safe copy of one peer's applied messages |
| `waitFor(t, what, timeout, cond)` | Poll a condition every 10 ms until true, with a descriptive failure |
| `commandsOf(msgs)` | Extract int commands and assert `CommandIndex == index+1` (ordering check) |
| `equalCommands(a,b)` | Compare two command sequences |

### 6.2 Tests

| Test | Milestone | What it proves |
|---|---|---|
| `Test3AInitialElection` | 3A | With 3 peers, exactly one leader is elected, all terms agree, and the leader stays the same through 600 ms of heartbeats |
| `Test3AReElection` | 3A | Isolating the leader causes the remaining 4 peers to elect a different leader within 5 s; after rejoin, the old leader learns the higher term and steps down |
| `Test3BBasicAgreement` | 3B | Six commands submitted to the leader are applied identically, in order, on all peers |
| `Test3BLogRepair` | 3B | A follower isolated during five commands catches up exactly after rejoin — exercises `ConflictIndex`/`ConflictTerm` repair |
| `Test3BNewLeaderCommitsOldEntries` | 3B | After a leader change, a new leader's own-term entry commits and causes earlier entries from the old term to be applied too (current-term rule) |
| `Test3BConcurrentStart` | 3B | 12 concurrent `Start` calls produce the same 12 commands in the same order on every peer |
| `Test3BUnreliableNetwork` | 3B | With 10% packet loss and heavy reply reordering, all peers eventually converge on the same log; includes per-peer diagnostics on timeout |

---

## 7. Design insights (why the code is written this way)

1. **One mutex, short critical sections.** All shared state uses `rf.mu`. Blocking operations — RPC calls and sends to `applyCh` — happen **outside** the lock; `sync.Cond.Wait` releases it while sleeping. This prevents deadlocks and keeps RPC handlers responsive.
2. **Term numbers are the epoch.** Every delayed reply is validated against `currentTerm` and role before it can mutate state. Higher terms always win, so stale leaders/candidates step down automatically.
3. **Election safety** comes from the log-freshness check in `RequestVote` and from the current-term commitment rule; together they guarantee that a committed entry is present in any future leader's log.
4. **Fast, low-RPC log repair.** `ConflictTerm`/`ConflictIndex` skip entire conflicting terms, `matchIndex` is monotonic, and a rejection only retries the specific peer that rejected — this keeps RPC counts low while still repairing divergent logs.
5. **Liveness under delay.** Candidates retry on a 600–900 ms timer instead of 150–300 ms, so replies delayed by the simulator can still win an election. `electionPending` ensures one election attempt per timeout.
6. **Exactly-once, ordered apply.** `lastApplied` plus a condition variable and sends outside the lock guarantee that each committed index reaches `applyCh` once and in order, without stalling the protocol.

---

## 8. A normal run, step by step

1. `Make` starts all three goroutines on every peer. Random timeouts fire at different times; one peer transitions to candidate, increments the term, and requests votes.
2. Peers with fresh-enough logs and no conflicting vote grant it. On a majority, the candidate becomes leader and immediately broadcasts `AppendEntries`, then continues every 120 ms.
3. `Start(cmd)` appends a local entry and triggers an immediate broadcast. Each `replicateTo` sends the suffix after that follower's `nextIndex`.
4. Followers verify `PrevLogIndex/PrevLogTerm`, repair conflicts, append the entries, and move `commitIndex` toward the leader's `LeaderCommit`.
5. Once a majority reports `matchIndex >= index` for a current-term entry, the leader advances `commitIndex`; followers learn it through later `AppendEntries`.
6. Each peer's `applier` wakes up and sends `ApplyMsg{CommandValid: true, Command: ..., CommandIndex: ...}` for every newly committed entry, in order.

---

## 9. Quick reference: key invariants

| Invariant | Enforced by |
|---|---|
| At most one leader per term | Majority voting + one vote per term |
| A leader contains all committed entries | `isUpToDate` election restriction |
| Only current-term entries are committed by counting | `advanceCommitIndex` term check |
| Logs converge to the leader's log | `AppendEntries` consistency check + conflict repair |
| Entries are applied once and in order | `lastApplied` + `applier` goroutine |
| No data races / no deadlocks | Single mutex; no RPC or channel send under lock |
| Graceful shutdown | `done` channel + `killed` checked in all loops |
