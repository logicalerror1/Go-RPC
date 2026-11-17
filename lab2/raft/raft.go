// Package raft implements the core Raft consensus algorithm (leader election,
// heartbeats, log replication and commitment) as described in the extended
// Raft paper. Persistence, snapshots and membership changes are out of scope.
package raft

import (
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

	"lab2/labrpc"
)

// Set to true when debugging a particular test.
const debug = false

const (
	follower = iota
	candidate
	leader
)

const (
	heartbeatInterval  = 120 * time.Millisecond // at most ~8 heartbeats per second
	electionTimeoutMin = 150 * time.Millisecond
	electionTimeoutMax = 300 * time.Millisecond
	// Candidates wait longer than followers: votes can be delayed or
	// reordered by the network, and retrying too early would restart the
	// election before those votes arrive.
	candidateRetryMin = 600 * time.Millisecond
	candidateRetryMax = 900 * time.Millisecond
	tickerInterval    = 10 * time.Millisecond
)

// LogEntry is one entry of the replicated log. Index 0 is a dummy entry.
type LogEntry struct {
	Term    int
	Command interface{}
}

// ApplyMsg is delivered to the service for every committed log entry.
// Snapshot-related fields are kept only for interface compatibility.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	SnapshotValid bool
	Snapshot      []byte
	SnapshotIndex int
	SnapshotTerm  int
	UseSnapshot   bool
}

// RequestVoteArgs / RequestVoteReply implement the RequestVote RPC.
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

// AppendEntriesArgs / AppendEntriesReply implement the AppendEntries RPC.
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictIndex int // first index of the conflicting term (fast log repair)
	ConflictTerm  int // -1 when the follower needs the whole suffix
}

// Raft is one consensus replica. All mutable state is protected by mu.
type Raft struct {
	mu      sync.Mutex
	peers   []*labrpc.ClientEnd
	me      int
	applyCh chan ApplyMsg

	// State from the paper.
	currentTerm int
	votedFor    int
	log         []LogEntry

	// Volatile state on all servers.
	state       int
	commitIndex int
	lastApplied int

	// Volatile state on leaders.
	nextIndex  []int
	matchIndex []int

	// Election timing and shutdown.
	electionDeadline time.Time
	electionPending  bool
	random           *rand.Rand
	done             chan struct{}
	killed           bool

	applyCond *sync.Cond
}

func (rf *Raft) logf(format string, args ...interface{}) {
	if debug {
		log.Printf("peer %d [term %d]: %s", rf.me, rf.currentTerm, fmt.Sprintf(format, args...))
	}
}

// Make creates a Raft server. peers includes an endpoint for this server too;
// me is this server's index in peers, and applyCh receives committed entries.
func Make(peers []*labrpc.ClientEnd, me int, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		peers:      peers,
		me:         me,
		applyCh:    applyCh,
		votedFor:   -1,
		state:      follower,
		log:        []LogEntry{{Term: 0, Command: 0}}, // dummy entry at index 0
		nextIndex:  make([]int, len(peers)),
		matchIndex: make([]int, len(peers)),
		random:     rand.New(rand.NewSource(time.Now().UnixNano() + int64(me)*7919)),
		done:       make(chan struct{}),
	}
	rf.applyCond = sync.NewCond(&rf.mu)
	rf.resetElectionTimer()

	go rf.ticker()
	go rf.applier()
	go rf.heartbeatLoop()
	return rf
}

// Start begins replication of command if this peer is the leader. It returns
// the log index, the current term, and whether this peer is the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.killed || rf.state != leader {
		return -1, rf.currentTerm, false
	}
	rf.log = append(rf.log, LogEntry{Term: rf.currentTerm, Command: command})
	index := len(rf.log) - 1
	rf.logf("start command %v at index %d", command, index)

	// Replicate immediately instead of waiting for the next heartbeat.
	go rf.broadcastAppendEntries()
	return index, rf.currentTerm, true
}

// GetState returns the current term and whether this peer believes it is leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == leader
}

// Kill shuts down the Raft server's background goroutines.
func (rf *Raft) Kill() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if !rf.killed {
		rf.killed = true
		rf.applyCond.Broadcast()
		close(rf.done)
	}
}

// RequestVote is called by candidates asking for this peer's vote.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer func() { reply.Term = rf.currentTerm }()

	reply.VoteGranted = false
	if args.Term < rf.currentTerm {
		return
	}
	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term, -1)
	}
	// One vote per term, and the candidate's log must be at least as fresh.
	if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		return
	}
	if !rf.isUpToDate(args.LastLogTerm, args.LastLogIndex) {
		return
	}

	rf.votedFor = args.CandidateId
	rf.resetElectionTimer()
	reply.VoteGranted = true
	rf.logf("granted vote to %d for term %d", args.CandidateId, args.Term)
}

// AppendEntries is called by the leader for heartbeats and log replication.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Fill in the reply term on every return path, including after a higher
	// term has been adopted below.
	defer func() { reply.Term = rf.currentTerm }()

	reply.Success = false
	reply.ConflictIndex = -1
	reply.ConflictTerm = -1

	if args.Term < rf.currentTerm {
		return
	}
	if args.Term > rf.currentTerm || rf.state != follower {
		rf.becomeFollower(args.Term, -1)
	}
	// Any AppendEntries from the current leader resets the election timer.
	rf.resetElectionTimer()

	// Check the log entry immediately preceding the new entries.
	if args.PrevLogIndex < 0 || args.PrevLogIndex > len(rf.log)-1 {
		reply.ConflictIndex = len(rf.log)
		return
	}
	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		conflictTerm := rf.log[args.PrevLogIndex].Term
		firstIndex := args.PrevLogIndex
		for firstIndex > 0 && rf.log[firstIndex-1].Term == conflictTerm {
			firstIndex--
		}
		reply.ConflictTerm = conflictTerm
		reply.ConflictIndex = firstIndex
		return
	}

	reply.Success = true
	// Delete conflicting entries and append the leader's entries.
	for offset, entry := range args.Entries {
		index := args.PrevLogIndex + 1 + offset
		if index < len(rf.log) {
			if rf.log[index].Term == entry.Term {
				continue
			}
			rf.log = rf.log[:index]
		}
		rf.log = append(rf.log, args.Entries[offset:]...)
		break
	}

	// Advance the commit index using the leader's knowledge.
	if args.LeaderCommit > rf.commitIndex {
		newCommit := args.LeaderCommit
		if len(args.Entries) > 0 {
			lastNew := args.PrevLogIndex + len(args.Entries)
			if newCommit > lastNew {
				newCommit = lastNew
			}
		}
		if newCommit > len(rf.log)-1 {
			newCommit = len(rf.log) - 1
		}
		if newCommit > rf.commitIndex {
			rf.commitIndex = newCommit
			rf.applyCond.Broadcast()
		}
	}
}

// --- internal state transitions (callers hold mu) ---

func (rf *Raft) becomeFollower(term, votedFor int) {
	rf.state = follower
	rf.currentTerm = term
	rf.votedFor = votedFor
	rf.electionPending = false
	rf.resetElectionTimer()
}

func (rf *Raft) becomeCandidate() {
	rf.state = candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.resetElectionTimer()
}

func (rf *Raft) becomeLeader() {
	rf.state = leader
	for peer := range rf.peers {
		rf.nextIndex[peer] = len(rf.log)
		rf.matchIndex[peer] = 0
	}
	rf.logf("became leader")
}

func (rf *Raft) resetElectionTimer() {
	rf.electionDeadline = time.Now().Add(rf.randomTimeout(electionTimeoutMin, electionTimeoutMax))
}

func (rf *Raft) resetCandidateTimer() {
	rf.electionDeadline = time.Now().Add(rf.randomTimeout(candidateRetryMin, candidateRetryMax))
}

func (rf *Raft) randomTimeout(minimum, maximum time.Duration) time.Duration {
	return minimum + time.Duration(rf.random.Int63n(int64(maximum-minimum)))
}

func (rf *Raft) isUpToDate(lastLogTerm, lastLogIndex int) bool {
	ownTerm := rf.log[len(rf.log)-1].Term
	if lastLogTerm != ownTerm {
		return lastLogTerm > ownTerm
	}
	return lastLogIndex >= len(rf.log)-1
}

// advanceCommitIndex applies Raft's majority rule, including the restriction
// that only entries from the leader's current term can be committed by counting.
func (rf *Raft) advanceCommitIndex() {
	if rf.state != leader {
		return
	}
	for index := len(rf.log) - 1; index > rf.commitIndex; index-- {
		if rf.log[index].Term != rf.currentTerm {
			continue
		}
		votes := 1 // the leader itself
		for peer := range rf.peers {
			if peer == rf.me {
				continue
			}
			if rf.matchIndex[peer] >= index {
				votes++
			}
		}
		if votes > len(rf.peers)/2 {
			rf.commitIndex = index
			rf.applyCond.Broadcast()
			break
		}
	}
}

// --- background goroutines ---

func (rf *Raft) ticker() {
	ticker := time.NewTicker(tickerInterval)
	defer ticker.Stop()
	for {
		select {
		case <-rf.done:
			return
		case <-ticker.C:
		}

		rf.mu.Lock()
		if rf.killed {
			rf.mu.Unlock()
			return
		}
		expired := rf.state != leader && !rf.electionPending && time.Now().After(rf.electionDeadline)
		if expired {
			rf.electionPending = true
		}
		rf.mu.Unlock()
		if expired {
			go rf.startElection()
		}
	}
}

// applier sends committed entries to the service in log order. Channel sends
// happen without holding mu so they can never block RPC handlers.
func (rf *Raft) applier() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for {
		for rf.lastApplied >= rf.commitIndex && !rf.killed {
			rf.applyCond.Wait()
		}
		if rf.killed {
			return
		}

		last := rf.commitIndex
		if last > len(rf.log)-1 {
			last = len(rf.log) - 1
		}
		if last < rf.lastApplied {
			continue
		}
		first := rf.lastApplied + 1
		messages := make([]ApplyMsg, 0, last-first+1)
		for index := first; index <= last; index++ {
			messages = append(messages, ApplyMsg{
				CommandValid: true,
				Command:      rf.log[index].Command,
				CommandIndex: index,
			})
		}

		rf.mu.Unlock()
		for _, message := range messages {
			rf.applyCh <- message
		}
		rf.mu.Lock()
		rf.lastApplied = last
	}
}

func (rf *Raft) heartbeatLoop() {
	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-rf.done:
			return
		case <-ticker.C:
		}
		rf.broadcastAppendEntries()
	}
}

// --- election and replication ---

func (rf *Raft) startElection() {
	rf.mu.Lock()
	if rf.killed || rf.state == leader || !rf.electionPending {
		rf.mu.Unlock()
		return
	}
	rf.electionPending = false
	rf.becomeCandidate()
	// Give delayed vote replies time to arrive before retrying.
	rf.resetCandidateTimer()
	term := rf.currentTerm
	lastIndex := len(rf.log) - 1
	lastTerm := rf.log[lastIndex].Term
	votes := 1 // self
	rf.logf("starting election for term %d", term)
	rf.mu.Unlock()

	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		go func(peer int) {
			args := RequestVoteArgs{
				Term:         term,
				CandidateId:  rf.me,
				LastLogIndex: lastIndex,
				LastLogTerm:  lastTerm,
			}
			reply := RequestVoteReply{}
			ok := rf.sendRequestVote(peer, &args, &reply)

			rf.mu.Lock()
			defer rf.mu.Unlock()
			if !ok || rf.killed || rf.currentTerm != term || rf.state != candidate {
				return
			}
			if reply.Term > rf.currentTerm {
				rf.becomeFollower(reply.Term, -1)
				return
			}
			if !reply.VoteGranted {
				return
			}
			votes++
			if votes > len(rf.peers)/2 {
				rf.becomeLeader()
				rf.mu.Unlock()
				rf.broadcastAppendEntries() // establish authority immediately
				rf.mu.Lock()
			}
		}(peer)
	}
}

// broadcastAppendEntries asks every peer to be served by one replicateTo
// goroutine each. A failure only retries that one peer (see replicateTo), so
// a slow or partitioned follower cannot cause an RPC storm.
func (rf *Raft) broadcastAppendEntries() {
	rf.mu.Lock()
	if rf.killed || rf.state != leader {
		rf.mu.Unlock()
		return
	}
	peers := make([]int, 0, len(rf.peers)-1)
	for peer := range rf.peers {
		if peer != rf.me {
			peers = append(peers, peer)
		}
	}
	rf.mu.Unlock()

	for _, peer := range peers {
		go rf.replicateTo(peer)
	}
}

// replicateTo sends one AppendEntries RPC to one peer and processes the reply.
func (rf *Raft) replicateTo(peer int) {
	rf.mu.Lock()
	if rf.killed || rf.state != leader {
		rf.mu.Unlock()
		return
	}
	term := rf.currentTerm
	next := rf.nextIndex[peer]
	if next < 1 {
		next = 1
	}
	if next > len(rf.log) {
		next = len(rf.log)
	}
	rf.nextIndex[peer] = next

	entries := make([]LogEntry, len(rf.log)-next)
	copy(entries, rf.log[next:])
	args := AppendEntriesArgs{
		Term:         term,
		LeaderId:     rf.me,
		PrevLogIndex: next - 1,
		PrevLogTerm:  rf.log[next-1].Term,
		Entries:      entries,
		LeaderCommit: rf.commitIndex,
	}
	rf.mu.Unlock()

	reply := AppendEntriesReply{}
	ok := rf.sendAppendEntries(peer, &args, &reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if !ok || rf.killed || rf.state != leader || rf.currentTerm != term {
		return
	}
	if reply.Term > rf.currentTerm {
		rf.becomeFollower(reply.Term, -1)
		return
	}

	if reply.Success {
		// matchIndex is monotonic, so a delayed success reply from an older
		// attempt can never regress replication progress.
		match := args.PrevLogIndex + len(args.Entries)
		if match > rf.matchIndex[peer] {
			rf.matchIndex[peer] = match
			if rf.nextIndex[peer] < match+1 {
				rf.nextIndex[peer] = match + 1
			}
			rf.advanceCommitIndex()
		}
		return
	}

	// Ignore a rejection belonging to an already superseded nextIndex.
	if rf.nextIndex[peer] != args.PrevLogIndex+1 {
		return
	}

	// Log mismatch: use the follower's conflict information to skip whole
	// conflicting terms instead of decrementing nextIndex by one.
	target := 1
	if reply.ConflictTerm != -1 {
		found := -1
		for index := args.PrevLogIndex; index >= 1; index-- {
			if rf.log[index].Term == reply.ConflictTerm {
				found = index
				break
			}
		}
		if found != -1 {
			target = found + 1
		} else if reply.ConflictIndex > 0 {
			target = reply.ConflictIndex
		}
	} else if reply.ConflictIndex > 0 {
		target = reply.ConflictIndex
	}
	if target > len(rf.log) {
		target = len(rf.log)
	}
	if target < 1 {
		target = 1
	}
	if target != rf.nextIndex[peer] {
		rf.nextIndex[peer] = target
		go rf.replicateTo(peer)
	}
}

// sendRequestVote / sendAppendEntries are the low-level RPC wrappers. They
// connect the Raft state machine to the labrpc endpoints supplied to Make.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	return rf.peers[server].Call("Raft.RequestVote", args, reply)
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	return rf.peers[server].Call("Raft.AppendEntries", args, reply)
}
