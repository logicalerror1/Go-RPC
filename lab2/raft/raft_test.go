package raft

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"lab2/labrpc"
)

func endName(from, to int) string { return fmt.Sprintf("end-%d-%d", from, to) }
func serverName(peer int) string  { return fmt.Sprintf("raft-server-%d", peer) }

type testConfig struct {
	t        *testing.T
	n        int
	network  *labrpc.Network
	rafts    []*Raft
	applyChs []chan ApplyMsg
	mu       sync.Mutex
	applied  [][]ApplyMsg
}

func makeTestConfig(t *testing.T, n int) *testConfig {
	t.Helper()
	config := &testConfig{
		t:        t,
		n:        n,
		network:  labrpc.MakeNetwork(),
		rafts:    make([]*Raft, n),
		applyChs: make([]chan ApplyMsg, n),
		applied:  make([][]ApplyMsg, n),
	}

	peers := make([][]*labrpc.ClientEnd, n)
	for i := 0; i < n; i++ {
		peers[i] = make([]*labrpc.ClientEnd, n)
		for j := 0; j < n; j++ {
			peers[i][j] = config.network.MakeEnd(endName(i, j))
		}
	}
	for i := 0; i < n; i++ {
		config.applyChs[i] = make(chan ApplyMsg, 1024)
		service := labrpc.MakeServer()
		config.rafts[i] = Make(peers[i], i, config.applyChs[i])
		service.Register(config.rafts[i])
		config.network.AddServer(serverName(i), service)
	}
	for i := 0; i < n; i++ {
		for j := 0; j < n; j++ {
			if err := config.network.Connect(endName(i, j), serverName(j)); err != nil {
				t.Fatal(err)
			}
		}
	}

	for i := 0; i < n; i++ {
		go func(peer int) {
			for message := range config.applyChs[peer] {
				config.mu.Lock()
				config.applied[peer] = append(config.applied[peer], message)
				config.mu.Unlock()
			}
		}(i)
	}

	t.Cleanup(func() {
		for _, raft := range config.rafts {
			raft.Kill()
		}
		config.network.Cleanup()
	})
	return config
}

func (config *testConfig) disconnect(from, to int) { config.network.Enable(endName(from, to), false) }
func (config *testConfig) connect(from, to int)    { config.network.Enable(endName(from, to), true) }

func (config *testConfig) isolate(peer int) {
	for other := 0; other < config.n; other++ {
		if other != peer {
			config.disconnect(peer, other)
			config.disconnect(other, peer)
		}
	}
}

func (config *testConfig) rejoin(peer int) {
	for other := 0; other < config.n; other++ {
		if other != peer {
			config.connect(peer, other)
			config.connect(other, peer)
		}
	}
}

func (config *testConfig) leaders() []int {
	leaders := make([]int, 0)
	for peer, raft := range config.rafts {
		if _, isLeader := raft.GetState(); isLeader {
			leaders = append(leaders, peer)
		}
	}
	return leaders
}

func (config *testConfig) singleLeader(timeout time.Duration) int {
	config.t.Helper()
	var found int
	waitFor(config.t, "a single leader", timeout, func() bool {
		leaders := config.leaders()
		if len(leaders) != 1 {
			return false
		}
		found = leaders[0]
		return true
	})
	return found
}

func (config *testConfig) singleLeaderAmong(peers []int, timeout time.Duration) int {
	config.t.Helper()
	var found int
	waitFor(config.t, "a single leader among the connected peers", timeout, func() bool {
		count := 0
		for _, leader := range config.leaders() {
			for _, peer := range peers {
				if leader == peer {
					count++
					found = leader
				}
			}
		}
		return count == 1
	})
	return found
}

func (config *testConfig) termsAgree() bool {
	term, _ := config.rafts[0].GetState()
	for _, raft := range config.rafts[1:] {
		other, _ := raft.GetState()
		if other != term {
			return false
		}
	}
	return true
}

func (config *testConfig) appliedSnapshot(peer int) []ApplyMsg {
	config.mu.Lock()
	defer config.mu.Unlock()
	result := make([]ApplyMsg, len(config.applied[peer]))
	copy(result, config.applied[peer])
	return result
}

func waitFor(t *testing.T, what string, timeout time.Duration, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func commandsOf(messages []ApplyMsg) []int {
	commands := make([]int, len(messages))
	for index, message := range messages {
		commands[index] = message.Command.(int)
		if message.CommandIndex != index+1 {
			panic("applied entries are out of order")
		}
	}
	return commands
}

func equalCommands(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// --- 3A: leader election and heartbeats ---

func Test3AInitialElection(t *testing.T) {
	config := makeTestConfig(t, 3)
	leader := config.singleLeader(5 * time.Second)
	if !config.termsAgree() {
		t.Fatal("terms disagree after election")
	}

	// A healthy leader's heartbeats must prevent further elections.
	time.Sleep(600 * time.Millisecond)
	leaders := config.leaders()
	if len(leaders) != 1 || leaders[0] != leader {
		t.Fatalf("leadership changed while healthy: got %v, want [%d]", leaders, leader)
	}
	if !config.termsAgree() {
		t.Fatal("terms disagree after heartbeats")
	}
}

func Test3AReElection(t *testing.T) {
	config := makeTestConfig(t, 5)
	oldLeader := config.singleLeader(5 * time.Second)
	config.isolate(oldLeader)

	connected := make([]int, 0, config.n-1)
	for peer := 0; peer < config.n; peer++ {
		if peer != oldLeader {
			connected = append(connected, peer)
		}
	}
	newLeader := config.singleLeaderAmong(connected, 5*time.Second)
	if newLeader == oldLeader {
		t.Fatal("isolated leader was re-elected")
	}

	config.rejoin(oldLeader)
	waitFor(t, "old leader to step down", 5*time.Second, func() bool {
		term, isLeader := config.rafts[oldLeader].GetState()
		currentTerm, _ := config.rafts[newLeader].GetState()
		return term == currentTerm && !isLeader
	})
}

// --- 3B: replicated log ---

func Test3BBasicAgreement(t *testing.T) {
	config := makeTestConfig(t, 3)
	leader := config.singleLeader(5 * time.Second)

	want := []int{}
	for command := 1; command <= 6; command++ {
		index, term, ok := config.rafts[leader].Start(command)
		if !ok || term <= 0 || index <= 0 {
			t.Fatalf("Start(%d) = (%d, %d, %v)", command, index, term, ok)
		}
		want = append(want, command)
	}

	waitFor(t, "all peers to apply every command", 5*time.Second, func() bool {
		for peer := 0; peer < config.n; peer++ {
			if !equalCommands(commandsOf(config.appliedSnapshot(peer)), want) {
				return false
			}
		}
		return true
	})
}

func Test3BLogRepair(t *testing.T) {
	config := makeTestConfig(t, 5)
	leader := config.singleLeader(5 * time.Second)
	follower := (leader + 1) % config.n
	config.isolate(follower)

	want := []int{}
	for command := 10; command < 15; command++ {
		if _, _, ok := config.rafts[leader].Start(command); !ok {
			t.Fatalf("leader %d lost leadership during Start", leader)
		}
		want = append(want, command)
	}

	connected := make([]int, 0, config.n-1)
	for peer := 0; peer < config.n; peer++ {
		if peer != follower {
			connected = append(connected, peer)
		}
	}
	waitFor(t, "the connected majority to commit", 5*time.Second, func() bool {
		for _, peer := range connected {
			if !equalCommands(commandsOf(config.appliedSnapshot(peer)), want) {
				return false
			}
		}
		return true
	})

	config.rejoin(follower)
	waitFor(t, "the recovering follower to catch up", 5*time.Second, func() bool {
		return equalCommands(commandsOf(config.appliedSnapshot(follower)), want)
	})
}

func Test3BNewLeaderCommitsOldEntries(t *testing.T) {
	config := makeTestConfig(t, 5)
	oldLeader := config.singleLeader(5 * time.Second)

	want := []int{}
	for command := 100; command < 104; command++ {
		if _, _, ok := config.rafts[oldLeader].Start(command); !ok {
			t.Fatal("leader lost leadership during Start")
		}
		want = append(want, command)
	}
	waitFor(t, "initial commitment", 5*time.Second, func() bool {
		for peer := 0; peer < config.n; peer++ {
			if !equalCommands(commandsOf(config.appliedSnapshot(peer)), want) {
				return false
			}
		}
		return true
	})

	config.isolate(oldLeader)
	connected := make([]int, 0, config.n-1)
	for peer := 0; peer < config.n; peer++ {
		if peer != oldLeader {
			connected = append(connected, peer)
		}
	}
	newLeader := config.singleLeaderAmong(connected, 5*time.Second)

	if _, _, ok := config.rafts[newLeader].Start(999); !ok {
		t.Fatal("new leader refused Start")
	}
	want = append(want, 999)
	waitFor(t, "the new leader's term entry to commit", 5*time.Second, func() bool {
		for _, peer := range connected {
			if !equalCommands(commandsOf(config.appliedSnapshot(peer)), want) {
				return false
			}
		}
		return true
	})
}

func Test3BConcurrentStart(t *testing.T) {
	config := makeTestConfig(t, 3)
	leader := config.singleLeader(5 * time.Second)

	var wait sync.WaitGroup
	for command := 0; command < 12; command++ {
		wait.Add(1)
		go func(value int) {
			defer wait.Done()
			config.rafts[leader].Start(value)
		}(command)
	}
	wait.Wait()

	waitFor(t, "concurrent commands to be applied in the same order everywhere", 5*time.Second, func() bool {
		reference := commandsOf(config.appliedSnapshot(0))
		if len(reference) != 12 {
			return false
		}
		seen := make(map[int]bool)
		for _, command := range reference {
			seen[command] = true
		}
		if len(seen) != 12 {
			return false
		}
		for peer := 1; peer < config.n; peer++ {
			if !equalCommands(commandsOf(config.appliedSnapshot(peer)), reference) {
				return false
			}
		}
		return true
	})
}

func Test3BUnreliableNetwork(t *testing.T) {
	config := makeTestConfig(t, 5)
	config.network.Reliable(false)
	leader := config.singleLeader(10 * time.Second)
	// Reorder replies only after a leader exists, as real test harnesses do:
	// during an election, multi-second reply delays make voting impractical.
	config.network.LongReordering(true)

	want := []int{}
	for command := 1; command <= 5; command++ {
		for {
			if _, _, ok := config.rafts[leader].Start(command); ok {
				break
			}
			leader = config.singleLeader(10 * time.Second)
		}
		want = append(want, command)
	}

	deadline := time.Now().Add(25 * time.Second)
	for time.Now().Before(deadline) {
		reference := commandsOf(config.appliedSnapshot(0))
		if len(reference) > 0 {
			allEqual := true
			for peer := 1; peer < config.n; peer++ {
				if !equalCommands(commandsOf(config.appliedSnapshot(peer)), reference) {
					allEqual = false
				}
			}
			if allEqual {
				return
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	for peer := 0; peer < config.n; peer++ {
		term, isLeader := config.rafts[peer].GetState()
		t.Logf("peer %d: term=%d leader=%v applied=%v", peer, term, isLeader, commandsOf(config.appliedSnapshot(peer)))
	}
	t.Fatalf("did not converge; started %v", want)
}
