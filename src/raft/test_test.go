package raft

//
// Raft tests.
//
// we will use the original test_test.go to test your code for grading.
// so, while you can modify this code to help you debug, please
// test with the original before submitting.
//

import (
	"bytes"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"6.824-2022/labgob"
)

// The tester generously allows solutions to complete elections in one second
// (much more than the paper's range of timeouts).
const RaftElectionTimeout = 1000 * time.Millisecond

func TestInitialElection2A(t *testing.T) {
	servers := 3
	cfg := make_config(t, servers, false, false)
	defer cfg.cleanup()

	cfg.begin("Test (2A): initial election")

	// is a leader elected?
	cfg.checkOneLeader()

	// sleep a bit to avoid racing with followers learning of the
	// election, then check that all peers agree on the term.
	time.Sleep(50 * time.Millisecond)
	term1 := cfg.checkTerms()
	if term1 < 1 {
		t.Fatalf("term is %v, but should be at least 1", term1)
	}

	// does the leader+term stay the same if there is no network failure?
	time.Sleep(2 * RaftElectionTimeout)
	term2 := cfg.checkTerms()
	if term1 != term2 {
		fmt.Printf("warning: term changed even though there were no failures")
	}

	// there should still be a leader.
	cfg.checkOneLeader()

	cfg.end()
}

func TestReElection2A(t *testing.T) {
	servers := 3
	cfg := make_config(t, servers, false, false)
	defer cfg.cleanup()

	cfg.begin("Test (2A): election after network failure")

	leader1 := cfg.checkOneLeader()

	// if the leader disconnects, a new one should be elected.
	cfg.t.Logf("============= disconnecting %v", leader1)
	cfg.disconnect(leader1)
	cfg.t.Logf("============= disconnected %v", leader1)
	cfg.checkOneLeader()
	cfg.t.Log("============= finished checking one leader")

	// if the old leader rejoins, that shouldn't
	// disturb the new leader. and the old leader
	// should switch to follower.

	cfg.t.Logf("============= reconnecting %v", leader1)
	cfg.connect(leader1)
	cfg.t.Logf("============= reconnected %v", leader1)
	leader2 := cfg.checkOneLeader()
	cfg.t.Log("============= finished checking one leader")

	// if there's no quorum, no new leader should
	// be elected.
	cfg.t.Logf("============= disconnecting %v", leader2)
	cfg.disconnect(leader2)
	cfg.t.Logf("============= disconnecting %v", (leader2+1)%servers)
	cfg.disconnect((leader2 + 1) % servers)
	cfg.t.Logf("============= sleeping")
	time.Sleep(1 * RaftElectionTimeout)
	cfg.t.Logf("============= woke up")

	// check that the one connected server
	// does not think it is the leader.
	cfg.checkNoLeader()
	cfg.t.Logf("============= checked no leader")

	// if a quorum arises, it should elect a leader.
	cfg.t.Logf("============= connecting %v", (leader2+1)%servers)
	cfg.connect((leader2 + 1) % servers)
	time.Sleep(2 * RaftElectionTimeout)
	cfg.checkOneLeader()
	cfg.t.Logf("============= checked one leader")

	// re-join of last node shouldn't prevent leader from existing.
	cfg.t.Logf("============= connecting %v", leader2)
	cfg.connect(leader2)
	cfg.t.Logf("============= connected %v", leader2)
	cfg.checkOneLeader()
	cfg.t.Logf("============= checked one leader")

	cfg.end()
}

func TestManyElections2A(t *testing.T) {
	servers := 7
	cfg := make_config(t, servers, false, false)
	defer cfg.cleanup()

	cfg.begin("Test (2A): multiple elections")

	cfg.checkOneLeader()

	iters := 10
	for ii := 1; ii < iters; ii++ {
		// disconnect three nodes
		i1 := rand.Int() % servers
		i2 := rand.Int() % servers
		i3 := rand.Int() % servers
		cfg.disconnect(i1)
		cfg.disconnect(i2)
		cfg.disconnect(i3)

		// either the current leader should still be alive,
		// or the remaining four should elect a new one.
		cfg.checkOneLeader()

		cfg.connect(i1)
		cfg.connect(i2)
		cfg.connect(i3)
	}

	cfg.checkOneLeader()

	cfg.end()
}

func TestBasicAgree2B(t *testing.T) {
	servers := 3
	cfg := make_config(t, servers, false, false)
	defer cfg.cleanup()

	cfg.begin("Test (2B): basic agreement")

	iters := 3
	for index := 1; index < iters+1; index++ {
		nd, _ := cfg.nCommitted(index)
		if nd > 0 {
			t.Fatalf("some have committed before Start()")
		}

		xindex := cfg.one(index*100, servers, false)
		if xindex != index {
			t.Fatalf("got index %v but expected %v", xindex, index)
		}
	}

	cfg.end()
}

// check, based on counting bytes of RPCs, that
// each command is sent to each peer just once.
func TestRPCBytes2B(t *testing.T) {
	servers := 3
	cfg := make_config(t, servers, false, false)
	defer cfg.cleanup()

	cfg.begin("Test (2B): RPC byte count")

	cfg.one(99, servers, false)
	bytes0 := cfg.bytesTotal()

	iters := 10
	var sent int64 = 0
	for index := 2; index < iters+2; index++ {
		cmd := randstring(5000)
		xindex := cfg.one(cmd, servers, false)
		if xindex != index {
			t.Fatalf("got index %v but expected %v", xindex, index)
		}
		sent += int64(len(cmd))
		cfg.t.Log("Bytes total: ", cfg.bytesTotal())
	}

	bytes1 := cfg.bytesTotal()
	got := bytes1 - bytes0
	expected := int64(servers) * sent
	if got > expected+50000 {
		t.Fatalf("too many RPC bytes; got %v, expected %v", got, expected)
	}

	cfg.end()
}

// test just failure of followers.
func For2023TestFollowerFailure2B(t *testing.T) {
	servers := 3
	cfg := make_config(t, servers, false, false)
	defer cfg.cleanup()

	cfg.begin("Test (2B): test progressive failure of followers")

	cfg.one(101, servers, false)

	// disconnect one follower from the network.
	leader1 := cfg.checkOneLeader()
	cfg.disconnect((leader1 + 1) % servers)

	// the leader and remaining follower should be
	// able to agree despite the disconnected follower.
	cfg.one(102, servers-1, false)
	time.Sleep(RaftElectionTimeout)
	cfg.one(103, servers-1, false)

	// disconnect the remaining follower
	leader2 := cfg.checkOneLeader()
	cfg.disconnect((leader2 + 1) % servers)
	cfg.disconnect((leader2 + 2) % servers)

	// submit a command.
	index, _, ok := cfg.rafts[leader2].Start(104)
	if ok != true {
		t.Fatalf("leader rejected Start()")
	}
	if index != 4 {
		t.Fatalf("expected index 4, got %v", index)
	}

	time.Sleep(2 * RaftElectionTimeout)

	// check that command 104 did not commit.
	n, _ := cfg.nCommitted(index)
	if n > 0 {
		t.Fatalf("%v committed but no majority", n)
	}

	cfg.end()
}

// test just failure of leaders.
func For2023TestLeaderFailure2B(t *testing.T) {
	servers := 3
	cfg := make_config(t, servers, false, false)
	defer cfg.cleanup()

	cfg.begin("Test (2B): test failure of leaders")

	cfg.one(101, servers, false)

	// disconnect the first leader.
	leader1 := cfg.checkOneLeader()
	cfg.disconnect(leader1)

	// the remaining followers should elect
	// a new leader.
	cfg.one(102, servers-1, false)
	time.Sleep(RaftElectionTimeout)
	cfg.one(103, servers-1, false)

	// disconnect the new leader.
	leader2 := cfg.checkOneLeader()
	cfg.disconnect(leader2)

	// submit a command to each server.
	for i := 0; i < servers; i++ {
		cfg.rafts[i].Start(104)
	}

	time.Sleep(2 * RaftElectionTimeout)

	// check that command 104 did not commit.
	n, _ := cfg.nCommitted(4)
	if n > 0 {
		t.Fatalf("%v committed but no majority", n)
	}

	cfg.end()
}

// test that a follower participates after
// disconnect and re-connect.
func TestFailAgree2B(t *testing.T) {
	servers := 3
	cfg := make_config(t, servers, false, false)
	defer cfg.cleanup()

	cfg.begin("Test (2B): agreement after follower reconnects")

	cfg.one(101, servers, false)

	// disconnect one follower from the network.
	leader := cfg.checkOneLeader()
	cfg.disconnect((leader + 1) % servers)

	// the leader and remaining follower should be
	// able to agree despite the disconnected follower.
	cfg.one(102, servers-1, false)
	cfg.one(103, servers-1, false)
	time.Sleep(RaftElectionTimeout)
	cfg.one(104, servers-1, false)
	cfg.one(105, servers-1, false)

	// re-connect
	cfg.connect((leader + 1) % servers)

	// the full set of servers should preserve
	// previous agreements, and be able to agree
	// on new commands.
	cfg.one(106, servers, true)
	time.Sleep(RaftElectionTimeout)
	cfg.one(107, servers, true)

	cfg.end()
}

func TestFailNoAgree2B(t *testing.T) {
	servers := 5
	cfg := make_config(t, servers, false, false)
	defer cfg.cleanup()

	cfg.begin("Test (2B): no agreement if too many followers disconnect")

	cfg.one(10, servers, false)

	// 3 of 5 followers disconnect
	leader := cfg.checkOneLeader()
	cfg.disconnect((leader + 1) % servers)
	cfg.disconnect((leader + 2) % servers)
	cfg.disconnect((leader + 3) % servers)

	index, _, ok := cfg.rafts[leader].Start(20)
	if ok != true {
		t.Fatalf("leader rejected Start()")
	}
	if index != 2 {
		t.Fatalf("expected index 2, got %v", index)
	}

	time.Sleep(2 * RaftElectionTimeout)

	n, _ := cfg.nCommitted(index)
	if n > 0 {
		t.Fatalf("%v committed but no majority", n)
	}

	// repair
	cfg.connect((leader + 1) % servers)
	cfg.connect((leader + 2) % servers)
	cfg.connect((leader + 3) % servers)

	// the disconnected majority may have chosen a leader from
	// among their own ranks, forgetting index 2.
	leader2 := cfg.checkOneLeader()
	index2, _, ok2 := cfg.rafts[leader2].Start(30)
	if ok2 == false {
		t.Fatalf("leader2 rejected Start()")
	}
	if index2 < 2 || index2 > 3 {
		t.Fatalf("unexpected index %v", index2)
	}

	cfg.one(1000, servers, true)

	cfg.end()
}

func TestConcurrentStarts2B(t *testing.T) {
	servers := 3
	cfg := make_config(t, servers, false, false)
	defer cfg.cleanup()

	cfg.begin("Test (2B): concurrent Start()s")

	var success bool
loop:
	for try := 0; try < 5; try++ {
		if try > 0 {
			// give solution some time to settle
			time.Sleep(3 * time.Second)
		}
		t.Log("===================Checking one leader")
		leader := cfg.checkOneLeader()
		t.Log("===================Found one leader", leader)
		t.Log("===================Committing entry {1}")
		_, term, ok := cfg.rafts[leader].Start(1)
		t.Log("===================Committed entry {1}", term, ok)
		if !ok {
			// leader moved on really quickly
			continue
		}

		iters := 5
		var wg sync.WaitGroup
		is := make(chan int, iters)
		for ii := 0; ii < iters; ii++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				t.Logf("===================Committing entry {%v}", 100+i)
				i, term1, ok := cfg.rafts[leader].Start(100 + i)
				t.Logf("===================Committed entry {%v} %v %v", 100+i, term1, ok)
				t.Log("===================Checking equal terms")
				if term1 != term {
					return
				}
				t.Log("===================Checked equal terms")
				if ok != true {
					return
				}
				is <- i
			}(ii)
		}

		wg.Wait()
		close(is)

		for j := 0; j < servers; j++ {
			if t, _ := cfg.rafts[j].GetState(); int(t) != term {
				// term changed -- can't expect low RPC counts
				continue loop
			}
		}

		failed := false
		cmds := []int{}
		for index := range is {
			cmd := cfg.wait(index, servers, term)
			if ix, ok := cmd.(int); ok {
				if ix == -1 {
					// peers have moved on to later terms
					// so we can't expect all Start()s to
					// have succeeded
					failed = true
					break
				}
				cmds = append(cmds, ix)
			} else {
				t.Fatalf("value %v is not an int", cmd)
			}
		}

		if failed {
			// avoid leaking goroutines
			go func() {
				for range is {
				}
			}()
			continue
		}

		for ii := 0; ii < iters; ii++ {
			x := 100 + ii
			ok := false
			for j := 0; j < len(cmds); j++ {
				if cmds[j] == x {
					ok = true
				}
			}
			if ok == false {
				t.Fatalf("cmd %v missing in %v", x, cmds)
			}
		}

		success = true
		break
	}

	if !success {
		t.Fatalf("term changed too often")
	}

	cfg.end()
}

func TestRejoin2B(t *testing.T) {
	servers := 3
	cfg := make_config(t, servers, false, false)
	defer cfg.cleanup()

	cfg.begin("Test (2B): rejoin of partitioned leader")

	t.Log("=====================Committing Entry 101 to all servers")
	cfg.one(101, servers, true)
	t.Log("=====================Committed Entry 101 to all servers")

	// leader network failure
	t.Log("=====================Checking One Leader")
	leader1 := cfg.checkOneLeader()
	t.Log("=====================Checked One Leader -->", leader1)
	t.Log("=====================Disconnecting Leader", leader1)
	cfg.disconnect(leader1)
	t.Log("=====================Disconnected Leader", leader1)

	// make old leader try to agree on some entries
	t.Log("=====================Adding Entries", 102, 103, 104, " to isolated", leader1)
	cfg.rafts[leader1].Start(102)
	cfg.rafts[leader1].Start(103)
	cfg.rafts[leader1].Start(104)
	t.Log("=====================Added Entries", 102, 103, 104, " to isolated", leader1)

	// new leader commits, also for index=2
	t.Log("=====================Committing 103 to the remaining two nodes")
	cfg.one(103, 2, true)
	t.Log("=====================Committed 103 to the remaining two nodes")

	// new leader network failure
	t.Log("=====================Checking New (Second) Leader")
	leader2 := cfg.checkOneLeader()
	t.Log("=====================Checked New (Second) Leader", leader2)
	t.Log("=====================Disconnecting new Leader", leader2)
	cfg.disconnect(leader2)
	t.Log("=====================Disconnected new Leader", leader2)

	// old leader connected again
	t.Log("=====================Connecting Old Leader", leader1)
	cfg.connect(leader1)
	t.Log("=====================Connected Old Leader", leader1)

	t.Log("=====================Committing 104 to", leader1, "and the remaining node")
	cfg.one(104, 2, true)
	t.Log("=====================Committed 104 to", leader1, "and the remaining node")

	t.Log("=====================Reconnecting Second Leader", leader2)
	// all together now
	cfg.connect(leader2)
	t.Log("=====================Reconnected Second Leader", leader2)
	t.Log("=====================New Leader commiting an entry", 105)
	cfg.one(105, servers, true)
	t.Log("=====================New Leader committed an entry", 105)

	cfg.end()
}

func TestBackup2B(t *testing.T) {
	servers := 5
	cfg := make_config(t, servers, false, false)
	defer cfg.cleanup()

	cfg.begin("Test (2B): leader backs up quickly over incorrect follower logs")

	cfg.one(rand.Int(), servers, true)

	// put leader and one follower in a partition
	leader1 := cfg.checkOneLeader()
	cfg.disconnect((leader1 + 2) % servers)
	cfg.disconnect((leader1 + 3) % servers)
	cfg.disconnect((leader1 + 4) % servers)

	// submit lots of commands that won't commit
	for i := 0; i < 50; i++ {
		cfg.rafts[leader1].Start(rand.Int())
	}

	time.Sleep(RaftElectionTimeout / 2)

	cfg.disconnect((leader1 + 0) % servers)
	cfg.disconnect((leader1 + 1) % servers)

	// allow other partition to recover
	cfg.connect((leader1 + 2) % servers)
	cfg.connect((leader1 + 3) % servers)
	cfg.connect((leader1 + 4) % servers)

	// lots of successful commands to new group.
	for i := 0; i < 50; i++ {
		cfg.one(rand.Int(), 3, true)
	}

	// now another partitioned leader and one follower
	leader2 := cfg.checkOneLeader()
	other := (leader1 + 2) % servers
	if leader2 == other {
		other = (leader2 + 1) % servers
	}
	cfg.disconnect(other)

	// lots more commands that won't commit
	for i := 0; i < 50; i++ {
		cfg.rafts[leader2].Start(rand.Int())
	}

	time.Sleep(RaftElectionTimeout / 2)

	// bring original leader back to life,
	for i := 0; i < servers; i++ {
		cfg.disconnect(i)
	}
	cfg.connect((leader1 + 0) % servers)
	cfg.connect((leader1 + 1) % servers)
	cfg.connect(other)

	// lots of successful commands to new group.
	for i := 0; i < 50; i++ {
		cfg.one(rand.Int(), 3, true)
	}

	// now everyone
	for i := 0; i < servers; i++ {
		cfg.connect(i)
	}
	cfg.one(rand.Int(), servers, true)

	cfg.end()
}

func TestCount2B(t *testing.T) {
	servers := 3
	cfg := make_config(t, servers, false, false)
	defer cfg.cleanup()

	cfg.begin("Test (2B): RPC counts aren't too high")

	rpcs := func() (n int) {
		for j := 0; j < servers; j++ {
			n += cfg.rpcCount(j)
		}
		return
	}

	_ = cfg.checkOneLeader()

	total1 := rpcs()

	if total1 > 30 || total1 < 1 {
		t.Fatalf("too many or few RPCs (%v) to elect initial leader\n", total1)
	}

	var total2 int
	var success bool
	var leader int
loop:
	for try := 0; try < 5; try++ {
		if try > 0 {
			// give solution some time to settle
			time.Sleep(3 * time.Second)
		}

		leader = cfg.checkOneLeader()
		total1 = rpcs()

		iters := 10
		starti, term, ok := cfg.rafts[leader].Start(1)
		if !ok {
			// leader moved on really quickly
			continue
		}
		cmds := []int{}
		for i := 1; i < iters+2; i++ {
			x := int(rand.Int31())
			cmds = append(cmds, x)
			index1, term1, ok := cfg.rafts[leader].Start(x)
			if term1 != term {
				// Term changed while starting
				continue loop
			}
			if !ok {
				// No longer the leader, so term has changed
				continue loop
			}
			if starti+i != index1 {
				t.Fatalf("Start() failed")
			}
		}

		for i := 1; i < iters+1; i++ {
			cmd := cfg.wait(starti+i, servers, term)
			if ix, ok := cmd.(int); ok == false || ix != cmds[i-1] {
				if ix == -1 {
					// term changed -- try again
					continue loop
				}
				t.Fatalf("wrong value %v committed for index %v; expected %v\n", cmd, starti+i, cmds)
			}
		}

		failed := false
		total2 = 0
		for j := 0; j < servers; j++ {
			if t, _ := cfg.rafts[j].GetState(); int(t) != term {
				// term changed -- can't expect low RPC counts
				// need to keep going to update total2
				failed = true
			}
			total2 += cfg.rpcCount(j)
		}

		if failed {
			continue loop
		}

		if total2-total1 > (iters+1+3)*3 {
			t.Fatalf("too many RPCs (%v) for %v entries\n", total2-total1, iters)
		}

		success = true
		break
	}

	if !success {
		t.Fatalf("term changed too often")
	}

	time.Sleep(RaftElectionTimeout)

	total3 := 0
	for j := 0; j < servers; j++ {
		total3 += cfg.rpcCount(j)
	}

	if total3-total2 > 3*20 {
		t.Fatalf("too many RPCs (%v) for 1 second of idleness\n", total3-total2)
	}

	cfg.end()
}

func TestPersist12C(t *testing.T) {
	servers := 3
	cfg := make_config(t, servers, false, false)
	defer cfg.cleanup()

	cfg.begin("Test (2C): basic persistence")

	cfg.one(11, servers, true)

	// crash and re-start all
	for i := 0; i < servers; i++ {
		cfg.start1(i, cfg.applier)
	}
	for i := 0; i < servers; i++ {
		cfg.disconnect(i)
		cfg.connect(i)
	}

	cfg.one(12, servers, true)

	leader1 := cfg.checkOneLeader()
	cfg.disconnect(leader1)
	cfg.start1(leader1, cfg.applier)
	cfg.connect(leader1)

	cfg.one(13, servers, true)

	leader2 := cfg.checkOneLeader()
	cfg.disconnect(leader2)
	cfg.one(14, servers-1, true)
	cfg.start1(leader2, cfg.applier)
	cfg.connect(leader2)

	cfg.wait(4, servers, -1) // wait for leader2 to join before killing i3

	i3 := (cfg.checkOneLeader() + 1) % servers
	cfg.disconnect(i3)
	cfg.one(15, servers-1, true)
	cfg.start1(i3, cfg.applier)
	cfg.connect(i3)

	cfg.one(16, servers, true)

	cfg.end()
}

func TestPersist22C(t *testing.T) {
	servers := 5
	cfg := make_config(t, servers, false, false)
	defer cfg.cleanup()

	cfg.begin("Test (2C): more persistence")

	index := 1
	for iters := 0; iters < 5; iters++ {
		cfg.one(10+index, servers, true)
		index++

		leader1 := cfg.checkOneLeader()

		cfg.disconnect((leader1 + 1) % servers)
		cfg.disconnect((leader1 + 2) % servers)

		cfg.one(10+index, servers-2, true)
		index++

		cfg.disconnect((leader1 + 0) % servers)
		cfg.disconnect((leader1 + 3) % servers)
		cfg.disconnect((leader1 + 4) % servers)

		cfg.start1((leader1+1)%servers, cfg.applier)
		cfg.start1((leader1+2)%servers, cfg.applier)
		cfg.connect((leader1 + 1) % servers)
		cfg.connect((leader1 + 2) % servers)

		time.Sleep(RaftElectionTimeout)

		cfg.start1((leader1+3)%servers, cfg.applier)
		cfg.connect((leader1 + 3) % servers)

		cfg.one(10+index, servers-2, true)
		index++

		cfg.connect((leader1 + 4) % servers)
		cfg.connect((leader1 + 0) % servers)
	}

	cfg.one(1000, servers, true)

	cfg.end()
}

func TestPersist32C(t *testing.T) {
	servers := 3
	cfg := make_config(t, servers, false, false)
	defer cfg.cleanup()

	cfg.begin("Test (2C): partitioned leader and one follower crash, leader restarts")

	cfg.one(101, 3, true)

	leader := cfg.checkOneLeader()
	cfg.disconnect((leader + 2) % servers)

	cfg.one(102, 2, true)

	cfg.crash1((leader + 0) % servers)
	cfg.crash1((leader + 1) % servers)
	cfg.connect((leader + 2) % servers)
	cfg.start1((leader+0)%servers, cfg.applier)
	cfg.connect((leader + 0) % servers)

	cfg.one(103, 2, true)

	cfg.start1((leader+1)%servers, cfg.applier)
	cfg.connect((leader + 1) % servers)

	cfg.one(104, servers, true)

	cfg.end()
}

// Test the scenarios described in Figure 8 of the extended Raft paper. Each
// iteration asks a leader, if there is one, to insert a command in the Raft
// log.  If there is a leader, that leader will fail quickly with a high
// probability (perhaps without committing the command), or crash after a while
// with low probability (most likey committing the command).  If the number of
// alive servers isn't enough to form a majority, perhaps start a new server.
// The leader in a new term may try to finish replicating log entries that
// haven't been committed yet.
func TestFigure82C(t *testing.T) {
	servers := 5
	cfg := make_config(t, servers, false, false)
	defer cfg.cleanup()

	cfg.begin("Test (2C): Figure 8")

	cfg.one(rand.Int(), 1, true)

	nup := servers
	for iters := 0; iters < 1000; iters++ {
		t.Log("Iteration", iters)
		leader := -1
		for i := 0; i < servers; i++ {
			if cfg.rafts[i] != nil {
				_, _, ok := cfg.rafts[i].Start(rand.Int())
				if ok {
					leader = i
					t.Log("Leader is", i)
				}
			}
		}

		if (rand.Int() % 1000) < 100 {
			ms := rand.Int63() % (int64(RaftElectionTimeout/time.Millisecond) / 2)
			t.Log("Sleep1", ms)
			time.Sleep(time.Duration(ms) * time.Millisecond)
		} else {
			ms := (rand.Int63() % 13)
			t.Log("Sleep2", ms)
			time.Sleep(time.Duration(ms) * time.Millisecond)
		}

		if leader != -1 {
			t.Log("Crashing", leader)
			cfg.crash1(leader)
			nup -= 1
		}

		if nup < 3 {
			s := rand.Int() % servers
			if cfg.rafts[s] == nil {
				t.Log("1 start1", s)
				cfg.start1(s, cfg.applier)
				t.Log("1 connect", s)
				cfg.connect(s)
				nup += 1
			}
		}
	}

	for i := 0; i < servers; i++ {
		if cfg.rafts[i] == nil {
			t.Log("2 start1", i)
			cfg.start1(i, cfg.applier)
			t.Log("2 connect", i)
			cfg.connect(i)
		}
	}
	cmd := rand.Int()
	t.Log("Agreeing on", cmd)
	cfg.one(cmd, servers, true)
	t.Log("Finshed agreeing on", cmd)

	cfg.end()
}

func TestUnreliableAgree2C(t *testing.T) {
	servers := 5
	cfg := make_config(t, servers, true, false)
	defer cfg.cleanup()

	cfg.begin("Test (2C): unreliable agreement")

	var wg sync.WaitGroup

	for iters := 1; iters < 50; iters++ {
		for j := 0; j < 4; j++ {
			wg.Add(1)
			go func(iters, j int) {
				defer wg.Done()
				cfg.one((100*iters)+j, 1, true)
			}(iters, j)
		}
		cfg.one(iters, 1, true)
	}

	cfg.setunreliable(false)

	wg.Wait()

	cfg.one(100, servers, true)

	cfg.end()
}

func TestFigure8Unreliable2C(t *testing.T) {
	servers := 5
	cfg := make_config(t, servers, true, false)
	defer cfg.cleanup()

	cfg.begin("Test (2C): Figure 8 (unreliable)")

	cfg.one(rand.Int()%10000, 1, true)

	nup := servers
	for iters := 0; iters < 1000; iters++ {
		if iters == 200 {
			cfg.setlongreordering(true)
		}
		leader := -1
		for i := 0; i < servers; i++ {
			_, _, ok := cfg.rafts[i].Start(rand.Int() % 10000)
			if ok && cfg.connected[i] {
				leader = i
			}
		}

		if (rand.Int() % 1000) < 100 {
			ms := rand.Int63() % (int64(RaftElectionTimeout/time.Millisecond) / 2)
			time.Sleep(time.Duration(ms) * time.Millisecond)
		} else {
			ms := (rand.Int63() % 13)
			time.Sleep(time.Duration(ms) * time.Millisecond)
		}

		if leader != -1 && (rand.Int()%1000) < int(RaftElectionTimeout/time.Millisecond)/2 {
			cfg.disconnect(leader)
			nup -= 1
		}

		if nup < 3 {
			s := rand.Int() % servers
			if cfg.connected[s] == false {
				cfg.connect(s)
				nup += 1
			}
		}
	}

	for i := 0; i < servers; i++ {
		if cfg.connected[i] == false {
			cfg.connect(i)
		}
	}
	cfg.one(rand.Int()%10000, servers, true)

	cfg.end()
}
func TestXIndexBug2C(t *testing.T) {
	servers := 5
	cfg := make_config(t, servers, false, false)
	defer cfg.cleanup()

	cfg.begin("Test (2C): XIndexBug")

	leader := cfg.checkOneLeader()
	t.Log("Leader is ", leader)

	cmd := rand.Int() % 10000
	t.Log("Agreeing on", cmd)
	cfg.one(cmd, servers, true)
	t.Log("Agreed on", cmd)

	index := -1
	for i := 0; i < 10; i++ {
		index = cfg.one(i, servers, false)
	}
	term := int32(cfg.rafts[leader].currentTerm.Load())
	victim := (leader + 1) % servers

	t.Log("Disconnecting victim", victim)
	cfg.disconnect(victim)
	cfg.rafts[victim].addLogEntry(
		LogEntry{Index: int32(index + 1), Term: term + 1, Command: 201},
		LogEntry{Index: int32(index + 2), Term: term + 1, Command: 202},
		LogEntry{Index: int32(index + 3), Term: term + 1, Command: 203},
		LogEntry{Index: int32(index + 4), Term: term + 1, Command: 204},
		LogEntry{Index: int32(index + 5), Term: term + 1, Command: 205},
		LogEntry{Index: int32(index + 6), Term: term + 1, Command: 206},
		LogEntry{Index: int32(index + 7), Term: term + 1, Command: 207},
		LogEntry{Index: int32(index + 8), Term: term + 1, Command: 208},
		LogEntry{Index: int32(index + 9), Term: term + 1, Command: 209},
		LogEntry{Index: int32(index + 10), Term: term + 1, Command: 210},
		LogEntry{Index: int32(index + 11), Term: term + 1, Command: 211},
		LogEntry{Index: int32(index + 12), Term: term + 1, Command: 212},
	)

	for i := 0; i < 5; i++ {
		cfg.one(300+i, servers-1, false)
	}

	time.Sleep(1 * time.Second)
	t.Log("Disconnecting leader", leader)
	cfg.disconnect(leader)
	time.Sleep(2 * time.Second)
	leader2 := cfg.checkOneLeader()
	t.Log("leader2", leader2)
	t.Log("Reconnecting old leader", leader)
	cfg.connect(leader)
	time.Sleep(1 * time.Second)

	t.Log("Disconnecting leader2", leader2)
	cfg.disconnect(leader2)
	time.Sleep(2 * time.Second)
	leader3 := cfg.checkOneLeader()
	t.Log("New leader", leader3)
	t.Log("Reconnecting old leader", leader2)
	cfg.connect(leader2)
	time.Sleep(1 * time.Second)

	for i := 0; i < 5; i++ {
		cfg.one(100+i, servers-1, false)
	}

	t.Log("Reconnecting victim", victim)
	cfg.connect(victim)
	time.Sleep(2 * time.Second)
	leader4 := cfg.checkOneLeader()
	t.Log("Current leader", leader4)

	cmd = 1000
	t.Log("Agreeing on", cmd)
	cfg.one(cmd, servers, true)
	t.Log("Agreed on", cmd)

	cfg.end()
}

// func TestFigure8v2Unreliable2C(t *testing.T) {
// 	servers := 5
// 	cfg := make_config(t, servers, true, false)
// 	defer cfg.cleanup()

// 	cfg.begin("Test (2C): Figure 8 (unreliable) v2")

// 	cmd := rand.Int() % 10000
// 	t.Log("Agreeing on", cmd)
// 	cfg.one(cmd, 1, true)
// 	t.Log("Agreed on", cmd)

// 	nup := servers
// 	for iters := 0; iters < 1000; iters++ {
// 		t.Log("iteration", iters)
// 		if iters == 200 {
// 			t.Log("setlongreordering")
// 			cfg.setlongreordering(true)
// 		}
// 		leader := -1
// 		for i := 0; i < servers; i++ {
// 			cmd = rand.Int() % 10000
// 			idx, term, ok := cfg.rafts[i].Start(cmd)
// 			if ok && cfg.connected[i] {
// 				leader = i
// 				t.Log("Sent", cmd, "to", leader, "got idx", idx, "term", term)
// 			}
// 		}

// 		if (rand.Int() % 1000) < 100 {
// 			ms := rand.Int63() % (int64(RaftElectionTimeout/time.Millisecond) / 2)
// 			t.Log("Sleep1", ms)
// 			time.Sleep(time.Duration(ms) * time.Millisecond)
// 		} else {
// 			ms := (rand.Int63() % 13)
// 			t.Log("Sleep2", ms)
// 			time.Sleep(time.Duration(ms) * time.Millisecond)
// 		}

// 		if leader != -1 && (rand.Int()%1000) < int(RaftElectionTimeout/time.Millisecond)/2 {
// 			t.Log("1Disconnecting", leader)
// 			cfg.disconnect(leader)
// 			nup -= 1
// 		}

// 		if nup < 3 {
// 			s := rand.Int() % servers
// 			if cfg.connected[s] == false {
// 				t.Log("1Connecting", s)
// 				cfg.connect(s)
// 				nup += 1
// 			}
// 		}
// 	}

// 	for i := 0; i < servers; i++ {
// 		if cfg.connected[i] == false {
// 			t.Log("2Connecting", i)
// 			cfg.connect(i)
// 		}
// 	}
// 	cmd = rand.Int() % 10000
// 	t.Log("Agreeing on", cmd)
// 	cfg.one(cmd, servers, true)
// 	t.Log("Agreed on", cmd)

//		cfg.end()
//	}
func TestFigure8UnreliableSimple2C(t *testing.T) {
	servers := 5
	cfg := make_config(t, servers, true, false)
	defer cfg.cleanup()

	cfg.begin("Test (2C): Figure 8 (unreliable, simple)")

	cmd := rand.Int() % 10000
	cfg.one(cmd, 1, true)

	nup := servers
	for iters := 0; iters < 1000; iters++ {
		// for iters := 0; iters < 25; iters++ {
		if iters == 200 {
			// if iters == 10 {
			cfg.setlongreordering(true)
		}
		leader := -1
		for i := 0; i < servers; i++ {
			cmd := rand.Int() % 10000
			_, _, ok := cfg.rafts[i].Start(cmd)
			if ok && cfg.connected[i] {
				leader = i
			}
		}

		if (rand.Int() % 1000) < 100 {
			ms := rand.Int63() % (int64(RaftElectionTimeout/time.Millisecond) / 2)
			time.Sleep(time.Duration(ms) * time.Millisecond)
		} else {
			ms := (rand.Int63() % 13)
			time.Sleep(time.Duration(ms) * time.Millisecond)
		}

		if leader != -1 && (rand.Int()%1000) < int(RaftElectionTimeout/time.Millisecond)/2 {
			cfg.disconnect(leader)
			nup -= 1
		}

		if nup < 3 {
			s := rand.Int() % servers
			if cfg.connected[s] == false {
				cfg.connect(s)
				nup += 1
			}
		}
	}

	for i := 0; i < servers; i++ {
		if cfg.connected[i] == false {
			cfg.connect(i)
		}
	}
	cmd = rand.Int() % 10000
	cfg.one(cmd, servers, true)

	cfg.end()
}

func internalChurn(t *testing.T, unreliable bool) {

	servers := 5
	cfg := make_config(t, servers, unreliable, false)
	defer cfg.cleanup()

	if unreliable {
		cfg.begin("Test (2C): unreliable churn")
	} else {
		cfg.begin("Test (2C): churn")
	}

	stop := int32(0)

	// create concurrent clients
	cfn := func(me int, ch chan []int) {
		var ret []int
		ret = nil
		defer func() { ch <- ret }()
		values := []int{}
		for atomic.LoadInt32(&stop) == 0 {
			x := rand.Int()
			index := -1
			ok := false
			for i := 0; i < servers; i++ {
				// try them all, maybe one of them is a leader
				cfg.mu.Lock()
				rf := cfg.rafts[i]
				cfg.mu.Unlock()
				if rf != nil {
					index1, _, ok1 := rf.Start(x)
					if ok1 {
						ok = ok1
						index = index1
					}
				}
			}
			if ok {
				// maybe leader will commit our value, maybe not.
				// but don't wait forever.
				for _, to := range []int{10, 20, 50, 100, 200} {
					nd, cmd := cfg.nCommitted(index)
					if nd > 0 {
						if xx, ok := cmd.(int); ok {
							if xx == x {
								values = append(values, x)
							}
						} else {
							cfg.t.Fatalf("wrong command type")
						}
						break
					}
					time.Sleep(time.Duration(to) * time.Millisecond)
				}
			} else {
				time.Sleep(time.Duration(79+me*17) * time.Millisecond)
			}
		}
		ret = values
	}

	ncli := 3
	cha := []chan []int{}
	for i := 0; i < ncli; i++ {
		cha = append(cha, make(chan []int))
		go cfn(i, cha[i])
	}

	for iters := 0; iters < 20; iters++ {
		if (rand.Int() % 1000) < 200 {
			i := rand.Int() % servers
			cfg.t.Log("Disconnecting", i)
			cfg.disconnect(i)
			cfg.t.Log("Disconnected", i)
		}

		if (rand.Int() % 1000) < 500 {
			i := rand.Int() % servers
			if cfg.rafts[i] == nil {
				cfg.t.Log("Start1", i)
				cfg.start1(i, cfg.applier)
				cfg.t.Log("Start1'd", i)
			}
			cfg.t.Log("Reconnecting", i)
			cfg.connect(i)
			cfg.t.Log("Reconnected", i)
		}

		if (rand.Int() % 1000) < 200 {
			i := rand.Int() % servers
			if cfg.rafts[i] != nil {
				cfg.t.Log("Crashing", i)
				cfg.crash1(i)
				cfg.t.Log("Crashed", i)
			}
		}

		// Make crash/restart infrequent enough that the peers can often
		// keep up, but not so infrequent that everything has settled
		// down from one change to the next. Pick a value smaller than
		// the election timeout, but not hugely smaller.
		time.Sleep((RaftElectionTimeout * 7) / 10)
	}

	time.Sleep(RaftElectionTimeout)
	cfg.setunreliable(false)
	for i := 0; i < servers; i++ {
		if cfg.rafts[i] == nil {
			cfg.start1(i, cfg.applier)
		}
		cfg.connect(i)
	}

	atomic.StoreInt32(&stop, 1)

	values := []int{}
	for i := 0; i < ncli; i++ {
		vv := <-cha[i]
		if vv == nil {
			t.Fatal("client failed")
		}
		values = append(values, vv...)
	}

	time.Sleep(RaftElectionTimeout)

	lastIndex := cfg.one(rand.Int(), servers, true)

	really := make([]int, lastIndex+1)
	for index := 1; index <= lastIndex; index++ {
		v := cfg.wait(index, servers, -1)
		if vi, ok := v.(int); ok {
			really = append(really, vi)
		} else {
			t.Fatalf("not an int")
		}
	}

	for _, v1 := range values {
		ok := false
		for _, v2 := range really {
			if v1 == v2 {
				ok = true
			}
		}
		if ok == false {
			cfg.t.Fatalf("didn't find a value")
		}
	}

	cfg.end()
}

func TestReliableChurn2C(t *testing.T) {
	internalChurn(t, false)
}

func TestUnreliableChurn2C(t *testing.T) {
	internalChurn(t, true)
}

func TestAppendEntriesReorderBug2C(t *testing.T) {
	servers := 5
	cfg := make_config(t, servers, false, false)
	defer cfg.cleanup()

	cfg.begin("Test (2C): Reordering AppendEntries Bug")
	leader := cfg.checkOneLeader()
	var term int
	for i := 1; i < 6; i++ {
		_, term, _ = cfg.rafts[leader].Start(i)
	}
	time.Sleep(2 * time.Second)
	request := AppendEntriesRequest{
		PrevLogIndex: 3,
		LeaderCommit: cfg.rafts[leader].commitIndex.Load(),
		Term:         cfg.rafts[leader].currentTerm.Load(),
		LeaderId:     int32(leader),
		PrevLogTerm:  cfg.rafts[leader].getLastLogTerm(),
		Entries: []LogEntry{
			{
				Index:   4,
				Term:    int32(term),
				Command: 4,
			},
		},
	}
	server := 0
	for server == leader {
		server = (server + 1) % servers
	}
	cfg.rafts[leader].sendAppendEntries(server, &request, &AppendEntriesReply{})
	cfg.disconnect(server)
	time.Sleep(1 * time.Second)
	cfg.rafts[server].lock()
	cfg.rafts[leader].lock()
	if len(cfg.rafts[server].logs) != len(cfg.rafts[leader].logs) {
		t.Fatalf("Inconsistent Logs: Expected Log of size %v got Log of size %v", len(cfg.rafts[leader].logs), len(cfg.rafts[server].logs))
	}
	cfg.rafts[leader].unlock()
	cfg.rafts[server].unlock()
	cfg.end()
}

const MAXLOGSIZE = 2000

func snapcommon(t *testing.T, name string, disconnect bool, reliable bool, crash bool) {
	iters := 30
	servers := 3
	cfg := make_config(t, servers, !reliable, true)
	cfg.t.Log("snapcommon", name, disconnect, reliable, crash)
	defer cfg.cleanup()

	cfg.begin(name)
	cfg.one(rand.Int(), servers, true)
	leader1 := cfg.checkOneLeader()
	cfg.t.Log("Leader is", leader1)

	for i := 0; i < iters; i++ {
		cfg.t.Log("--------- iter", i)
		victim := (leader1 + 1) % servers
		sender := leader1
		if i%3 == 1 {
			sender = (leader1 + 1) % servers
			victim = leader1
		}

		if disconnect {
			cfg.t.Log("Disconnecting", victim)
			cfg.disconnect(victim)
			cfg.t.Log("Disconnected", victim)
			cmd := rand.Int()
			cfg.t.Log("Agreeing on", cmd)
			cfg.one(cmd, servers-1, true)
			cfg.t.Log("Cons. after DC", victim, "cmd", cmd)
		}
		if crash {
			cfg.t.Log("Crashing", victim)
			cfg.crash1(victim)
			cfg.t.Log("Crashed", victim)
			cfg.one(rand.Int(), servers-1, true)
		}

		// perhaps send enough to get a snapshot
		nn := (SnapShotInterval / 2) + (rand.Int() % SnapShotInterval)
		for i := 0; i < nn; i++ {
			cmd := rand.Int()
			cfg.t.Log("Sending", cmd, "to", sender)
			cfg.rafts[sender].Start(cmd)
			cfg.t.Log("Sent", cmd, "to", sender)
		}

		// let applier threads catch up with the Start()'s
		if disconnect == false && crash == false {
			// make sure all followers have caught up, so that
			// an InstallSnapshot RPC isn't required for
			// TestSnapshotBasic2D().
			cfg.one(rand.Int(), servers, true)
		} else {
			cmd := rand.Int()
			cfg.t.Log("Agreeing on", cmd, "with", servers-1)
			cfg.one(cmd, servers-1, true)
			cfg.t.Log("Agreed on", cmd, "with", servers-1)
		}

		if cfg.LogSize() >= MAXLOGSIZE {
			cfg.t.Fatalf("Log size too large")
		}
		if disconnect {
			// reconnect a follower, who maybe behind and
			// needs to rceive a snapshot to catch up.
			cfg.t.Log("Reconnecting", victim)
			cfg.connect(victim)
			cfg.t.Log("Reconnected", victim)
			cmd := rand.Int()
			cfg.t.Log("Agreeing on", cmd)
			cfg.one(cmd, servers, true)
			cfg.t.Log("Agreed on", cmd)
			leader1 = cfg.checkOneLeader()
			cfg.t.Log("New leader", leader1)
		}
		if crash {
			cfg.t.Log("Restarting", victim)
			cfg.start1(victim, cfg.applierSnap)
			cfg.connect(victim)
			cfg.one(rand.Int(), servers, true)
			leader1 = cfg.checkOneLeader()
			cfg.t.Log("New leader", leader1)
		}
	}
	cfg.end()
}

func TestSnapshotWithDisconnect2D(t *testing.T) {
	servers := 3
	name := "TestSnapshotWithDisconnect2D"
	unreliable := false
	cfg := make_config(t, servers, unreliable, true)
	cfg.t.Log(name)
	defer cfg.cleanup()

	cfg.begin(name)
	cfg.one(rand.Int(), servers, true)
	leader := cfg.checkOneLeader()
	cfg.t.Log("Leader is", leader)
	//Will snap at 8
	for i := 1; i < 8-2; i++ {
		_, _, _ = cfg.rafts[leader].Start(i + 1)
	}
	// Snapshot happens at entry: 9: 9
	time.Sleep(1 * time.Second)
	cfg.t.Log("Disconnecting: ", leader)
	cfg.disconnect(leader)
	cfg.rafts[leader].Start(100)
	cfg.rafts[leader].Start(101)
	cfg.rafts[leader].Start(102)
	cfg.one(200, servers-1, false)
	cfg.one(201, servers-1, false)
	cfg.one(202, servers-1, false)
	time.Sleep(1 * time.Second)
	cfg.t.Log("Reconnecting: ", leader)
	cfg.connect(leader)
	// time.Sleep(1 * time.Second)
	cfg.one(203, servers, true)
	for i := 0; i < servers; i++ {
		cfg.rafts[i].lock()
		// fmt.Println(i, cfg.logs[i])
		// fmt.Println(i, cfg.rafts[i].logs)
		cfg.rafts[i].unlock()
	}
	cfg.end()
}

func TestSnapshotInstallAtLastIndex(t *testing.T) {
	servers := 3
	name := "TestSnapshotInstallAtLastIndex"
	unreliable := false
	cfg := make_config(t, servers, unreliable, true)
	cfg.t.Log(name)
	defer cfg.cleanup()

	cfg.begin(name)
	cfg.one(1, servers, false)
	leader := cfg.checkOneLeader()
	other1 := (leader + 1) % servers
	other2 := (leader + 2) % servers
	cfg.t.Log("Leader is", leader)
	snap1Idx := cfg.one(2, servers, false)
	cfg.one(3, servers, false)
	cfg.one(4, servers, false)
	cfg.t.Log("Disconnecting leader: ", leader)
	cfg.disconnect(leader)

	time.Sleep(1 * time.Second)
	newLeader := cfg.checkOneLeader()
	cfg.t.Log("New Leader is", newLeader)
	cfg.rafts[leader].Start(100)
	cfg.rafts[leader].Start(101)

	cfg.one(5, servers-1, false)
	cfg.one(6, servers-1, false)
	labgob.Register(LogEntry{})
	generateSnapshot := func(raftIdx int, commandIndex int32) []byte {
		cfg.t.Logf("Snapshotting command: %v, for node %v", commandIndex, raftIdx)
		cfg.rafts[raftIdx].lock()
		defer cfg.rafts[raftIdx].unlock()
		w := new(bytes.Buffer)
		e := labgob.NewEncoder(w)
		e.Encode(commandIndex)
		var xlog []interface{}
		for j := 0; j <= int(commandIndex); j++ {
			xlog = append(xlog, cfg.rafts[raftIdx].logs[j])
		}
		cfg.t.Logf("Encoding logs: %v", xlog)
		err := e.Encode(xlog)
		if err != nil {
			cfg.t.Fatalf("Couldnt generate snapshot: %v", err)

		}
		return w.Bytes()
	}

	clone := func(orig []byte) []byte {
		x := make([]byte, len(orig))
		copy(x, orig)
		return x
	}
	// cfg.rafts[leader].Snapshot(snap1Idx, generateSnapshot(leader, int32(snap1Idx)))
	snap := generateSnapshot(other1, int32(snap1Idx))

	r := bytes.NewBuffer(clone(snap))
	d := labgob.NewDecoder(r)

	var lastIncludedIndex int
	if d.Decode(&lastIncludedIndex) != nil {
		log.Fatalf("snapshot decode error1")
	}
	log.Println("ingest1", lastIncludedIndex)
	var xlog []interface{}
	x := d.Decode(&xlog)
	if x != nil {
		t.Fatal("Bad snapshot", x, xlog)
	}

	cfg.rafts[other1].Snapshot(snap1Idx, clone(snap))
	cfg.rafts[other2].Snapshot(snap1Idx, clone(snap))
	cfg.t.Log("Reconnecting old leader: ", leader)
	// cfg.connect(leader)
	cfg.connect(leader)
	time.Sleep(1 * time.Second)
	cfg.rafts[newLeader].lock()
	currentTerm := cfg.rafts[newLeader].currentTerm.Load()
	request := &InstallSnapshotRequest{}
	request.Term = currentTerm
	request.LeaderId = cfg.rafts[newLeader].me
	request.LastIncludedIndex = int(cfg.rafts[newLeader].lastSnapshottedIndex.Load())
	request.LastIncludedTerm = cfg.rafts[newLeader].lastSnapshottedTerm.Load()
	// request.Snapshot = cfg.rafts[newLeader].persister.ReadSnapshot()
	request.Snapshot = clone(snap)
	cfg.rafts[newLeader].unlock()
	cfg.rafts[newLeader].sendInstallSnapshot(leader, request, &InstallSnapshotReply{})
	time.Sleep(1 * time.Second)
	cfg.one(7, servers, false)

	for i := 0; i < servers; i++ {
		cfg.rafts[i].lock()
		fmt.Println(i, cfg.logs[i])
		fmt.Println(i, cfg.rafts[i].logs)
		cfg.rafts[i].unlock()
	}
	cfg.end()
}

func TestSnapshotBasic2D(t *testing.T) {
	snapcommon(t, "Test (2D): snapshots basic", false, true, false)
}

func TestSnapshotInstall2D(t *testing.T) {
	snapcommon(t, "Test (2D): install snapshots (disconnect)", true, true, false)
}

func TestSnapshotInstallUnreliable2D(t *testing.T) {
	snapcommon(t, "Test (2D): install snapshots (disconnect+unreliable)",
		true, false, false)
}

func TestSnapshotInstallCrash2D(t *testing.T) {
	snapcommon(t, "Test (2D): install snapshots (crash)", false, true, true)
}

func TestSnapshotInstallUnCrash2D(t *testing.T) {
	snapcommon(t, "Test (2D): install snapshots (unreliable+crash)", false, false, true)
}

// do the servers persist the snapshots, and
// restart using snapshot along with the
// tail of the log?
func TestSnapshotAllCrash2D(t *testing.T) {
	servers := 3
	iters := 5
	cfg := make_config(t, servers, false, true)
	defer cfg.cleanup()

	cfg.begin("Test (2D): crash and restart all servers")

	cfg.one(rand.Int(), servers, true)

	for i := 0; i < iters; i++ {
		// perhaps enough to get a snapshot
		nn := (SnapShotInterval / 2) + (rand.Int() % SnapShotInterval)
		for i := 0; i < nn; i++ {
			cfg.one(rand.Int(), servers, true)
		}

		index1 := cfg.one(rand.Int(), servers, true)

		// crash all
		for i := 0; i < servers; i++ {
			cfg.crash1(i)
		}

		// revive all
		for i := 0; i < servers; i++ {
			cfg.start1(i, cfg.applierSnap)
			cfg.connect(i)
		}

		index2 := cfg.one(rand.Int(), servers, true)
		if index2 < index1+1 {
			t.Fatalf("index decreased from %v to %v", index1, index2)
		}
	}
	cfg.end()
}
