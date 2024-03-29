package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"

	"bytes"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"6.824-2022/labgob"
	"6.824-2022/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntry struct {
	Index   int32
	Term    int32
	Command interface{}
}

const (
	Leader    int32 = 1
	Candidate int32 = 2
	Follower  int32 = 4
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.RWMutex // Lock to protect shared access to this peer's state
	snapMu    sync.Mutex
	applyMu   sync.Mutex
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int32               // this peer's index into peers[]
	dead      atomic.Int32

	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state  atomic.Int32
	leader atomic.Int32

	//Presistent State
	currentTerm atomic.Int32
	votedFor    atomic.Int32
	logs        []LogEntry

	//Voltaile State
	commitIndex atomic.Int32
	lastApplied atomic.Int32

	//Leader Voltaile State
	nextIndex  []atomic.Int32
	matchIndex []atomic.Int32

	//Snapshot State
	lastSnapshottedIndex atomic.Int32
	lastSnapshottedTerm  atomic.Int32

	// Channels
	eventCh chan *Event
	applyCh chan ApplyMsg

	// Timer and Tickers
	electionTimer   <-chan time.Time
	heartbeatTicker *time.Ticker

	// Event Handlers
	eventsHandlers map[int32]map[string]func(event *Event)

	//Utility Flags
	resetTimer    atomic.Bool
	stopHeartbeat atomic.Bool
	beingApplied  atomic.Int32
}

type SnapshotPayload struct {
	Index    int
	Snapshot []byte
}

// Setters
func (rf *Raft) setState(state int32) {
	rf.state.Store(state)
}

func (rf *Raft) setLeader(leader int32) {
	rf.leader.Store(leader)
}

func (rf *Raft) setCurrentTerm(term int32) {
	rf.currentTerm.Store(term)
}
func (rf *Raft) setLastSnapshottedIndex(idx int32) {
	rf.lastSnapshottedIndex.Store(idx)
}
func (rf *Raft) setLastSnapshottedTerm(term int32) {
	rf.lastSnapshottedTerm.Store(term)
}

func (rf *Raft) setVotedFor(votedFor int32) {
	rf.votedFor.Store(votedFor)
}

func (rf *Raft) setCommitIndex(commitIndex int32) {
	rf.commitIndex.Store(commitIndex)
}

func (rf *Raft) setLastApplied(lastApplied int32) {
	rf.lastApplied.Store(lastApplied)
}

func (rf *Raft) setNextIndex(i int, nextIndex int32) {
	rf.nextIndex[i].Store(nextIndex)
}

func (rf *Raft) setMatchIndex(i int, matchIndex int32) {
	rf.matchIndex[i].Store(matchIndex)
}

func (rf *Raft) incCurrentTerm() {
	rf.currentTerm.Add(1)
}

func (rf *Raft) getLogEntry(index int) (entry LogEntry) {
	rf.lock()
	defer rf.unlock()
	firstIdx := rf.lastSnapshottedIndex.Load()
	entry = rf.logs[index-int(firstIdx)]
	return
}

func (rf *Raft) addLogEntry(entries ...LogEntry) {
	rf.lock()
	defer rf.unlock()
	rf.logs = append(rf.logs, entries...)
}

func (rf *Raft) GetStateString() string {
	var state string
	switch rf.state.Load() {
	case Follower:
		state = " Follower  "
	case Candidate:
		state = " Candidate "
	case Leader:
		state = " Leader    "
	}
	return state
}

func (rf *Raft) lock() {
	if isVerbose {
		buf := make([]byte, 10000)
		runtime.Stack(buf, false)
		rf.Verbose("Locking, stack: %v", string(buf))
	}
	rf.mu.Lock()
}

func (rf *Raft) applyLock() {
	rf.applyMu.Lock()
}

func (rf *Raft) snapLock() {
	rf.Debug("Acquiring Snap Lock....")
	if isVerbose {
		buf := make([]byte, 10000)
		runtime.Stack(buf, false)
		rf.Verbose("Snapshot Locking, stack: %v", string(buf))
	}
	rf.snapMu.Lock()
	rf.Debug("Acquired Snap Lock....")
}

func (rf *Raft) snapUnlock() {
	rf.Verbose("Snap Unlocking")
	rf.snapMu.Unlock()
}

func (rf *Raft) applyUnlock() {
	rf.Verbose("Apply Unlocking")
	rf.applyMu.Unlock()
}

func (rf *Raft) unlock() {
	rf.Verbose("Main Unlocking")
	rf.mu.Unlock()
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int32, bool) {
	rf.lock()
	defer rf.unlock()
	term := rf.currentTerm.Load()
	isLeader := rf.state.Load() == Leader
	return term, isLeader
}

func (rf *Raft) generatePersistantState() ([]byte, error) {
	rf.lock()
	defer rf.unlock()
	buff := new(bytes.Buffer)
	enc := labgob.NewEncoder(buff)
	if enc.Encode(rf.currentTerm.Load()) != nil || enc.Encode(rf.votedFor.Load()) != nil || enc.Encode(rf.logs) != nil || enc.Encode(rf.lastSnapshottedIndex.Load()) != nil || enc.Encode(rf.lastSnapshottedTerm.Load()) != nil {
		rf.Error("Failed to encode raft state")
		return nil, fmt.Errorf("failed to encode raft state")
	}
	data := buff.Bytes()
	return data, nil
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	data, err := rf.generatePersistantState()
	if err != nil {
		rf.Error(err.Error())
	} else {
		rf.persister.SaveRaftState(data)
	}
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	buff := bytes.NewBuffer(data)
	dec := labgob.NewDecoder(buff)
	var currentTerm int32
	var lastSnapshottedIndex int32
	var lastSnapshottedTerm int32
	var votedFor int32
	var logs []LogEntry
	if dec.Decode(&currentTerm) != nil || dec.Decode(&votedFor) != nil || dec.Decode(&logs) != nil || dec.Decode(&lastSnapshottedIndex) != nil || dec.Decode(&lastSnapshottedTerm) != nil {
		rf.Error("Failed to read/recover persistent state")
	} else {
		rf.setCurrentTerm(currentTerm)
		rf.setVotedFor(votedFor)
		rf.lastSnapshottedIndex.Store(lastSnapshottedIndex)
		rf.lastSnapshottedTerm.Store(lastSnapshottedTerm)
		rf.logs = logs
	}
}

func (rf *Raft) applySnap(snapshot []byte, index int, term int32) {
	rf.Debug("Apply snap: last si: %v/T%v", rf.lastSnapshottedIndex.Load(), rf.lastSnapshottedTerm.Load())
	rf.Out("Apply snap: last si: %v/T%v [%v/T%v]", rf.lastSnapshottedIndex.Load(), rf.lastSnapshottedTerm.Load(), index, term)
	msg := ApplyMsg{
		SnapshotValid: true,
		Snapshot:      snapshot,
		SnapshotTerm:  int(term),
		SnapshotIndex: int(index) - 1,
	}
	rf.Out("apply snap message: %v", msg)
	rf.applyCh <- msg
	rf.Debug("Finish apply snap")
	// rf.Out("Finish apply snap")
}

func (rf *Raft) applyLogs() {
	rf.Out("Applying logs")
	rf.Debug("Applying logs")
	defer func() {
		rf.Debug("Finished applying logs")
		rf.Out("Finished applying logs")
	}()
	rf.applyLock()
	defer rf.applyUnlock()
	if len(rf.logs) == 0 {
		return
	}
	rf.lock()
	firstIdx := rf.lastSnapshottedIndex.Load()

	messages := []ApplyMsg{}
	var i int32
	if rf.beingApplied.Load() >= firstIdx {
		i = rf.beingApplied.Load() + 1
	} else if rf.lastApplied.Load() < firstIdx {
		i = firstIdx
	} else {
		i = rf.lastApplied.Load() + 1
	}
	rf.Debug("Being Applied: %v First Index: %v lastApplied: %v, lastSnapshottedIndex: %v", rf.beingApplied, firstIdx, rf.lastApplied.Load(), rf.lastSnapshottedIndex.Load())
	rf.Out("First Index: %v lastApplied: %v, lastSnapshottedIndex: %v", firstIdx, rf.lastApplied.Load(), rf.lastSnapshottedIndex.Load())
	var la int
	for ; i <= rf.commitIndex.Load(); i++ {
		entry := rf.logs[i-firstIdx]
		messages = append(messages, ApplyMsg{
			CommandValid: true,
			Command:      entry.Command,
			CommandIndex: int(entry.Index),
		})
		la = int(entry.Index)
	}
	rf.Debug("Applying logs xxxxx %v", messages)
	rf.Out("Applying logs xxxxx %v", messages)
	rf.beingApplied.Store(int32(la))
	rf.unlock()

	for _, msg := range messages {
		rf.Debug("Applying log xxxxx %v", msg)
		rf.Out("Applying log xxxxx %v", msg)
		rf.applyCh <- msg
		// rf.lastApplied.Store(int32(msg.CommandIndex))
		rf.setLastApplied(int32(msg.CommandIndex))
	}
	rf.beingApplied.Store(-1)

}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.SnapshotWithReply(index, snapshot, nil)
}

func (rf *Raft) SnapshotWithReply(index int, snapshot []byte, replyCh chan interface{}) {
	evtPayload := &SnapshotPayload{
		Index:    index,
		Snapshot: snapshot,
	}
	evt := rf.createEvent(EVENT_SNAPSHOT, evtPayload, replyCh)
	rf.Out("Emitting snapshot %v", index)
	rf.emit(evt, false)
	rf.Out("Done emitting snapshot %v", index)
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply, results chan bool) {
	ok := rf.peers[server].Call("Raft.RequestVote", args, &reply)
	if !ok {
		rf.Warn("Couldn't send RequestVote Unreachable node %v", server)
		results <- false
		return
	}

	if reply.Term < rf.currentTerm.Load() {
		rf.Out("Received an out-dated election response ignoring from %v/T%v...", server, reply.Term)
		return
	}

	if reply.Term > rf.currentTerm.Load() {
		// Step Down to Follower
		rf.Out("We are out-dated waiting for a leader to sync %v/T%v", server, reply.Term)
		rf.stepDownToFollower(reply.Term)
		rf.persist()
		results <- false
		return
	}
	rf.Debug("Received a vote [%v] from %v/T%v", reply.VoteGranted, server, reply.Term)
	results <- reply.VoteGranted
}

func (rf *Raft) sendInstallSnapshot(server int, request *InstallSnapshotRequest, reply *InstallSnapshotReply) {
	if rf.state.Load() != Leader {
		return
	}
	rf.Debug("sendInstallSnapshot to %v [%v]", server, request.LastIncludedIndex)
	ok := rf.peers[server].Call("Raft.InstallSnapshot", request, &reply)
	rf.Debug("sentInstallSnapshot to %v [%v]", server, request.LastIncludedIndex)
	if !ok {
		rf.Error("Couldn't Send InstallSnapshot Request to %v", server)
		return
	}

	if reply.Term > rf.currentTerm.Load() {
		rf.stepDownToFollower(reply.Term)
		return
	}
	rf.lock()
	rf.nextIndex[server].Store(int32(request.LastIncludedIndex))
	rf.unlock()
}

func (rf *Raft) sendAppendEntries(server int, request *AppendEntriesRequest, reply *AppendEntriesReply) {
	if rf.state.Load() != Leader {
		rf.Debug("sendAppendEntries, but not a leader anymore")
		return
	}
	rf.Debug("sendAppendEntries to %v with entries %v", server, request.Entries)
	ok := rf.peers[server].Call("Raft.AppendEntries", request, &reply)
	if !ok {
		rf.Debug("failed to sendAppendEntries to %v", server)
		return
	}

	defer rf.persist()

	if reply.Term > request.Term {
		rf.Debug("reply.Term %v > request.Term %v", reply.Term, request.Term)
		rf.stepDownToFollower(reply.Term)
		rf.setLeader(reply.LeaderId)
		return
	}

	rf.lock()
	if reply.Success {
		mx := max(int(request.PrevLogIndex)+len(request.Entries), int(rf.matchIndex[server].Load()))
		rf.setMatchIndex(server, int32(mx))
		rf.setNextIndex(server, int32(mx+1))
	} else {
		if reply.XTerm == -1 {
			//Follower log too short
			rf.Debug("Setting next index of %v to %v (xterm == -1)", server, reply.XIndex)
			rf.setNextIndex(server, int32(reply.XIndex))
		} else {
			isNextIndexUpdated := false
			for i := len(rf.logs) - 1; i >= 0; i-- {
				xidx, xterm := rf.logs[i].Index, rf.logs[i].Term
				if xterm == reply.XTerm {
					// Leader has XTerm so nextIndex is last entry for that term
					isNextIndexUpdated = true
					nextIndex := min(int(xidx+1), int(rf.nextIndex[server].Load()))
					rf.Debug("Leader has XTerm Setting Peer %v's Next Index %v", server, nextIndex)
					rf.setNextIndex(server, int32(nextIndex))
					break
				}

				if xterm < reply.XTerm {
					// Leader doesn't have XTerm
					isNextIndexUpdated = true
					nextIndex := min(reply.XIndex, int(rf.nextIndex[server].Load()))
					rf.Debug("Leader doesn't have XTerm Setting Peer %v's Next Index %v", server, nextIndex)
					rf.setNextIndex(server, int32(nextIndex))
					break
				}
			}

			if !isNextIndexUpdated {
				if rf.lastSnapshottedIndex.Load() != 0 {
					rf.setNextIndex(server, rf.lastSnapshottedIndex.Load()-1)
				} else {
					rf.setNextIndex(server, 1)
				}
			}
		}
	}
	logs := rf.logs
	if len(rf.logs) == 0 {
		rf.unlock()
		return
	}
	rf.unlock()
	rf.snapLock()
	lastSnapIndex := rf.lastSnapshottedIndex.Load()
	n := int32(rf.commitIndex.Load() + 1)
	if rf.commitIndex.Load() < lastSnapIndex {
		n = lastSnapIndex
	}
	for ; n < int32(len(logs)+int(lastSnapIndex)); n++ {
		count := 1
		if logs[n-lastSnapIndex].Term == rf.currentTerm.Load() {
			for peer := range rf.peers {
				if peer != int(rf.me) && rf.matchIndex[peer].Load() >= n {
					// rf.Debug("Match index of %v is %v > %v", peer, rf.matchIndex[peer].Load(), n)
					count++
				}
			}
		}
		// rf.Debug("log: %v, reached %v/%v", n, count, len(rf.peers)/2)
		if count > len(rf.peers)/2 {
			rf.Debug("log: %v, reached quorum", n)
			// if rf.commitIndex.Load() >= n {
			// }
			rf.setCommitIndex(int32(n))
			// dur = time.Since(start)
			go rf.applyLogs()
			break
		}
	}
	rf.snapUnlock()

}

func (rf *Raft) stepDownToFollower(term int32) {
	rf.Out("Going with the flow!")
	rf.setVotedFor(-1)
	rf.setState(Follower)
	rf.stopHeartbeat.Store(true)
	rf.setCurrentTerm(term)
	rf.persist()
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	if rf.state.Load() != Leader {
		return -1, int(rf.currentTerm.Load()), false
	}
	rf.lock()
	index := len(rf.logs) - 1
	var incIndex int32
	if index >= 0 {
		incIndex = rf.logs[index].Index + 1
	} else {
		incIndex = rf.lastSnapshottedIndex.Load()
	}
	term := rf.currentTerm.Load()
	isLeader := true

	entry := LogEntry{
		Index:   incIndex,
		Term:    term,
		Command: command,
	}

	rf.logs = append(rf.logs, entry)
	rf.nextIndex[rf.me].Add(1)
	rf.matchIndex[rf.me].Store(incIndex)

	rf.unlock()

	rf.persist()
	defer func() {
		event := rf.createEvent(EVENT_HEARTBEAT, nil, nil)
		rf.heartbeatTicker.Reset(50 * time.Millisecond)
		rf.emit(event, true)
	}()

	return int(incIndex), int(term), isLeader
}

func (rf *Raft) createEvent(name string, payload interface{}, response chan interface{}) (event *Event) {
	event = &Event{}
	event.Name = name
	event.Payload = payload
	event.Response = response
	event.CreatedAt = time.Now()
	event.CreatedState = rf.state.Load()
	return
}

func (rf *Raft) getLastLogIndex() int {
	rf.lock()
	defer rf.unlock()
	if len(rf.logs) == 0 {
		return int(rf.lastSnapshottedIndex.Load() - 1)
	}
	return int(rf.logs[len(rf.logs)-1].Index)
}

func (rf *Raft) getLastLogTerm() int32 {
	rf.lock()
	defer rf.unlock()
	if len(rf.logs) == 0 {
		return rf.lastSnapshottedTerm.Load()
	}
	lastLogIndex := len(rf.logs) - 1
	return rf.logs[lastLogIndex].Term
}

func (rf *Raft) voteForSelf() {
	rf.Out("No Leader Maybe I will be")
	rf.setState(Candidate)
	rf.setVotedFor(rf.me)
	rf.incCurrentTerm()
	rf.persist()
}

func (rf *Raft) broadcastVoteRequest() {
	request := RequestVoteArgs{
		Term:         rf.currentTerm.Load(),
		CandidateId:  rf.me,
		LastLogTerm:  rf.getLastLogTerm(),
		LastLogIndex: rf.getLastLogIndex(),
	}

	rf.Debug("Request ready: %v", request)
	results := make(chan bool)
	for peer := range rf.peers {
		if peer == int(rf.me) {
			continue
		}
		go rf.sendRequestVote(peer, &request, &RequestVoteReply{}, results)
	}

	votesGranted := 1
	votes := 1
	go func() {
		for !rf.killed() {
			vote := <-results
			rf.Debug("Received a reply!!! %v", vote)
			votes++
			if vote {
				votesGranted++
			}
			rf.Debug("Votes Granted so far: %v, Cutoff: %v", votesGranted, len(rf.peers)/2+1)
			if votesGranted >= len(rf.peers)/2+1 {
				evt := rf.createEvent(EVENT_END_ELECTIONS, true, nil)
				rf.emit(evt, false)
				return
			}

			if votes >= len(rf.peers)/2+1 {
				evt := rf.createEvent(EVENT_END_ELECTIONS, false, nil)
				rf.emit(evt, false)
				return
			}
		}
	}()
}

func (rf *Raft) areLogsUptoDate(cLastIndex int, cLastTerm int32) bool {
	lastIndex, lastTerm := rf.getLastLogIndex(), rf.getLastLogTerm()
	if cLastTerm == lastTerm {
		return cLastIndex >= lastIndex
	}
	return cLastTerm > lastTerm
}

func (rf *Raft) startHeartbeatTicker() {
	for !rf.killed() {
		<-rf.heartbeatTicker.C
		if rf.stopHeartbeat.Load() {
			rf.heartbeatTicker.Stop()
		} else {
			event := rf.createEvent(EVENT_HEARTBEAT, nil, nil)
			rf.emit(event, false)
		}

	}
}

func (rf *Raft) sendAppendEntriesToAllPeers() {
	rf.snapLock()
	defer rf.snapUnlock()
	currentTerm := rf.currentTerm.Load()
	for peer := range rf.peers {
		if peer == int(rf.me) {
			continue
		}
		rf.lock()
		if rf.nextIndex[peer].Load() < rf.lastSnapshottedIndex.Load() {
			// Send Install Snapshot
			request := &InstallSnapshotRequest{}
			request.Term = currentTerm
			request.LeaderId = rf.me
			request.LastIncludedIndex = int(rf.lastSnapshottedIndex.Load())
			request.LastIncludedTerm = rf.lastSnapshottedTerm.Load()
			request.Snapshot = rf.persister.ReadSnapshot()
			rf.unlock()
			rf.Out("Sending install snapshot to %v because %v < %v", peer, rf.nextIndex[peer].Load(), rf.lastSnapshottedIndex.Load())
			go rf.sendInstallSnapshot(peer, request, &InstallSnapshotReply{})
			continue
		}
		request := &AppendEntriesRequest{}
		nextIndex := rf.nextIndex[peer].Load() - rf.lastSnapshottedIndex.Load()
		rf.Debug("Peer: %v, peer nextIndex: %v, lastSI: %v, nextIndex: %v", peer, rf.nextIndex[peer].Load(), rf.lastSnapshottedIndex.Load(), nextIndex)
		if int(nextIndex) >= len(rf.logs) {
			request.Entries = []LogEntry{}
		} else {
			request.Entries = rf.logs[nextIndex:]
		}
		logs := rf.logs
		request.LeaderId = rf.me
		request.LeaderCommit = rf.commitIndex.Load()
		request.Term = currentTerm
		prevIndex := rf.nextIndex[peer].Load() - 1 - rf.lastSnapshottedIndex.Load()
		rf.Debug("For Peer %v Prev Index: %v LSI: %v New Prev Index: %v Len(logs): %v", peer, rf.nextIndex[peer].Load()-1, rf.lastSnapshottedIndex.Load(), prevIndex, len(logs))
		if prevIndex < 0 {
			request.PrevLogIndex = int(rf.lastSnapshottedIndex.Load() - 1)
			request.PrevLogTerm = rf.lastSnapshottedTerm.Load()
		} else {
			request.PrevLogIndex = int(logs[prevIndex].Index)
			request.PrevLogTerm = logs[prevIndex].Term
		}

		rf.unlock()
		go rf.sendAppendEntries(peer, request, &AppendEntriesReply{})
	}
}

func (rf *Raft) sendHeartbeats() {
	rf.sendAppendEntriesToAllPeers()
}

func (rf *Raft) resetVoltaileState() {
	lastLogIndex := rf.getLastLogIndex()
	rf.nextIndex = make([]atomic.Int32, len(rf.peers))
	rf.matchIndex = make([]atomic.Int32, len(rf.peers))
	rf.lock()
	for peer := range rf.peers {
		// if rf.lastSnapshottedIndex.Load() == 0 {
		rf.setMatchIndex(peer, 0)
		// } else {
		// rf.setMatchIndex(peer, rf.lastSnapshottedIndex.Load()-1)
		// }
		rf.setNextIndex(peer, int32(lastLogIndex)+1)
	}
	rf.unlock()
}

func (rf *Raft) startHearbeat() {
	// rf.lock()
	rf.stopHeartbeat.Store(false)
	rf.heartbeatTicker.Reset(50 * time.Millisecond)
	// rf.unlock()
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	event := rf.createEvent(EVENT_SHUTDOWN, nil, nil)
	rf.emit(event, false)
}

func (rf *Raft) killed() bool {
	return rf.dead.Load() == 1
}
func (rf *Raft) Killed() bool {
	return rf.dead.Load() == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) serve() {
	rf.Debug("Serving...")
	for !rf.killed() {
		event := <-rf.eventCh
		if event.Name == EVENT_HEARTBEAT {
			rf.Verbose("Event: %v", event.Name)
		} else {
			rf.Debug("Event: %v", event.Name)
		}
		rf.eventsHandlers[rf.state.Load()][event.Name](event)
	}
}

func (rf *Raft) resetElectionTimer() {
	rf.resetTimer.Store(true)
}

func (rf *Raft) electionTicker() {
	for !rf.killed() {
		<-rf.electionTimer
		if rf.dead.Load() != 1 {
			if rf.resetTimer.Load() {
				rf.resetTimer.Store(false)
				rf.electionTimer = randomTimeout()
			} else {
				event := rf.createEvent(EVENT_START_ELECTIONS, nil, nil)
				if rf.state.Load() != Leader {
					rf.emit(event, false)
				}
				rf.electionTimer = randomTimeout()
			}
		}
	}
}

func (msg ApplyMsg) String() string {
	str := fmt.Sprintf("[CommandValid: %v, Command: %v, CommandIndex: %v", msg.CommandValid, msg.Command, msg.CommandIndex)
	if msg.SnapshotValid {
		str = str + " " + fmt.Sprintf("SnapshotValid: %v, SnapShotTerm: %v, SnapshotIndex: %v", msg.SnapshotValid, msg.SnapshotTerm, msg.SnapshotIndex)
	}
	str = str + "]"
	return str
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	// SetDebug(true)
	// SetVerbose(true)
	SuppressLogs()
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = int32(me)

	// Initialize State
	rf.setState(Follower)
	rf.setCurrentTerm(0)
	rf.setVotedFor(-1)
	rf.setCommitIndex(0)
	rf.setLastApplied(0)
	rf.setLastSnapshottedIndex(0)
	rf.setLastSnapshottedTerm(0)
	rf.beingApplied.Store(-1)
	rf.resetTimer.Store(false)
	rf.eventCh = make(chan *Event)
	rf.applyCh = applyCh
	rf.resetVoltaileState()
	rf.eventsHandlers = make(map[int32]map[string]func(event *Event))
	rf.eventsHandlers[Follower] = make(map[string]func(event *Event))
	rf.eventsHandlers[Candidate] = make(map[string]func(event *Event))
	rf.eventsHandlers[Leader] = make(map[string]func(event *Event))
	rf.heartbeatTicker = time.NewTicker(50 * time.Millisecond)
	rf.heartbeatTicker.Stop()
	rf.stopHeartbeat.Store(true)
	go rf.startHeartbeatTicker()

	rf.addLogEntry(LogEntry{Term: 0, Index: 0})
	rf.electionTimer = randomTimeout()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// Register Event Handlers
	rf.on(EVENT_HEARTBEAT, rf.handleHeartbeats, Leader)
	rf.on(EVENT_END_ELECTIONS, rf.handleEndElections, Candidate)
	rf.on(EVENT_START_ELECTIONS, rf.handleStartElections, Follower|Candidate)
	rf.on(EVENT_APPEND_ENTRIES, rf.handleAppendEntries, Follower|Candidate|Leader)
	rf.on(EVENT_REQUEST_VOTE, rf.handleRequestVote, Follower|Candidate|Leader)
	rf.on(EVENT_SHUTDOWN, rf.handleShutdown, Follower|Leader|Candidate)
	rf.on(EVENT_SNAPSHOT, rf.handleSnaphshot, Follower|Leader|Candidate)
	rf.on(EVENT_INSTALL_SNAPSHOT, rf.handleInstallSnapshot, Follower|Candidate)
	// start ticker goroutine to start elections
	go rf.electionTicker()
	go rf.serve()

	return rf
}
