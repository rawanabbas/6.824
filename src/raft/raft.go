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

	"runtime"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
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
	Term    int32
	Command interface{}
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int32
	CandidateId  int32
	LastLogTerm  int32
	LastLogIndex int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int32
	VoteGranted bool
}

type AppendEntriesRequest struct {
	PrevLogIndex int
	LeaderCommit int32
	Term         int32
	LeaderId     int32
	PrevLogTerm  int32
	Entries      []LogEntry
}

type AppendEntriesReply struct {
	Term     int32
	Success  bool
	LeaderId int32
}

const (
	Leader    int32 = 1
	Candidate       = 2
	Follower        = 4
)

const EVENT_REQUEST_VOTE = "RequestVote"
const EVENT_APPEND_ENTRIES = "AppendEntries"
const EVENT_START_ELECTIONS = "StartElections"
const EVENT_END_ELECTIONS = "EndElections"
const EVENT_RESET_ELECTIONS = "ResetElections"
const EVENT_HEARTBEAT = "Heartbeat"
const EVENT_SHUTDOWN = "Shutdown"

type Event struct {
	Name         string
	Payload      interface{}
	Response     chan interface{}
	CreatedAt    time.Time
	CreatedState int32
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex // Lock to protect shared access to this peer's state
	timerMu   sync.Mutex
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int32               // this peer's index into peers[]
	dead      atomic.Int32

	// Your data here (2A, 2B, 2C).
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
}

func (rf *Raft) emptyHandler(event *Event) {
	rf.Warn("Got an event %v while on state %v", event.Name, rf.getStateString())
	return
}

// Event Handle Registration
func (rf *Raft) on(name string, handler func(event *Event), filter int32) {
	index := 0
	for ; filter != 0 && index < 3; filter = filter >> 1 {
		currentState := int32(1 << index)
		if filter&1 == 1 {
			rf.eventsHandlers[currentState][name] = handler
		}

		index = index + 1
	}

	for _, state := range [3]int32{Leader, Follower, Candidate} {
		if _, ok := rf.eventsHandlers[state][name]; !ok {
			rf.eventsHandlers[state][name] = rf.emptyHandler
		}
	}
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
func (rf *Raft) decrNextIndex(i int) {
	if rf.nextIndex[i].Load() > 1 {
		rf.nextIndex[i].Add(-1)
	}
}

func (rf *Raft) setMatchIndex(i int, matchIndex int32) {
	rf.matchIndex[i].Store(matchIndex)
}

func (rf *Raft) incCurrentTerm() {
	rf.currentTerm.Add(1)
}

func (rf *Raft) getStateString() string {
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
		// buf := make([]byte, 10000)
		// runtime.Stack(buf, false)
		// rf.Verbose("Locking, stack: %v", string(buf))
	}
	rf.mu.Lock()
}

func (rf *Raft) unlock() {
	// rf.Verbose("Unlocking")
	rf.mu.Unlock()
}
func (rf *Raft) lockTimer() {
	if isVerbose {
		buf := make([]byte, 10000)
		runtime.Stack(buf, false)
		rf.Verbose("Locking, stack: %v", string(buf))
	}
	rf.timerMu.Lock()
}

func (rf *Raft) unlockTimer() {
	rf.Verbose("Unlocking")
	rf.timerMu.Unlock()
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

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

func (rf *Raft) applyLogs() {
	for i := rf.lastApplied.Load() + 1; i <= rf.commitIndex.Load(); i++ {
		rf.applyCh <- ApplyMsg{
			CommandValid: true,
			Command:      rf.logs[i].Command,
			CommandIndex: int(i),
		}
		rf.lastApplied.Store(i)
	}
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
	// Your code here (2D).

}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B)
	replyCh := make(chan interface{})
	event := rf.createEvent(EVENT_REQUEST_VOTE, args, replyCh)
	rf.emit(event, false)
	for {
		select {
		case resp := <-replyCh:
			rf.Debug("Response: %v", resp)
			r := resp.(*RequestVoteReply)
			reply.Term = r.Term
			reply.VoteGranted = r.VoteGranted
			rf.Debug("Reply: %v", reply)
			return
		}
	}
}

func (rf *Raft) AppendEntries(request *AppendEntriesRequest, reply *AppendEntriesReply) {
	replyCh := make(chan interface{})
	event := rf.createEvent(EVENT_APPEND_ENTRIES, request, replyCh)
	rf.emit(event, false)
	for {
		select {
		case resp := <-replyCh:
			r := resp.(*AppendEntriesReply)
			reply.LeaderId = r.LeaderId
			reply.Success = r.Success
			reply.Term = r.Term
			return
		}
	}
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
		rf.Warn("Unreachable node %v", server)
		results <- false
		return
	}

	if reply.Term < rf.currentTerm.Load() {
		rf.Out("Received an out-dated election response ignoring...")
		return
	}

	if reply.Term > rf.currentTerm.Load() {
		// Step Down to Follower
		rf.Out("We are out-dated waiting for a leader to sync")
		rf.stepDownToFollower(reply.Term)
		results <- false
		return
	}

	results <- reply.VoteGranted
	return
}

func (rf *Raft) sendAppendEntries(server int, request *AppendEntriesRequest, reply *AppendEntriesReply) {
	rf.Debug("sendAppendEntries to %v with entries %v", server, request.Entries)
	ok := rf.peers[server].Call("Raft.AppendEntries", request, &reply)
	if !ok {
		return
	}

	if reply.Term > rf.currentTerm.Load() {
		rf.stepDownToFollower(reply.Term)
		rf.setLeader(reply.LeaderId)
		return
	}

	if reply.Success {
		mx := max(int(request.PrevLogIndex)+len(request.Entries), int(rf.matchIndex[server].Load()))
		rf.setMatchIndex(server, int32(mx))
		rf.setNextIndex(server, int32(mx+1))
	} else {
		rf.decrNextIndex(server)
	}
	rf.lock()
	logs := rf.logs
	rf.unlock()
	for n := rf.getLastLogIndex(); n >= int(rf.commitIndex.Load()); n-- {
		count := 1
		if logs[n].Term == rf.currentTerm.Load() {
			for peer := range rf.peers {
				if peer != int(rf.me) && int(rf.matchIndex[peer].Load()) >= n {
					count++
				}
			}
		}
		if count > len(rf.peers)/2 {
			rf.setCommitIndex(int32(n))
			go rf.applyLogs()
			break
		}
	}

}

func (rf *Raft) stepDownToFollower(term int32) {
	rf.setVotedFor(-1)
	rf.setState(Follower)
	rf.setCurrentTerm(term)
}

func (rf *Raft) emit(event *Event, async bool) {
	if async {
		go func() {
			rf.eventCh <- event
		}()
	} else {
		rf.eventCh <- event
	}
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
		return -1, -1, false
	}
	term := rf.currentTerm.Load()
	isLeader := true

	entry := LogEntry{
		Term:    term,
		Command: command,
	}

	rf.lock()
	rf.logs = append(rf.logs, entry)
	rf.unlock()
	index := rf.getLastLogIndex()

	return int(index), int(term), isLeader
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
	return len(rf.logs) - 1
}

func (rf *Raft) getLastLogTerm() int32 {
	lastLogIndex := max(rf.getLastLogIndex()-1, 0)
	rf.lock()
	defer rf.unlock()
	return rf.logs[lastLogIndex].Term
}

func (rf *Raft) voteForSelf() {
	rf.setState(Candidate)
	rf.setVotedFor(rf.me)
	rf.incCurrentTerm()
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
		for {
			select {
			case vote := <-results:
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
	for {
		select {
		case <-rf.heartbeatTicker.C:
			if rf.stopHeartbeat.Load() {
				rf.heartbeatTicker.Stop()
			} else {
				event := rf.createEvent(EVENT_HEARTBEAT, nil, nil)
				rf.emit(event, false)
			}
		}
	}
}

func (rf *Raft) sendAppendEntriesToAllPeers() {

	for peer := range rf.peers {
		if peer == int(rf.me) {
			continue
		}
		request := &AppendEntriesRequest{}
		rf.lock()
		request.Entries = rf.logs[rf.nextIndex[peer].Load():]
		logs := rf.logs
		request.LeaderId = rf.me
		request.LeaderCommit = rf.commitIndex.Load()
		request.Term = rf.currentTerm.Load()
		prevIndex := rf.nextIndex[peer].Load() - 1
		request.PrevLogIndex = max(int(prevIndex), 0)
		request.PrevLogTerm = logs[request.PrevLogIndex].Term
		rf.unlock()
		go rf.sendAppendEntries(peer, request, &AppendEntriesReply{})
	}
}

func (rf *Raft) sendHeartbeats() {
	rf.sendAppendEntriesToAllPeers()
}

func (rf *Raft) resetVoltaileState() {
	rf.nextIndex = make([]atomic.Int32, len(rf.peers))
	rf.matchIndex = make([]atomic.Int32, len(rf.peers))
	for peer := range rf.peers {
		rf.setMatchIndex(peer, 0)
		rf.setNextIndex(peer, 1)
	}
}

func (rf *Raft) handleStartElections(event *Event) {
	rf.voteForSelf()
	rf.broadcastVoteRequest()
}

func (rf *Raft) handleRequestVote(event *Event) {
	request := event.Payload.(*RequestVoteArgs)
	reply := &RequestVoteReply{}

	if rf.state.Load() == Candidate {
		rf.Debug("Handling Competing Votes----------------------")
	}

	if request.Term < rf.currentTerm.Load() {
		rf.Debug("Outdated candidate requested vote rejecting...")
		reply.Term = rf.currentTerm.Load()
		reply.VoteGranted = false
		event.Response <- reply
		return
	}

	if request.Term > rf.currentTerm.Load() {
		rf.stepDownToFollower(request.Term)
	}

	if (rf.votedFor.Load() == -1 || rf.votedFor.Load() == request.CandidateId) && rf.areLogsUptoDate(request.LastLogIndex, request.LastLogTerm) {
		rf.Debug("Logs are up to date and not voted for anyone else yet granting vote....")
		reply.Term = request.Term
		reply.VoteGranted = true
		rf.setVotedFor(request.CandidateId)
		event.Response <- reply
		return
	}

	rf.Debug("Logs are not up to date rejecting vote...")
	reply.Term = rf.currentTerm.Load()
	reply.VoteGranted = false
	event.Response <- reply
	return
}

func (rf *Raft) startHearbeat() {
	rf.lock()
	rf.stopHeartbeat.Store(false)
	rf.heartbeatTicker.Reset(50 * time.Millisecond)
	rf.unlock()
}

func (rf *Raft) handleEndElections(event *Event) {
	win := event.Payload.(bool)
	if win {
		rf.Out("Stepping up as leader...")
		rf.setState(Leader)
		rf.resetVoltaileState()
		rf.sendHeartbeats()
		rf.startHearbeat()
	} else {
		rf.Out("Stepping down to follower...")
		rf.setVotedFor(-1)
		rf.setState(Follower)
	}
}

func (rf *Raft) handleAppendEntries(event *Event) {
	request := event.Payload.(*AppendEntriesRequest)
	rf.resetElectionTimer()
	reply := &AppendEntriesReply{}
	// rf.Debug("handle append entries %v", event.Payload)
	if request.Term < rf.currentTerm.Load() {
		reply.Term = rf.currentTerm.Load()
		reply.Success = false
		event.Response <- reply
		return
	}

	if request.Term > rf.currentTerm.Load() {
		if rf.state.Load() == Leader {
			rf.stopHeartbeat.Store(true)
		}
		rf.stepDownToFollower(request.Term)
		rf.setLeader(request.LeaderId)
	}

	lastLogIndex := rf.getLastLogIndex()
	reply.Success = false
	reply.Term = rf.currentTerm.Load()
	if request.PrevLogIndex > lastLogIndex || rf.logs[request.PrevLogIndex].Term != request.PrevLogTerm {
		event.Response <- reply
		return
	}

	j := 0
	i := request.PrevLogIndex + 1
	for ; i < lastLogIndex+1 && j < len(request.Entries); i, j = i+1, j+1 {
		if rf.logs[i].Term != request.Entries[j].Term {
			break
		}
	}
	rf.lock()
	rf.logs = rf.logs[:i]
	rf.logs = append(rf.logs, request.Entries[j:]...)
	rf.unlock()
	reply.Success = true
	if request.LeaderCommit > rf.commitIndex.Load() {
		mn := min(int(request.LeaderCommit), rf.getLastLogIndex())
		rf.setCommitIndex(int32(mn))
		//Commit new log changes
		rf.applyLogs()
	}
	event.Response <- reply
}

func (rf *Raft) handleHeartbeats(event *Event) {
	rf.sendHeartbeats()
}

func (rf *Raft) handleShutdown(event *Event) {
	rf.Debug("Final Logs before shutdown: %v", rf.logs)
	rf.dead.Store(1)
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
	// atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	event := rf.createEvent(EVENT_SHUTDOWN, nil, nil)
	rf.emit(event, false)
}

func (rf *Raft) killed() bool {
	return rf.dead.Load() == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) serve() {
	rf.Debug("Serving...")
	for rf.killed() == false {
		select {
		case event := <-rf.eventCh:
			if event.Name == EVENT_HEARTBEAT {
				rf.Verbose("Event: %v", event.Name)
			} else {
				rf.Debug("Event: %v", event.Name)
			}
			rf.eventsHandlers[rf.state.Load()][event.Name](event)
		}
	}
}

func (rf *Raft) resetElectionTimer() {
	rf.resetTimer.Store(true)
}

func (rf *Raft) electionTicker() {
	for {
		select {
		case <-rf.electionTimer:
			if rf.dead.Load() != 1 {
				if rf.resetTimer.Load() == true {
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
	SetDebug(true)
	// SetVerbose(true)
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = int32(me)

	// Your initialization code here (2A, 2B, 2C).
	rf.setState(Follower)
	rf.setCurrentTerm(0)
	rf.setVotedFor(-1)
	rf.setCommitIndex(0)
	rf.setLastApplied(0)
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

	rf.logs = append(rf.logs, LogEntry{Term: 0})

	rf.electionTimer = randomTimeout()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// Register Event Handlers
	rf.on(EVENT_HEARTBEAT, rf.handleHeartbeats, Leader)
	rf.on(EVENT_END_ELECTIONS, rf.handleEndElections, Candidate)
	rf.on(EVENT_REQUEST_VOTE, rf.handleRequestVote, Follower|Candidate)
	rf.on(EVENT_START_ELECTIONS, rf.handleStartElections, Follower|Candidate)
	rf.on(EVENT_APPEND_ENTRIES, rf.handleAppendEntries, Follower|Candidate|Leader)
	rf.on(EVENT_SHUTDOWN, rf.handleShutdown, Follower|Leader|Candidate)
	// start ticker goroutine to start elections
	go rf.electionTicker()
	go rf.serve()

	return rf
}
