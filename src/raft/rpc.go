package raft

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

type InstallSnapshotRequest struct {
	Term              int32
	LeaderId          int32
	LastIncludedIndex int
	LastIncludedTerm  int32
	Snapshot          []byte
}

type InstallSnapshotReply struct {
	Term int32
}

// RequestVote RPC handler.
func (rf *Raft) RequestVote(request *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B)
	replyCh := make(chan interface{})
	event := rf.createEvent(EVENT_REQUEST_VOTE, request, replyCh)
	rf.emit(event, false)
	for {
		select {
		case resp := <-replyCh:
			rf.Debug("Response: %v", resp)
			r := resp.(*RequestVoteReply)
			reply.Term = r.Term
			reply.VoteGranted = r.VoteGranted
			rf.persist()
			rf.Debug("Reply: %v", reply)
			return
		}
	}
}

// AppendEntries RPC handler
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

func (rf *Raft) InstallSnapshot(request *InstallSnapshotRequest, reply *InstallSnapshotReply) {
	replyCh := make(chan interface{})
	event := rf.createEvent(EVENT_INSTALL_SNAPSHOT, request, replyCh)
	rf.emit(event, false)
	for {
		select {
		case resp := <-replyCh:
			r := resp.(*InstallSnapshotReply)
			reply.Term = r.Term
			return
		}
	}
}
