package raft

func (rf *Raft) defaultHandler(event *Event) {
	rf.Warn("Got an event %v while on state %v", event.Name, rf.getStateString())
	return
}

func (rf *Raft) handleStartElections(event *Event) {
	rf.voteForSelf()
	rf.broadcastVoteRequest()
}

func (rf *Raft) handleRequestVote(event *Event) {
	request := event.Payload.(*RequestVoteArgs)
	reply := &RequestVoteReply{}
	defer rf.persist()
	rf.Debug("handleRequestVote from %v, index: %v, term: %v", request.CandidateId, request.LastLogIndex, request.LastLogTerm)
	if rf.state.Load() == Candidate {
		rf.Debug("Handling Competing Votes")
	}

	if rf.state.Load() == Leader {
		rf.Debug("I am a leader but found a competing vote")
	}

	if request.Term < rf.currentTerm.Load() {
		rf.Debug("Outdated candidate requested vote rejecting...")
		reply.Term = rf.currentTerm.Load()
		reply.VoteGranted = false
		event.Response <- reply
		return
	}

	if request.Term > rf.currentTerm.Load() {
		rf.Debug("Stepping down to a follower")
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

func (rf *Raft) handleEndElections(event *Event) {
	win := event.Payload.(bool)
	if win {
		rf.Out("Stepping up as leader...")
		rf.setState(Leader)
		rf.resetVoltaileState()
		rf.sendHeartbeats()
		rf.startHearbeat()
	} else {
		rf.setVotedFor(-1)
		rf.stepDownToFollower(rf.currentTerm.Load())
	}
}

func (rf *Raft) handleAppendEntries(event *Event) {
	request := event.Payload.(*AppendEntriesRequest)
	rf.resetElectionTimer()
	defer rf.persist()
	reply := &AppendEntriesReply{}
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
	if request.PrevLogIndex > lastLogIndex || rf.getLogEntry(request.PrevLogIndex).Term != request.PrevLogTerm {
		event.Response <- reply
		return
	}

	j := 0
	i := request.PrevLogIndex + 1
	for ; i < lastLogIndex+1 && j < len(request.Entries); i, j = i+1, j+1 {
		// if rf.logs[i].Term != request.Entries[j].Term {
		if rf.getLogEntry(i).Term != request.Entries[j].Term {
			break
		}
	}
	rf.lock()
	rf.logs = rf.logs[:i]
	rf.unlock()
	rf.addLogEntry(request.Entries[j:]...)
	// rf.logs = append(rf.logs, request.Entries[j:]...)
	reply.Success = true
	if request.LeaderCommit > rf.commitIndex.Load() {
		mn := min(int(request.LeaderCommit), rf.getLastLogIndex())
		rf.setCommitIndex(int32(mn))
		rf.applyLogs()
	}
	rf.persist()
	event.Response <- reply
}

func (rf *Raft) handleHeartbeats(event *Event) {
	rf.sendHeartbeats()
}

func (rf *Raft) handleShutdown(event *Event) {
	rf.dead.Store(1)
}
