package raft

func (rf *Raft) defaultHandler(event *Event) {
	rf.Warn("Got an event %v while on state %v", event.Name, rf.GetStateString())
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
	if len(request.Entries) > 0 || request.LeaderCommit > rf.commitIndex.Load() {
	}
	rf.resetElectionTimer()
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
	if request.PrevLogIndex > lastLogIndex {
		rf.lock()
		entryExists := (request.PrevLogIndex - int(rf.lastSnapshottedIndex.Load())) < len(rf.logs)
		rf.unlock()
		if !entryExists || (request.PrevLogIndex > (int(rf.lastSnapshottedIndex.Load())-1) && rf.getLogEntry(request.PrevLogIndex).Term != request.PrevLogTerm) || (rf.lastSnapshottedTerm.Load() != request.PrevLogTerm) {
			event.Response <- reply
			return
		}
	}

	if len(request.Entries) > 0 && request.Entries[len(request.Entries)-1].Index <= rf.lastSnapshottedIndex.Load() {
		reply.Success = true
		event.Response <- reply
		return
	}

	j := 0
	i := request.PrevLogIndex + 1
	lastSnapIdx := int(rf.lastSnapshottedIndex.Load())
	for ; i < lastLogIndex+1 && j < len(request.Entries); i, j = i+1, j+1 {
		if i < lastSnapIdx {
			continue
		}
		if rf.getLogEntry(i).Term != request.Entries[j].Term {
			rf.lock()
			idx := i - int(rf.lastSnapshottedIndex.Load())
			rf.logs = rf.logs[:idx]
			rf.unlock()
			break
		}
	}

	// rf.lock()
	// idx := i - int(rf.lastSnapshottedIndex.Load())
	// rf.logs = rf.logs[:idx]
	// rf.unlock()
	rf.addLogEntry(request.Entries[j:]...)
	reply.Success = true
	if request.LeaderCommit > rf.commitIndex.Load() {
		mn := min(int(request.LeaderCommit), rf.getLastLogIndex())
		rf.setCommitIndex(int32(mn))
		rf.applyLogs()
	}
	rf.persist()
	event.Response <- reply
}

func (rf *Raft) handleInstallSnapshot(event *Event) {
	request := event.Payload.(*InstallSnapshotRequest)
	reply := &InstallSnapshotReply{}
	reply.Term = rf.currentTerm.Load()
	rf.resetElectionTimer()
	if request.Term < rf.currentTerm.Load() {
		event.Response <- reply
		return
	}
	idx := -1
	rf.lock()
	for i, entry := range rf.logs {
		if entry.Index == int32(request.LastIncludedIndex) && entry.Term == request.LastIncludedTerm {
			idx = i
			break
		}
	}
	if idx > -1 {
		rf.logs = rf.logs[idx+1:]
	} else {
		rf.logs = []LogEntry{}
		rf.lastApplied.Store(int32(request.LastIncludedIndex) - 1)
	}
	rf.lastSnapshottedIndex.Store(int32(request.LastIncludedIndex))
	rf.lastSnapshottedTerm.Store(request.LastIncludedTerm)
	event.Response <- reply
	rf.unlock()
	state, _ := rf.generatePersistantState()
	rf.persister.SaveStateAndSnapshot(state, request.Snapshot)
	rf.applySnap(request.Snapshot)
}

func (rf *Raft) handleHeartbeats(event *Event) {
	rf.sendHeartbeats()
}

func (rf *Raft) handleShutdown(event *Event) {
	rf.dead.Store(1)
}
