package raft

import (
	"bytes"

	"6.824-2022/labgob"
)

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

func (rf *Raft) handleSnaphshot(event *Event) {
	evtPayload := event.Payload.(*SnapshotPayload)
	index := evtPayload.Index
	snapshot := evtPayload.Snapshot
	// Your code here (2D).
	rf.Debug("------------ Snapshot: %v", index)
	rf.snapLock()
	rf.lock()
	rf.Debug("------------ Snapshot: %v [%v](%v)\n", index, len(rf.logs), rf.lastSnapshottedIndex.Load())
	defer rf.unlock()
	if index <= int(rf.lastSnapshottedIndex.Load())-1 {
		rf.Debug("Index %v is already snapshotted, already at %v", index, rf.lastSnapshottedIndex.Load())
		rf.snapUnlock()
		return
	}
	arrIdx := index - int(rf.lastSnapshottedIndex.Load())
	rf.Debug("------------ Snapshot: %v [%v](%v) arrIdx: %v\n", index, len(rf.logs), rf.lastSnapshottedIndex.Load(), arrIdx)
	rf.lastSnapshottedIndex.Store(int32(index) + 1)
	rf.lastSnapshottedTerm.Store(rf.logs[arrIdx].Term)
	rf.logs = rf.logs[arrIdx+1:]
	rf.snapUnlock()
	buff := new(bytes.Buffer)
	enc := labgob.NewEncoder(buff)
	if enc.Encode(rf.currentTerm.Load()) != nil || enc.Encode(rf.votedFor.Load()) != nil || enc.Encode(rf.logs) != nil || enc.Encode(rf.lastSnapshottedIndex.Load()) != nil || enc.Encode(rf.lastSnapshottedTerm.Load()) != nil {
		rf.Error("Failed to encode raft state")
		return
	}
	state := buff.Bytes()
	rf.persister.SaveStateAndSnapshot(state, snapshot)
}

func (rf *Raft) handleAppendEntries(event *Event) {
	request := event.Payload.(*AppendEntriesRequest)
	rf.Debug("handleAppendEntries: %v", len(request.Entries))
	rf.lock()
	rf.Debug("-----handleAppendEntries Server: %v Logs: %v", rf.me, rf.logs)
	rf.unlock()
	defer func() {
		rf.Debug("End of handleAppendEntries: %v", len(request.Entries))
	}()
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
	rf.Debug("===================Prev Log Index: %v, Prev log term: %v, Last Log Index: %v LSI: %v", request.PrevLogIndex, request.PrevLogTerm, lastLogIndex, rf.lastSnapshottedIndex.Load())
	var prevTerm int32
	if request.PrevLogIndex <= lastLogIndex {
		rf.lock()
		entryExists := (request.PrevLogIndex - int(rf.lastSnapshottedIndex.Load())) < len(rf.logs)
		rf.unlock()
		if !entryExists || (request.PrevLogIndex > (int(rf.lastSnapshottedIndex.Load())-1) && rf.getLogEntry(request.PrevLogIndex).Term != request.PrevLogTerm) || ((request.PrevLogIndex == (int(rf.lastSnapshottedIndex.Load()) - 1)) && rf.lastSnapshottedTerm.Load() != request.PrevLogTerm) {
			if request.PrevLogIndex <= int(rf.lastSnapshottedIndex.Load()) {
				rf.Debug("Sending previous term as %v [1]", rf.lastSnapshottedTerm.Load())
				prevTerm = rf.lastSnapshottedTerm.Load()
			} else {
				rf.Debug("entry exists: %v,  rf.getLogEntry(request.PrevLogIndex).Term != request.PrevLogTerm: %v, request.PrevLogIndex == (int(rf.lastSnapshottedIndex.Load()) - 1): %v, rf.lastSnapshottedTerm.Load() != request.PrevLogTerm: %v", entryExists, rf.getLogEntry(request.PrevLogIndex).Term != request.PrevLogTerm, request.PrevLogIndex == (int(rf.lastSnapshottedIndex.Load())-1), rf.lastSnapshottedTerm.Load() != request.PrevLogTerm)
				rf.Debug("Sending previous term as %v=%v/T%v [2] %v", lastLogIndex, lastLogIndex, rf.getLastLogTerm(), rf.getLogEntry(lastLogIndex))
				prevTerm = rf.getLastLogTerm()
			}
			reply.XTerm = prevTerm
			for i := request.PrevLogIndex; i >= int(rf.lastSnapshottedIndex.Load()); i-- {
				entry := rf.getLogEntry(i)
				reply.XIndex = int(entry.Index)
				if entry.Term != prevTerm {
					break
				}
			}
			event.Response <- reply
			return
		}
	} else {
		// Log too short
		reply.XTerm = -1
		rf.Debug("Sending previous term as %v/T-1 [3]", lastLogIndex+1)
		reply.XIndex = lastLogIndex + 1
		event.Response <- reply
		return
	}
	if len(request.Entries) > 0 {
		rf.Debug("Not a Heartbeat, last SI %v, last entry: %v", rf.lastSnapshottedIndex.Load(), request.Entries[len(request.Entries)-1].Index)
	}

	if len(request.Entries) > 0 && request.Entries[len(request.Entries)-1].Index < rf.lastSnapshottedIndex.Load()-1 {
		rf.Debug("Entries already part of snapshot")
		reply.Success = true
		event.Response <- reply
		return
	}

	rf.lock()
	logs := rf.logs
	rf.unlock()
	rf.snapLock()
	// if len(logs) > 0 {
	// 	rf.Debug("xxx6-2 %v -- %v", logs[len(logs)-1].Index, logs)
	// } else {
	// 	rf.Debug("xxx6-2b captured logs are empty...")
	// }
	j := 0
	i := request.PrevLogIndex + 1
	lastSnapIdx := int(rf.lastSnapshottedIndex.Load())
	// rf.Debug("xxx7")
	for ; i-lastSnapIdx < len(logs) && j < len(request.Entries); i, j = i+1, j+1 {
		if i < lastSnapIdx {
			rf.Debug("i < lastSnapIdx (%v < %v), continue", i, lastSnapIdx)
			continue
		}
		rf.Debug("I: %v LSIL: %v LSI: %v LSIDIFF: %v LSZ: %v", i, rf.lastSnapshottedIndex.Load(), lastSnapIdx, i-lastSnapIdx, len(logs))
		if logs[i-lastSnapIdx].Term != request.Entries[j].Term {
			rf.lock()
			idx := i - int(rf.lastSnapshottedIndex.Load())
			rf.logs = rf.logs[:idx]
			rf.unlock()
			break
		}
	}
	// rf.Debug("xxx8")
	rf.snapUnlock()
	rf.Debug("xxx9 %v", request.Entries[j:])
	rf.addLogEntry(request.Entries[j:]...)
	// rf.Debug("xxx11 %v   %v", request.LeaderCommit, rf.commitIndex.Load())
	reply.Success = true
	if request.LeaderCommit > rf.commitIndex.Load() {
		mn := min(int(request.LeaderCommit), rf.getLastLogIndex())
		rf.setCommitIndex(int32(mn))
		rf.Debug("applying logs %v, latest entries: %v", mn, request.Entries)
		go rf.applyLogs()
		rf.Debug("applied logs")
	}
	rf.persist()
	rf.Debug("Sending success!")
	event.Response <- reply
}

func (rf *Raft) handleInstallSnapshot(event *Event) {
	request := event.Payload.(*InstallSnapshotRequest)
	rf.Debug("handleInstallSnapshot Idx: %v, Term: %v", request.LastIncludedIndex, request.LastIncludedTerm)
	reply := &InstallSnapshotReply{}
	reply.Term = rf.currentTerm.Load()
	rf.resetElectionTimer()
	if request.Term < rf.currentTerm.Load() {
		event.Response <- reply
		return
	}

	if request.Term > rf.currentTerm.Load() {
		rf.stepDownToFollower(request.Term)
	}

	idx := -1
	rf.snapLock()
	rf.lock()
	if request.LastIncludedIndex <= int(rf.lastSnapshottedIndex.Load())-1 {
		rf.Debug("Index %v is already snapshotted, already at %v", request.LastIncludedIndex, rf.lastSnapshottedIndex.Load())
		rf.unlock()
		rf.snapUnlock()
		return
	}
	for i, entry := range rf.logs {
		if entry.Index == int32(request.LastIncludedIndex) && entry.Term == request.LastIncludedTerm {
			idx = i
			break
		}
	}
	if idx > -1 {
		rf.logs = rf.logs[idx:]
	} else {
		rf.logs = []LogEntry{}
	}
	rf.lastSnapshottedIndex.Store(int32(request.LastIncludedIndex))
	rf.lastSnapshottedTerm.Store(request.LastIncludedTerm)
	rf.lastApplied.Store(int32(request.LastIncludedIndex) - 1)
	rf.setCommitIndex(int32(max(request.LastIncludedIndex-1, int(rf.commitIndex.Load()))))
	event.Response <- reply
	rf.unlock()
	rf.snapUnlock()
	state, _ := rf.generatePersistantState()
	rf.persister.SaveStateAndSnapshot(state, request.Snapshot)
	rf.Debug("go applySnap, last si: %v , snap: %v  %v|%v", rf.lastSnapshottedIndex.Load(), request.Snapshot, request.LastIncludedTerm, request.LastIncludedTerm)
	go rf.applySnap(request.Snapshot)
}

func (rf *Raft) handleHeartbeats(event *Event) {
	rf.sendHeartbeats()
}

func (rf *Raft) handleShutdown(event *Event) {
	rf.dead.Store(1)
}
