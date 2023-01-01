package raft

import "time"

const EVENT_REQUEST_VOTE = "RequestVote"
const EVENT_APPEND_ENTRIES = "AppendEntries"
const EVENT_START_ELECTIONS = "StartElections"
const EVENT_END_ELECTIONS = "EndElections"
const EVENT_RESET_ELECTIONS = "ResetElections"
const EVENT_HEARTBEAT = "Heartbeat"
const EVENT_SHUTDOWN = "Shutdown"
const EVENT_SNAPSHOT = "Snapshot"
const EVENT_INSTALL_SNAPSHOT = "InstallSnapshot"

type Event struct {
	Name         string
	Payload      interface{}
	Response     chan interface{}
	CreatedAt    time.Time
	CreatedState int32
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
			rf.eventsHandlers[state][name] = rf.defaultHandler
		}
	}
}
