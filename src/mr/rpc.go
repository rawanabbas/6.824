package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

const TASK_REQUEST = 0
const REDUCE_FINISH = 1
const MAP_FINISH = 2
const MAP = 3
const REDUCE = 4

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

type ConnectionRequest struct {
	Addr string
}

type ConnectionReply struct {
	Success bool
}

type PingRequest struct {
}

type PingResponse struct {
	Success bool
}

// Add your RPC definitions here.
type UpdateStatusRequest struct {
	Type int
	Addr string
	Task string
}

type UpdateStatusReply struct {
	Filename       string
	NReduce        int
	TaskType       int
	ReduceFileList []string
	TaskNumber     int
}

type IntermediateFileRequest struct {
	Filename   string
	TaskNumber int
}

type IntermediateFileReply struct {
	Success bool
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
