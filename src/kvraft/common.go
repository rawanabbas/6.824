package kvraft

type Operation int

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeout     = "ErrTimeout"
)

const (
	GetOp    Operation = 0
	AppendOp Operation = 1
	PutOp    Operation = 2
)

type Err string

// Put or Append
type PutAppendRequest struct {
	Key       string
	Value     string
	ClientId  int64
	RequestId int64
	Op        Operation
}

type PutAppendReply struct {
	Err Err
}

type GetRequest struct {
	Key string
	// You'll have to add definitions here.
	ClientId  int64
	RequestId int64
}

type GetReply struct {
	Err   Err
	Value string
}
