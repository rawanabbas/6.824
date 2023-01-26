package kvraft

import (
	"crypto/rand"
	"math/big"
	"sync/atomic"

	"6.824-2022/labrpc"
)

type Clerk struct {
	// You will have to modify this struct.
	servers   []*labrpc.ClientEnd
	id        atomic.Int64
	leaderId  atomic.Int64
	requestId atomic.Int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.id.Store(nrand())
	ck.leaderId.Store(0)
	ck.requestId.Store(0)
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	ck.requestId.Add(1)
	leader := ck.leaderId.Load()
	request := &GetRequest{
		Key:       key,
		ClientId:  ck.id.Load(),
		RequestId: ck.requestId.Load(),
	}
	ret := ""
	for ; ; leader = (leader + 1) % int64(len(ck.servers)) {
		reply := &GetReply{}
		ok := ck.servers[leader].Call("KVServer.Get", request, &reply)
		if ok {
			if reply.Err == OK {
				ret = reply.Value
				break
			} else if reply.Err == ErrNoKey {
				break

			}
		}
	}
	if ck.leaderId.Load() != leader {
		ck.leaderId.Store(leader)
	}
	return ret
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	ck.requestId.Add(1)
	leader := ck.leaderId.Load()
	var typ Operation
	if op == "Put" {
		typ = PutOp
	} else {
		typ = AppendOp
	}
	request := &PutAppendRequest{
		Key:       key,
		Value:     value,
		Op:        typ,
		ClientId:  ck.id.Load(),
		RequestId: ck.requestId.Load(),
	}
	for ; ; leader = (leader + 1) % int64(len(ck.servers)) {
		reply := &PutAppendReply{}
		ok := ck.servers[leader].Call("KVServer.PutAppend", request, &reply)
		if ok && reply.Err == OK {
			break
		}
	}
	if ck.leaderId.Load() != leader {
		ck.leaderId.Store(leader)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
