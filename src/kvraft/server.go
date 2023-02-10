package kvraft

import (
	"bytes"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"6.824-2022/labgob"
	"6.824-2022/labrpc"
	"6.824-2022/logger"
	"6.824-2022/raft"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type      Operation
	Key       string
	Value     string
	ClientId  int64
	RequestId int64
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big
	// Your definitions here.
	db          map[string]string
	responses   map[int]chan Op
	snapshot    chan int
	lastRequest map[int64]int64
}

func (kv *KVServer) Get(request *GetRequest, reply *GetReply) {
	// Your code here.
	kv.Debug("------------------------Getting...")
	op := Op{
		Type:      GetOp,
		Key:       request.Key,
		ClientId:  request.ClientId,
		RequestId: request.RequestId,
	}
	kv.Debug("Sending Op...")
	err, appliedOp := kv.sendToStateMachine(op)
	if err != OK {
		reply.Err = err
		return
	}
	kv.Debug("Checking if same Op")
	if kv.isEqualOp(*appliedOp, op) {
		kv.lock()
		val, ok := kv.db[op.Key]
		if !ok {
			reply.Err = ErrNoKey
			kv.unlock()
			kv.Error("No Key -{%v}- Found", op.Key)
			kv.Debug("No Key Found Returning")
			return
		}
		reply.Value = val
		kv.Debug("Fetched Value")
		kv.unlock()
		kv.Debug("Returning")
		reply.Err = OK
		return
	}
	// }
}

func (kv *KVServer) PutAppend(request *PutAppendRequest, reply *PutAppendReply) {
	// Your code here
	kv.lock()
	val, ok := kv.lastRequest[request.ClientId]
	kv.unlock()
	if ok && val >= request.RequestId {
		reply.Err = OK
		return
	}
	if request.Op == PutOp {
		kv.Put(request, reply)
	} else {
		kv.Append(request, reply)
	}
}

func (kv *KVServer) Put(request *PutAppendRequest, reply *PutAppendReply) {
	// Your code here.
	kv.Debug("------------------------Putting.... %v=%v", request.Key, request.Value)
	op := Op{
		Type:      PutOp,
		Key:       request.Key,
		Value:     request.Value,
		ClientId:  request.ClientId,
		RequestId: request.RequestId,
	}
	err, appliedOp := kv.sendToStateMachine(op)
	if err != OK {
		reply.Err = err
		return
	}
	if kv.isEqualOp(*appliedOp, op) {
		reply.Err = OK
		return
	}
	reply.Err = ErrNoKey
}

func (kv *KVServer) Append(request *PutAppendRequest, reply *PutAppendReply) {
	// Your code here.
	kv.Debug("------------------------Appending... %v", request)
	op := Op{
		Type:      AppendOp,
		Key:       request.Key,
		Value:     request.Value,
		ClientId:  request.ClientId,
		RequestId: request.RequestId,
	}
	err, appliedOp := kv.sendToStateMachine(op)
	if err != OK {
		reply.Err = err
		return
	}
	if kv.isEqualOp(*appliedOp, op) {
		reply.Err = OK
		return
	}
}

func (kv *KVServer) isEqualOp(op1 Op, op2 Op) bool {
	return op1.ClientId == op2.ClientId && op1.RequestId == op2.RequestId
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	kv.rf.Kill()
	kv.Debug("Waiting for Raft to be killed")
	for !kv.rf.Killed() {
		time.Sleep(50 * time.Millisecond)
	}
	kv.Debug("Raft killed")
	atomic.StoreInt32(&kv.dead, 1)
	kv.Debug("Waiting for KV server to be killed")
	for !kv.killed() {
		time.Sleep(50 * time.Millisecond)
	}
	kv.Debug("KV server killed")
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) lock() {
	kv.Debug("Locking....")
	kv.mu.Lock()
}

func (kv *KVServer) unlock() {
	kv.Debug("Unlocking...")
	kv.mu.Unlock()
}

func (kv *KVServer) applier(persister *raft.Persister) {
	for !kv.killed() {
		kv.Debug("Waiting for MSG on ApplyChannel")
		msg := <-kv.applyCh
		kv.Debug("Received %v on ApplyChannel", msg)
		if msg.CommandValid {
			op := msg.Command.(Op)
			kv.Debug("Sending on Channel Index %v", msg.CommandIndex)

			kv.lock()
			rid, ok := kv.lastRequest[op.ClientId]
			ch, hasChannel := kv.responses[msg.CommandIndex]
			if !(ok && rid >= op.RequestId) {
				// It is a new request
				kv.lastRequest[op.ClientId] = op.RequestId
				switch op.Type {
				case PutOp:
					kv.db[op.Key] = op.Value
				case AppendOp:
					kv.db[op.Key] += op.Value
				case GetOp:
					op.Value = kv.db[op.Key]
				}
			}
			kv.unlock()
			if hasChannel {
				go func(ch chan Op) {
					ch <- op
				}(ch)
			} else {
				kv.Debug("cant send, no channel")
			}
			if kv.maxraftstate >= 0 && persister.RaftStateSize() >= kv.maxraftstate {
				kv.Debug("Snapshotting!! %v %v --- %v", msg.CommandIndex, persister.RaftStateSize(), kv.maxraftstate)
				// kv.snapshot <- msg.CommandIndex
				kv.lock()

				kv.Debug("Sent snap %v", msg.CommandIndex)
				data, err := kv.generateSnapshot()
				if err != nil {
					kv.Error("An error has occured while generating the snapshot!")
				}
				kv.Debug("Calling snap %v", msg.CommandIndex)
				kv.rf.Snapshot(msg.CommandIndex, data)
				kv.Debug("Called snap %v", msg.CommandIndex)
				kv.unlock()
			}
		} else if msg.SnapshotValid {
			// TODO:
			kv.readSnapshot(msg.Snapshot)
		}
	}
	kv.Debug("Killed")
}

func (kv *KVServer) snapshotter() {
	if kv.maxraftstate < 0 {
		return
	}
	for !kv.killed() {
		kv.Debug("=========waiting for kv.snapshot")
		index := <-kv.snapshot
		kv.Debug("=========recv'd %v", index)
		data, err := kv.generateSnapshot()
		if err != nil {
			kv.Error("An error has occured while generating the snapshot!")
		}
		kv.rf.Snapshot(index, data)
		kv.Debug("finish snap %v", index)
	}
}

func (kv *KVServer) sendToStateMachine(op Op) (Err, *Op) {
	kv.Debug("Sending Op.... %v", op)
	index, _, isLeader := kv.rf.Start(op)
	kv.Debug("Op Sent %v", index)
	if !isLeader {
		kv.Debug("BUT Not Leader")
		return ErrWrongLeader, nil
	}

	kv.Debug("Creating Channel on Index: %v", index)
	kv.lock()
	_, ok := kv.responses[index]
	if !ok {
		kv.responses[index] = make(chan Op)
	}
	ch := kv.responses[index]
	kv.unlock()
	kv.Debug("Waiting for application %v  -  %v", index, op.RequestId)
	select {
	case appliedOp := <-ch:
		kv.Debug("Applied %v  -   %v", index, appliedOp.RequestId)
		return OK, &appliedOp
	case <-time.After(1 * time.Second):
		kv.Debug("timedout %v  -   %v", index, op.RequestId)
		return ErrTimeout, nil
	}

}

func (kv *KVServer) generateSnapshot() ([]byte, error) {
	// kv.lock()
	// defer kv.unlock()
	buff := new(bytes.Buffer)
	enc := labgob.NewEncoder(buff)
	if enc.Encode(kv.db) != nil || enc.Encode(kv.lastRequest) != nil {
		return nil, fmt.Errorf("cannot generate snapshot")
	}
	return buff.Bytes(), nil
}

func (kv *KVServer) readSnapshot(data []byte) {
	kv.lock()
	defer kv.unlock()

	if data == nil || len(data) < 1 {
		return
	}

	buff := bytes.NewBuffer(data)
	dec := labgob.NewDecoder(buff)
	var db map[string]string
	var lastRequest map[int64]int64

	if dec.Decode(&db) != nil || dec.Decode(&lastRequest) != nil {
		kv.Error("Failed to recover snapshot data")
		return
	}

	kv.db = db
	kv.lastRequest = lastRequest

}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	logger.SetDebug(true)
	labgob.Register(Op{})

	SetLoggerPrefixes()

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.responses = make(map[int]chan Op)
	kv.snapshot = make(chan int)
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.db = make(map[string]string)
	kv.lastRequest = make(map[int64]int64)

	kv.readSnapshot(persister.ReadSnapshot())

	go kv.applier(persister)
	// go kv.snapshotter()
	// You may need initialization code here.
	return kv
}
