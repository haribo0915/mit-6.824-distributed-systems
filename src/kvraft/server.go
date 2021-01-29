package kvraft

import (
	"../labgob"
	"../labrpc"
	"../raft"
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = 0
const (
	GET    = "Get"
	PUT    = "Put"
	APPEND = "Append"
)
const OperationTimeoutInMillis = 700

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}


type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Code  string
	Key   string
	Value string
	SerialNumber SerialNumber
}

type Result struct {
	Value string
	Err Err
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	storage                  map[string]string
	clientToOperationCounter map[int64]int64
	commandIndexToResult     map[int]chan Result
	shutdown                 chan bool
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{GET,args.Key,"",args.SerialNumber}

	if commandIndex, _, isLeader := kv.rf.Start(op); isLeader {
		result := make(chan Result)
		kv.insertResultChannel(commandIndex, result)
		select {
		case <-time.After(time.Duration(OperationTimeoutInMillis) * time.Millisecond):
			reply.Err = ErrWrongLeader
		case res := <-result:
			reply.Err = res.Err
			reply.Value = res.Value
			DPrintf("[server_%v] Got key: %v value: %v", kv.me, args.Key, res.Value)
		}
		kv.removeResultChannel(commandIndex)
	} else {
		reply.Err = ErrWrongLeader
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{PUT,args.Key,args.Value,args.SerialNumber}
	switch args.Op {
	case PUT:
		op.Code = PUT
	case APPEND:
		op.Code = APPEND
	default:
		DPrintf("[server_%v] unknown op code %v in PutAppend arguments", kv.me, args.Op)
	}

	if commandIndex, _, isLeader := kv.rf.Start(op); isLeader {
		result := make(chan Result)
		kv.insertResultChannel(commandIndex, result)
		select {
		case <-time.After(time.Duration(OperationTimeoutInMillis) * time.Millisecond):
			reply.Err = ErrWrongLeader
		case res := <-result:
			reply.Err = res.Err
		}
		kv.removeResultChannel(commandIndex)
	} else {
		reply.Err = ErrWrongLeader
	}
}

func (kv *KVServer) insertResultChannel(commandIndex int, result chan Result) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.commandIndexToResult[commandIndex] = result
}

func (kv *KVServer) getResultChannel(commandIndex int) (result chan Result, find bool) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	result, find = kv.commandIndexToResult[commandIndex]
	return
}

func (kv *KVServer) removeResultChannel(commandIndex int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	delete(kv.commandIndexToResult, commandIndex)
}

func (kv *KVServer) run() {
	for {
		select {
		case applyMsg := <-kv.applyCh:
			if applyMsg.CommandValid && applyMsg.Command != nil {
				op := applyMsg.Command.(Op)
				res := kv.doOperation(&op)
				go kv.saveSnapshot()
				if kv.isLeader() {
					if resultChan, ok := kv.getResultChannel(applyMsg.CommandIndex); ok {
						resultChan <- res
					}
					kv.removeResultChannel(applyMsg.CommandIndex)
				}
			} else {
				kv.readSnapshot()
			}
		case <-kv.shutdown:
			return
		}
	}
}

func (kv *KVServer) doOperation(op *Op) Result {
	result := Result{Err: OK}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	switch op.Code {
	case GET:
		if value, ok := kv.storage[op.Key]; ok {
			result.Value = value
		} else {
			result.Err = ErrNoKey
		}
	case PUT:
		prevCounter, find :=  kv.clientToOperationCounter[op.SerialNumber.ClientId]
		if !find || op.SerialNumber.Counter > prevCounter {
			kv.storage[op.Key] = op.Value
			kv.clientToOperationCounter[op.SerialNumber.ClientId] = op.SerialNumber.Counter
		}
	case APPEND:
		prevCounter, find :=  kv.clientToOperationCounter[op.SerialNumber.ClientId]
		if !find || op.SerialNumber.Counter > prevCounter {
			if _, ok := kv.storage[op.Key]; ok {
				kv.storage[op.Key] += op.Value
			} else {
				kv.storage[op.Key] = op.Value
			}
			kv.clientToOperationCounter[op.SerialNumber.ClientId] = op.SerialNumber.Counter
		}
	default:
		DPrintf("[server_%v] error - unknown opcode", kv.me)
	}

	return result
}

func (kv *KVServer) isLeader() bool {
	_, isLeader := kv.rf.GetState()
	return isLeader
}

func (kv* KVServer) saveSnapshot() {
	if kv.maxraftstate != -1 && kv.rf.RaftStateSize() >= kv.maxraftstate {
		kv.mu.Lock()
		defer kv.mu.Unlock()
		w := new(bytes.Buffer)
		e := labgob.NewEncoder(w)

		// Snapshot.
		e.Encode(kv.storage)
		e.Encode(kv.clientToOperationCounter)
		kv.rf.PersistStateAndSnapshot(w.Bytes(), true)
	}
}

func (kv* KVServer) readSnapshot() {
	data := kv.rf.ReadKVSnapshot()
	if data == nil || len(data) < 1 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var storage map[string]string
	var clientToOperationCounter map[int64]int64
	d.Decode(&storage)
	d.Decode(&clientToOperationCounter)
	kv.mu.Lock()
	kv.mu.Unlock()
	kv.storage = storage
	kv.clientToOperationCounter = clientToOperationCounter
}

// Kill
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
	kv.shutdown  <- true
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// StartKVServer
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
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.storage = make(map[string]string)
	kv.clientToOperationCounter = make(map[int64]int64)
	// TODO: not sure why cannot use operation serial number as key.
	kv.commandIndexToResult = make(map[int]chan Result)
	kv.shutdown = make(chan bool)
	kv.readSnapshot()
	go kv.run()

	return kv
}
