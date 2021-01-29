package kvraft

import (
	"../labrpc"
	"sync/atomic"
)
import "crypto/rand"
import "math/big"


type Clerk struct {
	servers []*labrpc.ClientEnd

	// You will have to modify this struct.
	id           int64
	counter      int64
	cachedLeader int
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

	// You'll have to add code here.
	ck.id = nrand()
	ck.counter = 0
	ck.cachedLeader = 0

	return ck
}

// Get
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
//
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	args := GetArgs{key, SerialNumber{ck.id, ck.counter}}
	ck.counter += 1
	DPrintf("[client_%v] started sending Get %v operation", ck.id, key)
	for {
		reply := GetReply{}
		if ok := ck.servers[ck.cachedLeader].Call("KVServer.Get", &args, &reply); ok {
			switch reply.Err {
			case OK:
				DPrintf("[client_%v] Get success - key: %v value: %v", ck.id, key, reply.Value)
				return reply.Value
			case ErrNoKey:
				DPrintf("[client_%v] Get error - key %v didn't exist", ck.id, key)
				return ""
			case ErrWrongLeader:
				DPrintf("[client_%v] Get error - wrong [leader_%v]", ck.id, ck.cachedLeader)
				ck.cachedLeader = ck.getNextLeader()
			default:
				DPrintf("[client_%v] Get error - unknown error %v", ck.id, reply.Err)
				ck.cachedLeader = ck.getNextLeader()
			}
		} else {
			DPrintf("[client_%v] Get error - rpc to [leader_%v] failed", ck.id, ck.cachedLeader)
			ck.cachedLeader = ck.getNextLeader()
		}
	}
}

func (ck *Clerk) getNextLeader() int {
	return (ck.cachedLeader + 1) % len(ck.servers)
}

// PutAppend
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	atomic.AddInt64(&ck.counter, 1)
	args := PutAppendArgs{key, value, op, SerialNumber{ck.id, ck.counter}}
	DPrintf("[client_%v] started sending %v operation with key: %v value: %v", ck.id, op, key, value)
	for {
		reply := PutAppendReply{}
		if ok := ck.servers[ck.cachedLeader].Call("KVServer.PutAppend", &args, &reply); ok {
			switch reply.Err {
			case OK:
				DPrintf("[client_%v] %v success - key: %v value: %v", ck.id, op, key, value)
				return
			case ErrNoKey:
				DPrintf("[client_%v] %v no key - key: %v value: %v", ck.id, op, key, value)
				return
			case ErrWrongLeader:
				DPrintf("[client_%v] %v error - wrong [leader_%v]", ck.id, op, ck.cachedLeader)
				ck.cachedLeader = ck.getNextLeader()
			}
		} else {
			DPrintf("[client_%v] %v error - rpc to [leader_%v] failed", ck.id, op, ck.cachedLeader)
			ck.cachedLeader = ck.getNextLeader()
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
