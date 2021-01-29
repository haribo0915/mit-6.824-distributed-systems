package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string
type SerialNumber struct {
	ClientId int64
	Counter int64
}

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	SerialNumber SerialNumber
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string

	// You'll have to add definitions here.
	SerialNumber SerialNumber
}

type GetReply struct {
	Err   Err
	Value string
}
