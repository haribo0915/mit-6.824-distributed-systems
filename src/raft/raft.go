package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isLeader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"math/rand"
	"sync"
	"time"
)
import "sync/atomic"
import "../labrpc"

import "../labgob"

// ApplyMsg
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

const MinimumElectionTimeoutInMillis = 300
const MaximumElectionTimeoutInMillis = 600

type State int

const (
	Leader State = iota
	Candidate
	Follower
	Shutdown
)

// Raft
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm           int
	votedFor              int
	log                   []Entry
	commitIndex           int
	lastApplied           int
	nextIndex             []int
	matchIndex            []int
	currentState          State
	applyCh               chan ApplyMsg

	lastHeartBeat 		  time.Time

	raftMsgChan                         chan interface{}
	isReadyToApplyEntriesToStateMachine chan bool
	shutdown                            chan bool
}

// GetState return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isLeader bool

	// Your code here (2A).
	term = rf.currentTerm
	isLeader = rf.currentState == Leader

	return term, isLeader
}

func (rf *Raft) setTargetState(targetState State) {
	rf.currentState = targetState
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}


//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []Entry
	d.Decode(&currentTerm)
	d.Decode(&votedFor)
	d.Decode(&log)
	rf.currentTerm = currentTerm
	rf.votedFor = votedFor
	rf.log = log
}

// RequestVoteArgs
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term int
	CandidateId int
	LastLogIndex int
	LastLogTerm int
}

// RequestVoteReply
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term int
	LeaderId int
	PrevLogIndex int
	PrevLogTerm int
	Entries []Entry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term int
	Success bool
	ConflictOpt ConflictOpt
}

type Entry struct {
	Term int
	Index int
	Command interface{}
}

type ConflictOpt struct {
	Term int
	Index int
}

type RequestVoteMsg struct {
	rpc *RequestVoteArgs
	done chan<- RequestVoteReply
}

type AppendEntriesMsg struct {
	rpc *AppendEntriesArgs
	done chan<- AppendEntriesReply
}

// RequestVote
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	done := make(chan RequestVoteReply, 1)
	rf.raftMsgChan <- RequestVoteMsg{args, done}
	// Copy data.
	r := <-done
	reply.Term = r.Term
	reply.VoteGranted = r.VoteGranted
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).
	done := make(chan AppendEntriesReply, 1)
	rf.raftMsgChan <- AppendEntriesMsg{args, done}
	r := <-done
	// Copy data.
	reply.Term = r.Term
	reply.Success = r.Success
	reply.ConflictOpt = r.ConflictOpt
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf* Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// Start
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	if rf.currentState == Leader {
		entry := Entry{rf.currentTerm, rf.getLastLogIndex() + 1, command}
		index = entry.Index
		term = entry.Term
		rf.log = append(rf.log, entry)
		rf.persist()
		DPrintf("[peer_%v] started reaching agreement on command_%v", rf.me, index)
	} else {
		isLeader = false
	}

	return index, term, isLeader
}

// Kill
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	rf.shutdown <- true
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) getNextElectionTimeout() int {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	return MinimumElectionTimeoutInMillis + r.Intn(MaximumElectionTimeoutInMillis-MinimumElectionTimeoutInMillis)
}

func (rf *Raft) updateCurrentTerm(newTerm int, votedFor int) {
	rf.currentTerm = newTerm
	rf.votedFor = votedFor
}

func (rf *Raft) revertToFollower(newTerm int, votedFor int) {
	rf.updateCurrentTerm(newTerm, votedFor)
	rf.setTargetState(Follower)
	rf.persist()
}

func (rf *Raft) handleRaftMsg(msg interface{}) {
	switch msg.(type) {
	case RequestVoteMsg:
		msg := msg.(RequestVoteMsg)
		msg.done <- rf.handleVoteRequest(msg.rpc)
	case AppendEntriesMsg:
		msg := msg.(AppendEntriesMsg)
		msg.done <- rf.handleAppendEntriesRequest(msg.rpc)
	default:
		DPrintf("[peer_%v] message type error", rf.me)
	}
}

func (rf *Raft) handleVoteRequest(rpc *RequestVoteArgs) RequestVoteReply {
	// If candidate's current term is less than this peer's current term, reject.
	if rpc.Term < rf.currentTerm {
		return RequestVoteReply{rf.currentTerm, false}
	}

	// If we observe a term greater than our own outside of the election timeout
	// minimum, then we must update term & immediately become follower. We still need to
	// do vote checking after this.
	if rpc.Term > rf.currentTerm {
		rf.revertToFollower(rpc.Term, -1)
	}

	// Do not respond to the request if we've received a heartbeat within the election timeout minimum.
	// We believe the leader still exists.
	delta := time.Since(rf.lastHeartBeat)
	if delta <= time.Millisecond * MinimumElectionTimeoutInMillis {
		return RequestVoteReply{rf.currentTerm, false}
	}

	// Check if candidate's log is at least as up-to-date as this peer's.
	// If candidate's log is not at least as up-to-date as this peer, then reject.
	isClientUpToDate := rpc.LastLogTerm > rf.getLastLogTerm() || (rpc.LastLogTerm == rf.getLastLogTerm() && rpc.LastLogIndex >= rf.getLastLogIndex())
	if !isClientUpToDate {
		return RequestVoteReply{rf.currentTerm, false}
	}

	// Candidate's log is up-to-date so handle voting conditions.
	switch rf.votedFor {
	// This peer has already voted for the candidate.
	case rpc.CandidateId:
		rf.lastHeartBeat = time.Now()
		return RequestVoteReply{rf.currentTerm, true}
	// This peer has not yet voted for the current term, so vote for the candidate.
	case -1:
		rf.lastHeartBeat = time.Now()
		rf.votedFor = rpc.CandidateId
		rf.revertToFollower(rpc.Term, rf.votedFor)
		rf.persist()
		return RequestVoteReply{rf.currentTerm, true}
	// This peer has already voted for a different candidate.
	default:
		return RequestVoteReply{rf.currentTerm, false}
	}
}

func (rf *Raft) handleAppendEntriesRequest(rpc *AppendEntriesArgs) AppendEntriesReply {
	reply := AppendEntriesReply{rf.currentTerm, false, ConflictOpt{0, 0}}

	// If candidate's current term is less than this peer's current term, reject.
	if rpc.Term < rf.currentTerm {
		return reply
	}

	if rpc.Term > rf.currentTerm {
		rf.revertToFollower(rpc.Term, -1)
	}

	// Start log matching process.
	entry, foundEntryAtPrevLogIndex := rf.getEntryAtIndex(rpc.PrevLogIndex)
	matched := foundEntryAtPrevLogIndex && entry.Term == rpc.PrevLogTerm

	if matched {
		reply.Success = true
		rf.lastHeartBeat = time.Now()

		posOfFirstUnmatchedEntry := 0
		for ; posOfFirstUnmatchedEntry < len(rpc.Entries); posOfFirstUnmatchedEntry++ {
			if entry, ok := rf.getEntryAtIndex(rpc.Entries[posOfFirstUnmatchedEntry].Index); ok {
				// If existing entry conflicts with a new one (same index but different terms),
				// delete the existing entry and all that follow it.
				if rpc.Entries[posOfFirstUnmatchedEntry].Term != entry.Term {
					DPrintf("[peer_%v] CONFLICT %v, [peer_%v]: %v", rf.me, rf.log, rpc.LeaderId, rpc.Entries)
					rf.log = rf.log[:entry.Index] // truncate
					break
				}
			} else {
				// Log was empty starting from posOfFirstUnmatchedEntry.
				break
			}
		}
		// If entries are empty, AppendEntriesRequest just serves as heartbeat.
		if len(rpc.Entries) != 0 {
			rf.log = append(rf.log, rpc.Entries[posOfFirstUnmatchedEntry:]...)
			rf.persist()
		}

		// If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry).
		if rpc.LeaderCommit > rf.commitIndex {
			if rf.getLastLogIndex() < rpc.LeaderCommit {
				rf.commitIndex = rf.getLastLogIndex()
			} else {
				rf.commitIndex = rpc.LeaderCommit
			}
			// Block until raft applies entries to state machine.
			rf.isReadyToApplyEntriesToStateMachine <- true
		}

		return reply
	}

	// Existing entry doesn't match leader's entry at rpc.PrevLogIndex.
	if foundEntryAtPrevLogIndex {
		// Conflict optimization: use the foremost index of entries with conflicting term as next index.
		reply.ConflictOpt.Term = entry.Term
		if index, ok := rf.getFirstIndexWithTermFrom(entry.Index, entry.Term); ok {
			reply.ConflictOpt.Index = index
		}
	} else {
		// If rpc.PrevLogIndex < 0 or rpc.PrevLogIndex >= len(rf.log), set ConflictOpt to the last entry.
		reply.ConflictOpt.Term = rf.getLastLogTerm()
		reply.ConflictOpt.Index = rf.getLastLogIndex()
	}

	return reply
}

func (rf *Raft) getEntryAtIndex(index int) (Entry, bool) {
	switch {
	// If index is out of the bound, return false.
	case index < 0 || index >= len(rf.log):
		return Entry{}, false
	default:
		return rf.log[index], true
	}
}

func (rf *Raft) getLastLogIndex() int {
	return rf.log[len(rf.log) - 1].Index
}

func (rf *Raft) getLastLogTerm() int {
	return rf.log[len(rf.log) - 1].Term
}

func (rf *Raft) getFirstIndexWithTermFrom(startIndex int, term int) (int, bool) {
	for i := startIndex; i >= 0; i-- {
		if rf.log[i].Term != term {
			return rf.log[i+1].Index, true
		}
	}

	return 0, false
}

func (rf *Raft) getLastIndexWithTermFrom(startIndex int, term int) (int, bool) {
	for i := startIndex; i >= 0; i-- {
		if rf.log[i].Term == term {
			return i, true
		}
	}

	return 0, false
}

func (rf *Raft) applyEntriesToStateMachine() {
	for  {
		// Block until raft is ready to apply.
		<-rf.isReadyToApplyEntriesToStateMachine

		for i := rf.lastApplied + 1 ; i <= rf.commitIndex; i++ {
			DPrintf("[peer_%v] applied command_%v \n", rf.me, i)
			DPrintf("[peer_%v] current log: %v", rf.me, rf.log)
			if entry, ok := rf.getEntryAtIndex(i); ok {
				msg := ApplyMsg{true, entry.Command, i}
				rf.applyCh <- msg
				rf.lastApplied = i
			}
		}
	}
}

// Make
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
// TODO: Take care of the blocking and goroutines leaks caused by zero buffer channel.
// TODO: Take care of the buffer size of each channel.
// TODO: [Test Error] TestFigure8Unreliable2C - config.go:475: one(5383) failed to reach agreement
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.mu = sync.Mutex{}
	rf.currentTerm = 0
	// Set votedFor to nil.
	rf.votedFor = -1
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.applyCh = applyCh
	rf.currentState = Follower

	numOfPeers := len(peers)
	rf.nextIndex = make([]int, numOfPeers, numOfPeers)
	for i := range rf.nextIndex {
		rf.nextIndex[i] = 1
	}
	rf.matchIndex = make([]int, numOfPeers, numOfPeers)

	// Insert an empty entry at index 0 so as to make the log
	// start storing entry from index 1.
	NoOpEntry := Entry{0, 0, nil}
	rf.log = []Entry{NoOpEntry}

	rf.raftMsgChan = make(chan interface{}, 1000)
	rf.isReadyToApplyEntriesToStateMachine = make(chan bool)
	rf.shutdown = make(chan bool)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	DPrintf("[peer_%v] Initial term: %v, votedFor: %v, log: %v", rf.me, rf.currentTerm, rf.votedFor, rf.log)

	go rf.run()
	go rf.applyEntriesToStateMachine()

	return rf
}

func (rf *Raft) run() {
	for {
		switch rf.currentState {
		case Leader:
			leader := new(LeaderState)
			leader.init(rf)
			leader.run()
		case Candidate:
			candidate := new(CandidateState)
			candidate.init(rf)
			candidate.run()
		case Follower:
			follower := new(FollowerState)
			follower.init(rf)
			follower.run()
		case Shutdown:
			DPrintf("[peer_%v] shutdown successfully", rf.me)
			return
		default:
			DPrintf("[peer_%v] running in unknown state", rf.me)
			return
		}
	}
}

type LeaderState struct {
	rf                      *Raft
	shouldRevertToFollower  chan int
	shouldUpdateCommitIndex chan bool
}

func (leader *LeaderState) init(rf *Raft) {
	leader.rf = rf
	leader.shouldRevertToFollower = make(chan int)
	leader.shouldUpdateCommitIndex = make(chan bool)
}

// The master goroutine serves as the central server and synchronizes
// distributed worker goroutines which request to modify shared variables.
func (leader *LeaderState) run() {
	DPrintf("[peer_%v] running at Leader state", leader.rf.me)
	// Send RPCs to all members in parallel.
	leader.spawnAppendEntriesRequests()

	// TODO: Commit an initial entry as part of becoming the cluster leader.

	for {
		if leader.rf.currentState != Leader {
			return
		}

		select {
		case msg := <-leader.rf.raftMsgChan:
			leader.rf.handleRaftMsg(msg)
		case term := <-leader.shouldRevertToFollower:
			leader.rf.revertToFollower(term, -1)
		case <-leader.shouldUpdateCommitIndex:
			if leader.handleUpdateCommitIndex() {
				// Block until raft applies entries to state machine.
				leader.rf.isReadyToApplyEntriesToStateMachine <- true
			}
		case <-leader.rf.shutdown:
			leader.rf.setTargetState(Shutdown)
		}
	}
}

func (leader *LeaderState) spawnAppendEntriesRequests() {
	for i := 0; i < len(leader.rf.peers); i++ {
		if i == leader.rf.me {
			continue
		}

		go func(peerId int) {
			for {
				if leader.rf.currentState != Leader {
					return
				}

				// Send append entries request per 150 milliseconds.
				time.Sleep(time.Millisecond * 150)

				var entries []Entry
				for i := leader.rf.nextIndex[peerId] ; i <= leader.rf.getLastLogIndex(); i++ {
					entries = append(entries, leader.rf.log[i])
				}

				request := new(AppendEntriesArgs)
				reply := new(AppendEntriesReply)

				request.Term = leader.rf.currentTerm
				request.LeaderId = leader.rf.me
				request.PrevLogIndex = leader.rf.nextIndex[peerId] - 1

				// TODO: [Corner Case] not sure why sometimes index out of bound will happen.
				if entry, ok := leader.rf.getEntryAtIndex(request.PrevLogIndex); ok {
					request.PrevLogTerm = entry.Term
				} else {
					request.PrevLogTerm = 0
				}

				request.Entries = entries
				request.LeaderCommit = leader.rf.commitIndex

				if ok := leader.rf.sendAppendEntries(peerId, request, reply); ok {
					leader.handleAppendEntriesReply(peerId, request, reply)
				}
			}
		}(i)
	}
}

func (leader *LeaderState) handleAppendEntriesReply(peerId int, request *AppendEntriesArgs, reply *AppendEntriesReply) {
	if reply.Term > leader.rf.currentTerm {
		leader.shouldRevertToFollower <- reply.Term
		return
	}

	if reply.Success {
		leader.rf.matchIndex[peerId] = len(request.Entries) + request.PrevLogIndex
		leader.rf.nextIndex[peerId] = leader.rf.matchIndex[peerId] + 1
		// Block until raft update commit index.
		leader.shouldUpdateCommitIndex <- true
	} else {
		// Conflict optimization: use the last index of existing entries with the same term of conflicting entry as next index.
		if index, ok := leader.rf.getLastIndexWithTermFrom(reply.ConflictOpt.Index, reply.ConflictOpt.Term); ok {
			leader.rf.nextIndex[peerId] = index
		} else {
			leader.rf.nextIndex[peerId] = reply.ConflictOpt.Index
		}

		// TODO: [Corner Case] not sure why sometimes index < 1.
		if reply.ConflictOpt.Index < 1 {
			leader.rf.nextIndex[peerId] = 1
		}
	}
}

func (leader *LeaderState) handleUpdateCommitIndex() (isUpdated bool) {
	nextCommitIndex := leader.rf.getLastLogIndex()
	numOfPeers := len(leader.rf.peers)

	// Find if there exists an N such that N > commitIndex, a majority of
	// matchIndex[i] ≥ N, and log[N].term == currentTerm: set commitIndex = N.
	for ; nextCommitIndex > leader.rf.commitIndex; nextCommitIndex-- {
		numOfMatched := 1
		for peerId := 0; peerId < numOfPeers; peerId++ {
			if peerId == leader.rf.me {
				continue
			}
			if leader.rf.matchIndex[peerId] >= nextCommitIndex {
				numOfMatched++
			}
		}

		if numOfMatched >= numOfPeers / 2 + 1 && (leader.rf.log[nextCommitIndex].Term == leader.rf.currentTerm) {
			break
		}
	}
	isUpdated = leader.rf.commitIndex == nextCommitIndex

	leader.rf.commitIndex = nextCommitIndex

	return
}

type CandidateState struct {
	rf *Raft
	numOfGrantedVotes int
	numOfRequiredVotes int
}

func (candidate *CandidateState) init(rf *Raft) {
	candidate.rf = rf
	candidate.numOfGrantedVotes = 0
	candidate.numOfRequiredVotes = 0
}


func (candidate *CandidateState) run() {
	DPrintf("[peer_%v] running at Candidate state", candidate.rf.me)
	// Each iteration of the outer loop represents a new term.
	for {
		if candidate.rf.currentState != Candidate {
			return
		}

		// Setup initial state per term.
		candidate.numOfGrantedVotes = 1
		candidate.numOfRequiredVotes = len(candidate.rf.peers) / 2 + 1

		// Setup new term.
		candidate.rf.currentTerm += 1
		candidate.rf.votedFor = candidate.rf.me
		candidate.rf.persist()

		// Send RPCs to all members in parallel.
		pendingVotes := candidate.spawnParallelVoteRequests()
		DPrintf("[peer_%v] finished spawning vote requests", candidate.rf.me)

	// Inner processing loop for this Raft state.
	InnerLoop:
		for {
			if candidate.rf.currentState != Candidate {
				return
			}

			electionTimeout := candidate.rf.getNextElectionTimeout()
			select {
			// This election has timed-out. Break to outer loop, which starts a new term.
			case <- time.After(time.Duration(electionTimeout) * time.Millisecond):
				DPrintf("[peer_%v] election timeout, resume election", candidate.rf.me)
				break InnerLoop
			case vote := <-pendingVotes:
				candidate.handleVoteReply(vote.int, vote.RequestVoteReply)
			case msg := <-candidate.rf.raftMsgChan:
				candidate.rf.handleRaftMsg(msg)
			case <-candidate.rf.shutdown:
				candidate.rf.setTargetState(Shutdown)
			}
		}
	}
}

func (candidate *CandidateState) spawnParallelVoteRequests() <-chan struct{int; RequestVoteReply} {
	request := RequestVoteArgs{candidate.rf.currentTerm, candidate.rf.me, candidate.rf.getLastLogIndex(), candidate.rf.getLastLogTerm()}
	reply := RequestVoteReply{}

	pendingVotes := make(chan struct{int; RequestVoteReply}, 1000)
	for i := 0; i < len(candidate.rf.peers); i++ {
		if i == candidate.rf.me {
			continue
		}
		go func(peerId int, request RequestVoteArgs, reply RequestVoteReply) {
			if ok := candidate.rf.sendRequestVote(peerId, &request, &reply); ok {
				pendingVotes <- struct {int; RequestVoteReply}{peerId, reply}
			}
		}(i, request, reply)
	}

	return pendingVotes
}

func (candidate *CandidateState) handleVoteReply(peerId int, reply RequestVoteReply) {
	if reply.Term > candidate.rf.currentTerm {
		candidate.rf.revertToFollower(reply.Term, -1)
		return
	}

	if reply.VoteGranted {
		DPrintf("[peer_%v] received vote from peer_%v", candidate.rf.me, peerId)
		// Check whether the peer exists in the config.
		if peerId < len(candidate.rf.peers) {
			candidate.numOfGrantedVotes += 1
		}

		// If we've received enough votes, then transition to leader state.
		if candidate.numOfGrantedVotes >= candidate.numOfRequiredVotes {
			DPrintf("[peer_%v] transitioning to Leader state as minimum number of votes have been received", candidate.rf.me)
			candidate.rf.setTargetState(Leader)
		}
	}
}

type FollowerState struct {
	rf *Raft
}

func (follower *FollowerState) init(rf *Raft) {
	follower.rf = rf
}

func (follower *FollowerState) run() {
	DPrintf("[peer_%v] running at Follower state", follower.rf.me)
	for {
		if follower.rf.currentState != Follower {
			return
		}

		electionTimeout := follower.rf.getNextElectionTimeout()

		select {
		case <- time.After(time.Duration(electionTimeout) * time.Millisecond):
			DPrintf("[peer_%v] election timeout, transition to Candidate state", follower.rf.me)
			follower.rf.setTargetState(Candidate)
		case msg := <-follower.rf.raftMsgChan:
			follower.rf.handleRaftMsg(msg)
		case <-follower.rf.shutdown:
			follower.rf.setTargetState(Shutdown)
		}
	}
}
