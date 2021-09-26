package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"
	// "crypto/rand"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

const (
	Follwer    = 0
	Candidates = 1
	Leader     = 2
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntry struct {
	// 状态机命令
	Command string
	// 领导者接收到该条目的任期（第一个索引为1）
	Term int
}

//
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

	// 状态参数
	// 服务器已知最新任期（在服务器首次启动的时候初始化为0，单调递增）
	CurrentTerm int
	// 当前任期内收到选票的候选人id，如果没有头给人和候选者，则为空
	VotedFor int
	// 日志条目，每个条目包含了用于状态机的命令，以及领导者接收到该条目时的任期（第一个索引为1）
	Log []LogEntry

	// 服务器上的易失性状态
	// 已知已提交的最高的日志条目的索引（初始值为0，单调递增）
	CommitIndex int
	// 已经被应用到状态机的最高的日至条目的索引（初始值为0，单调递增）
	LastApplied int
	// 领导者（服务器）上的易失性状态（选举后已经重新初始化）
	// 对于每一台服务器，发送到该服务器的下一个日志条目的索引（初始值为领导者最后的日志条目的索引 + 1）
	NextIndex []int
	// 对于每一台服务器，已知的已经复制到该服务器的最高日志条目的索引（初始值为0，单调递增）
	MatchIndex []int

	// 记录此台服务器的状态
	State int
	// 记录此台服务器上次接收心跳检测的时间
	HeartBeat time.Time
}

type AppendEntries struct {
	// 领导人的任期
	Term int
	// 领导人ID， 因此跟随者可以对客户端进行重定向
	LeaderID int
	// 紧邻新日志条目之前的那个日志条目的索引
	PrevLogIndex int
	// 紧邻新日志条目之前的那个日志条目的任期
	PrevLogTerm int
	// 需要被保存的日志条目（被当作心跳使用，则日志条目内容为空；为了提高效率可能一次性发送多个）
	Entries []LogEntry
	// 领导者的已知已提交的最高的日志条目的索引
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool

	term = rf.CurrentTerm
	if rf.State == Leader {
		isleader = true
	} else {
		isleader = false
	}
	// Your code here (2A).
	return term, isleader
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
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
// 候选人请求投票
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	// In Raft Paper Figure2
	// 候选人的任期号
	Term int
	// 请求选票的候选人的id
	CandidateId int
	// 候选人的最后日志条目的索引值
	LastLogIndex int
	// 候选人最后日志条目的任期号
	LastLogTerm int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	// 当前任期号，以便于候选人去更新自己的任期号
	Term int
	// 候选人赢得了此张选票时为真
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
// 候选人向其他节点寻求选票
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	if args.Term < rf.CurrentTerm {
		fmt.Printf("[寻求选票] 候选人任期小于自己的任期.\n")
		reply.VoteGranted = false
	}

	// 如果 VotedFor 为空或者为Candidateld,并且候选人的日志指导和自己一样新，
	// 那么就投票给他
	if rf.VotedFor == -1 && args.LastLogTerm >= rf.Log[rf.CommitIndex].Term {
		reply.VoteGranted = true
	}
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

//
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

	return index, term, isLeader
}

//
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
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) getHeartBeatTime() time.Time {

}

func (rf *Raft) getelectionTimeout() time.Duration {

}

// Leader 需要向 flowers 周期性地发送心跳包
func (rf *Raft) sendHeartBeats() {

}

// 候选人发起选举
func (rf *Raft) election() {
	// 发起选举，首先增加自己的任期
	rf.CurrentTerm += 1
	// 转变为候选人状态
	rf.State = Candidates
	// 为自己投一票
	rf.VotedFor = rf.me
	// 并行地向除自己的服务器索要选票
	// 如果没有收到选票，它会反复尝试，直到发生以下三种情况之一：
	// 1. 获得超过半数的选票；成为 Leader，并向其他节点发送 AppendEntries 心跳;
	// 2. 收到来自 Leader 的 RPC， 转为 Follwer
	// 3. 其他两种情况都没发生，没人能够获胜（electionTimeout 已过）：增加 currentTerm,
	// 开始新一轮选举
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			req := RequestVoteArgs{}
			reply := RequestVoteReply{}
			// 初始化请求的参数
			req.Term = rf.CurrentTerm
			req.CandidateId = rf.me
			req.LastLogIndex = len(rf.Log)
			if len(rf.Log) == 0 {
				req.LastLogTerm = 1
			} else {
				req.LastLogTerm = rf.Log[len(rf.Log)-1].Term
			}
			go rf.sendRequestVote(rf.me, &req, &reply)
		}
	}
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for !rf.killed() {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

		electionTimeOut := rf.getelectionTimeout()
		time.Sleep(electionTimeOut)

		duration := time.Since(rf.getHeartBeatTime())

		// 如果超过选举超时时间没有接收到心跳包，则变成候选者发起选举
		if duration > electionTimeOut {
			rf.election()
		} else {
			// 如果接到了心跳包则变成追随者
			fmt.Printf("[Debug] %v is follower.\n", rf.me)
			continue
		}
	}
}

//
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
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.State = Follwer
	rf.CurrentTerm = 1

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	// Leader send heartbeats to all followers
	go rf.sendHeartBeats()

	return rf
}
