package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"

	"bytes"
	"log/slog"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	CurrentTerm int
	State       State
	VotedFor    int
	VoteCount   int

	lastHeartBeat time.Time

	log              []LogEntry
	commitIndex      int
	nextIndex        []int //每个follower单独维护
	matchIndex       []int //已经append到follower的索引
	lastAppliedIndex int
	applyCh          chan raftapi.ApplyMsg

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

type LogEntry struct {
	Term    int
	Index   int
	Command interface{}
}

type State int

const (
	Follower State = iota
	Candidate
	Leader
)

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).
	rf.mu.Lock()
	term = rf.CurrentTerm
	isleader = rf.State == Leader
	rf.mu.Unlock()
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VotedFor)
	e.Encode(rf.log)

	data := w.Bytes()

	rf.persister.Save(data, nil)

}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var logEntries []LogEntry
	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&logEntries) != nil {
		slog.Warn("Error")
	} else {
		rf.CurrentTerm = currentTerm
		rf.VotedFor = votedFor
		rf.log = logEntries
	}

	// Your code here (3C).
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

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	CandidateId int
	CurrentTerm int

	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	IsVote bool
	Term   int
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()

	if args.CurrentTerm > rf.CurrentTerm {
		rf.CurrentTerm = args.CurrentTerm
		rf.State = Follower
		rf.VotedFor = -1
		rf.persist()
	}
	reply.Term = rf.CurrentTerm

	if rf.CurrentTerm > args.CurrentTerm || (rf.VotedFor != -1 && rf.VotedFor != args.CandidateId) {
		reply.IsVote = false
	} else if args.LastLogTerm > rf.lastLogTerm() {
		reply.IsVote = true
	} else if args.LastLogTerm == rf.lastLogTerm() && args.LastLogIndex >= rf.lastLogIndex() {
		reply.IsVote = true
	} else {
		reply.IsVote = false
	}

	/*slog.Info("election: vote",
	slog.Int("candidate", args.CandidateId),
	slog.Int("me", rf.me),
	slog.Int("candidate_term", args.CurrentTerm),
	slog.Int("my_term", rf.CurrentTerm),
	slog.Bool("granted", reply.IsVote))*/

	//rf.lastHeartBeat = time.Now()

	if reply.IsVote {
		if rf.VotedFor != args.CandidateId {
			rf.VotedFor = args.CandidateId
			rf.persist()
		}
		// 刷新选举超时，避免多个候选人持续竞争
		rf.lastHeartBeat = time.Now()
		//rf.CurrentTerm = args.CurrentTerm
	}
	//slog.Info("vote for", slog.Int("node", rf.me), slog.Int("term", rf.CurrentTerm))
	rf.mu.Unlock()
}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntriesArgs struct {
	Term         int
	Leader       int
	Entries      []LogEntry
	LeaderCommit int

	PreLogIndex int
	PreLogTerm  int
}

type AppendEntriesReply struct {
	IsSuccess     bool
	Term          int
	ConflictTerm  int
	ConflictIndex int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.lastHeartBeat = time.Now()
	reply.Term = rf.CurrentTerm
	//slog.Info("Term", slog.Int("leaderTerm", args.Term), slog.Int("myTerm", rf.CurrentTerm), slog.Int("me", rf.me))

	if args.Term > rf.CurrentTerm {
		rf.CurrentTerm = args.Term
		rf.VotedFor = -1
		rf.persist()
	}

	if args.Term >= rf.CurrentTerm {
		rf.State = Follower
		//rf.CurrentTerm = args.Term

		//-------------------
		/*if args.PreLogIndex == 101 {
			slog.Info("PreLogIndex 101", slog.Int("PreLogTerm", args.PreLogTerm), slog.Int("myLastLogTerm", rf.log[args.PreLogIndex].Term), slog.Int("commitIndex", rf.commitIndex), slog.Int("me", rf.me))
		}*/

		if args.PreLogIndex >= len(rf.log) { //节点不包含preLog
			reply.IsSuccess = false
			reply.ConflictIndex = len(rf.log)
			slog.Info("nextIndex not in", slog.Int("PreLogIndex", args.PreLogIndex), slog.Int("expected", len(rf.log)-1), slog.Int("me", rf.me))
			return
		} else if args.PreLogTerm != rf.log[args.PreLogIndex].Term { //prelog Term不匹配，快速回退
			reply.IsSuccess = false
			reply.ConflictTerm = rf.log[args.PreLogIndex].Term

			first := args.PreLogIndex
			for first >= 0 && rf.log[first].Term == reply.ConflictTerm {
				first--
			}
			reply.ConflictIndex = first + 1

			//slog.Info("not term", slog.Int("logsize", len(rf.log)), slog.Int("preLogIndex", args.PreLogIndex))
			slog.Info("nextIndex not term", slog.Int("PreLogIndex", args.PreLogIndex), slog.Int("PreLogTerm", args.PreLogTerm), slog.Int("myLastLogTerm", rf.log[args.PreLogIndex].Term))

			//rf.log = rf.log[:reply.ConflictIndex] //term不匹配删除index之后的日志
			//rf.persist()
			if rf.commitIndex >= len(rf.log) {
				rf.commitIndex = len(rf.log) - 1
			}

			slog.Info("fast backup",
				slog.Int("PreLogIndex", args.PreLogIndex),
				slog.Int("PreLogTerm", args.PreLogTerm),
				slog.Int("ConflictTerm", reply.ConflictTerm),
				slog.Int("ConflictIndex", reply.ConflictIndex),
				slog.Int("logsize_after_truncate", len(rf.log)),
				slog.Int("me", rf.me))
			return
		}
		if args.LeaderCommit > rf.commitIndex {
			rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)
		}

		if len(args.Entries) > 0 {
			rf.log = append(rf.log[:args.PreLogIndex+1], args.Entries...)
			rf.persist()
			//slog.Info("node append log")
		}
		//slog.Info("update committedIndex", slog.Int("node", rf.me), slog.Int("value", rf.commitIndex))
		reply.IsSuccess = true
		//slog.Info("heartBeat update", slog.Int("node", rf.me), slog.Int("leader", args.Leader), slog.Int("term", rf.CurrentTerm))

	}
}

func (rf *Raft) heatBeatCheck() {
	for {
		rf.mu.Lock()
		if rf.killed() || rf.State != Leader {
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
		for i := range rf.peers {
			if i == rf.me {
				continue
			}

			go func(server int) {
				rf.mu.Lock()
				isLeader := rf.State == Leader
				if isLeader {
					//检测commitIndex更新
					for N := rf.commitIndex + 1; N <= len(rf.log)-1; N++ {
						count := 1
						for _, match := range rf.matchIndex {
							if match >= N {
								count++
							}
						}
						if count > len(rf.peers)/2 && rf.log[N].Term == rf.CurrentTerm {
							rf.commitIndex = N
							//slog.Info("update committedIndex", slog.Int("node", rf.me), slog.Int("value", rf.commitIndex))
						}
					}

					//slog.Info("send log", slog.Int("nextIndex", rf.nextIndex[server]), slog.Int("node", server),
					//	slog.Int("me", rf.me), slog.Int("myterm", rf.CurrentTerm))
					//sentIndex := len(rf.log) - 1
					fromIndex := rf.nextIndex[server]
					preLogIndex := fromIndex - 1
					preLogTerm := rf.log[preLogIndex].Term

					entries := rf.log[fromIndex:]
					args := AppendEntriesArgs{
						Term:         rf.CurrentTerm,
						Leader:       rf.me,
						Entries:      entries,
						LeaderCommit: rf.commitIndex,
						//SentIndex:    len(rf.log) - 1,
						//FromIndex:    rf.nextIndex[server],
						PreLogIndex: preLogIndex,
						PreLogTerm:  preLogTerm,
					}
					rf.mu.Unlock()
					reply := AppendEntriesReply{
						ConflictTerm:  -1,
						ConflictIndex: -1,
					}

					//slog.Info("send log", slog.Int("from", fromIndex), slog.Int("to", sentIndex),
					//	slog.Int("node", server), slog.Int("me", rf.me), slog.Int("myterm", rf.CurrentTerm), slog.Int("logsize", len(rf.log)))

					ok := rf.peers[server].Call("Raft.AppendEntries", &args, &reply)
					rf.mu.Lock()
					if ok {
						if reply.Term > rf.CurrentTerm {
							rf.CurrentTerm = reply.Term
							rf.State = Follower
							rf.VotedFor = -1
							rf.persist()
							rf.mu.Unlock()
							return
						}
						if reply.IsSuccess {
							// 只有确实发送了 entries 才推进 nextIndex/matchIndex
							//slog.Info("send log ok", slog.Int("leader", rf.me), slog.Int("follower", server))
							if len(entries) > 0 {
								advancedTo := preLogIndex + len(entries)
								if len(entries) == 0 {
									advancedTo = preLogIndex
								}
								rf.matchIndex[server] = advancedTo
								rf.nextIndex[server] = advancedTo + 1
							}
						}
						if !reply.IsSuccess && reply.ConflictTerm != -1 { //有冲突任期
							//leader有冲突任期
							found := -1
							for i := len(rf.log) - 1; i >= 0; i-- {
								if rf.log[i].Term == reply.ConflictTerm {
									found = i
									break
								}
							}
							//没找到冲突任期
							if found != -1 {
								rf.nextIndex[server] = found + 1
							} else {
								rf.nextIndex[server] = reply.ConflictIndex
							}

						} else if !reply.IsSuccess && reply.ConflictIndex != -1 { //无冲突任期
							/*if rf.nextIndex[server] != 1 {
								rf.nextIndex[server]--
							} */
							rf.nextIndex[server] = max(reply.ConflictIndex, 0)
							slog.Info("update nextIndex", slog.Int("value", rf.nextIndex[server]), slog.Int("conflictIndex", reply.ConflictIndex), slog.Int("node", server))
							//slog.Warn("send log failed", slog.Int("node", server), slog.Int("me", rf.me))
						}

						if rf.matchIndex[server] >= rf.nextIndex[server] {
							rf.matchIndex[server] = rf.nextIndex[server] - 1
						}

					} else if !ok {
						//slog.Warn("rpc timeout", slog.Int("leader", rf.me), slog.Int("follower", server))
					}
					rf.mu.Unlock()
				}
			}(i)
		}

		time.Sleep(100 * time.Millisecond)
	}
}

func (rf *Raft) applyLog() {
	for !rf.killed() {
		rf.mu.Lock()
		var msgs []raftapi.ApplyMsg
		// 只能应用到本地已存在的日志下标，避免跳过未到达的条目
		applyUpto := rf.commitIndex
		if maxLocal := len(rf.log) - 1; applyUpto > maxLocal {
			applyUpto = maxLocal
		}
		for rf.lastAppliedIndex < applyUpto {
			rf.lastAppliedIndex++

			slog.Info("applyLog", slog.Int("index", rf.lastAppliedIndex), slog.Int("logTerm", rf.log[rf.lastAppliedIndex].Term), slog.Int("commitIndex", rf.commitIndex), slog.Int("logsize", len(rf.log)), slog.Int("me", rf.me))
			msgs = append(msgs, raftapi.ApplyMsg{
				CommandValid: true,
				Command:      rf.log[rf.lastAppliedIndex].Command,
				CommandIndex: rf.lastAppliedIndex,
			})
		}

		rf.mu.Unlock()

		for _, msg := range msgs {
			rf.applyCh <- msg
		}
		time.Sleep(50 * time.Millisecond)
	}
}

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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := false

	// Your code here (3B).
	//slog.Info("start")

	rf.mu.Lock()
	term = rf.CurrentTerm
	isLeader = rf.State == Leader
	if isLeader {
		index = len(rf.log)
		rf.log = append(rf.log,
			LogEntry{
				Term:    rf.CurrentTerm,
				Index:   index,
				Command: command,
			})
		rf.persist()
		//slog.Info("append a log", slog.Int("index", index), slog.Int("logsize", len(rf.log)))
	}
	rf.mu.Unlock()
	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) startElection() {
	//成为候选人，投票给自己
	rf.mu.Lock()
	rf.State = Candidate
	rf.VoteCount = 1
	rf.VotedFor = rf.me
	rf.CurrentTerm++
	rf.persist()
	//slog.Info("start election", slog.Int("me", rf.me))
	//slog.Info("start election", slog.Int("node", rf.me), slog.Int("term", rf.CurrentTerm))

	// 在锁保护下一次性捕获所有需要的状态
	currentTerm := rf.CurrentTerm
	//candidateId := rf.me
	lastLogIndex := rf.lastLogIndex()
	lastLogTerm := rf.lastLogTerm()

	rf.mu.Unlock()
	//向所有节点发送投票请求
	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		go func(server int) { //用routine防止rpc阻塞节点
			args := RequestVoteArgs{
				CandidateId:  rf.me,
				CurrentTerm:  currentTerm,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}
			reply := RequestVoteReply{}
			ok := rf.sendRequestVote(server, &args, &reply)

			rf.mu.Lock()
			if ok {
				if reply.Term > rf.CurrentTerm {
					rf.State = Follower
					rf.CurrentTerm = reply.Term
					rf.persist()
					rf.mu.Unlock()
					return
				}
				if reply.IsVote {
					rf.VoteCount++
					//slog.Info("got vote", slog.Int("node", rf.me), slog.Int("votes", rf.VoteCount), slog.Int("term", rf.CurrentTerm))
					if rf.VoteCount > len(rf.peers)/2 && rf.State == Candidate {
						slog.Info("become leader", slog.Int("startnode", rf.me), slog.Int("votes", rf.VoteCount), slog.Int("need votes", len(rf.peers)/2), slog.Int("term", currentTerm), slog.Int("nextIndex", len(rf.log)))
						rf.State = Leader //票数过半成为leader
						//init data
						for i := range rf.nextIndex {
							rf.nextIndex[i] = len(rf.log)
						}
						for i := range rf.matchIndex {
							rf.matchIndex[i] = 0
						}
						go rf.heatBeatCheck()
					}

				}
			}
			rf.mu.Unlock()
		}(i)

	}
}

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (3A)
		// Check if a leader election should be started.

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 150 + (rand.Int63() % 150) //150ms-300ms
		timeout := time.Duration(ms) * time.Millisecond

		//检测  选举超时
		rf.mu.Lock()
		if time.Since(rf.lastHeartBeat) > timeout {
			if rf.State != Leader {
				go rf.startElection()
			}
		}
		rf.mu.Unlock()
		time.Sleep(10 * time.Millisecond)

	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {

	slog.Info("make")

	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.CurrentTerm = 0
	rf.State = Follower
	rf.VotedFor = -1

	rf.applyCh = applyCh
	rf.commitIndex = 0
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for i := range rf.matchIndex {
		rf.matchIndex[i] = 0
	}
	rf.lastAppliedIndex = 0

	rf.log = append(rf.log, LogEntry{
		Term:    0,
		Index:   0,
		Command: nil,
	})

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.persist()
	// start ticker goroutine to start elections
	go rf.ticker()

	//go rf.updateCommittedIndex()
	go rf.applyLog()

	return rf
}

func (rf *Raft) lastLogIndex() int {
	if len(rf.log) == 0 {
		return 0
	}
	return len(rf.log) - 1
}

func (rf *Raft) lastLogTerm() int {
	if len(rf.log) == 0 {
		return 0
	}
	return rf.log[len(rf.log)-1].Term
}
