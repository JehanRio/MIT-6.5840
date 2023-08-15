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
	// "log"
	"bytes"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
	"6.5840/labrpc"
)

const (
	Follower = iota
	Candidate
	Leader
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
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

type logEntry struct {
	Command interface{}	// 命令请求
	Term int	// 当前日志的任期
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	// 持久性状态
	currentTerm int				// 记录当前的任期
	votedFor int				// 投给了谁
	log	[]logEntry				// 日志条目数组

	// 易失性状态
	CommitIndex	int				// 最新一次提交的日志条目编号
	lastApplied	int				// 最新一次回应 client 的日志条目编号

	// leader状态
	nextIndex []int				// 对于每一个server，需要发送给他下一个日志条目的索引，初始化为index+1
	matchIndex []int			// 对于每一个server，已经复制给该server的最后日志条目下标

	// 快照状态
	lastIncludedIndex 	int		// 快照最大的logIndex
	lastIncludedTerm 	int		// 最后一条压缩日志的term，不是压缩时的Term
	snapshot			[]byte	// 快照数据

	// 自己的一些变量状态
	state int					// follower/candidate/leader
	timer time.Time				// 计时器
	voteCount int				// 票数

	applyChan chan ApplyMsg
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()	
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == Leader
}




//----------------------------------------------日志压缩(快照）部分---------------------------------------------------------
type InstallSnapshotArgs struct {
	Term 				int		// 发送请求的Term
	LeaderId			int		// 请求方的Id
	LastIncludedIndex	int		// 快照最后applied的日志下标
	LastIncludedTerm	int		// 快照最后applied时的Term
	Data				[]byte	// 快照区块的原始字节流数据
	// offset				int		// 次传输chunk在快照文件的偏移量，快照文件可能很大，因此需要分chunk，此次不分片
	// Done 				bool	// true表示是最后一个chunk
}

type InstallSnapshotReply struct {
	Term				int		// 让leader自己更新的
}

//----------------------------------------------leader选举部分------------------------------------------------------------
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	Term int			// 候选人的任期
	CandidateId	int		// 候选人ID
	LastLogIndex int	// 候选人最后的日志索引
	LastLogTerm	 int	// 候选人最后的日志任期
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	Term int			// 投票人的当前任期
	VoteGranted	bool 	// true表示该节点把票投给了候选人
}

//----------------------------------------------心跳部分------------------------------------------------------------
type AppendEntriesArgs struct {
	Term 			int	// leader's term
	LeaderId 		int	// Leader的id
	PrevLogIndex	int	// 前一个日志的日志号
	PrevLogTerm 	int	// 前一个日志的任期号
	Entries			[]logEntry	// 当前日志体
	LeaderCommit	int	// leader的已提交日志号	若leadercommit>CommitIndex，那么把CommitIndex设为min(若leadercommit, index of last new entry)
}

type AppendEntriesReply struct {
	Term	int	// 自己当前的任期号
	Success	bool	// 如果follower包括前一个日志，则true
	CommitIndex  int	// 用于返回与Leader.Term的匹配项,方便同步日志(帮助我们更快定位)
}

//----------------------------------------------外部接口------------------------------------------------------------

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != Leader {
		return index, term, false
	}

	index = rf.getLastIndex()+1
	term = rf.currentTerm

	// 添加新日志
	e := logEntry{command, rf.currentTerm}
	rf.log = append(rf.log, e)
	rf.persist()
	return index, term, true
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.state = Follower
	rf.voteCount = 0
	rf.applyChan = applyCh

	rf.CommitIndex = 0
	rf.lastApplied = 0
	

	rf.lastIncludedIndex = 0
	rf.lastIncludedTerm = 0

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	
	// 快照
	rf.log = append(rf.log, logEntry{})	// 添加一个空日志，起始长度为1
	rf.snapshot = nil
	rf.snapshot = persister.ReadSnapshot()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	if rf.lastIncludedIndex > 0 {
		rf.lastApplied = rf.lastIncludedIndex
	}
	// start ticker goroutine to start elections
	go rf.electionTicker()
	go rf.appendTicker()
	go rf.committedTicker()
	
	return rf
}


//----------------------------------------------ticker------------------------------------------------------------
func (rf *Raft) electionTicker() {
	for !rf.killed() {
		nowTime := time.Now()
		time.Sleep(time.Duration(generateOverTime(int64(rf.me))) * time.Millisecond)


		rf.mu.Lock()
		// 超过定时的时间了，进行选举；且只有follower和candidate可以发起选举
		// if time.Now().After(rf.timer) && rf.state != Leader {
		if rf.timer.Before(nowTime) && rf.state != Leader {	
			rf.state = Candidate
			rf.voteCount = 1
			rf.votedFor = rf.me
			rf.currentTerm++
			rf.persist()
			// fmt.Println("增加term:", rf.currentTerm)
			
			rf.startElectionL()
			rf.timer = time.Now()
		}
		rf.mu.Unlock()
		// ms := 150
		// time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) appendTicker() {
	for !rf.killed() {
		// 直接休息心跳时间
		// heartbeatTimeout := time.Millisecond * time.Duration(50+rand.Intn(50))
		// time.Sleep(heartbeatTimeout)
		time.Sleep(HeartbeatSleep * time.Millisecond)
		rf.mu.Lock()
		if rf.state == Leader {
			rf.mu.Unlock()
			rf.leaderAppendEntries()
		} else {
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) committedTicker() {
	// put the committed entry to apply on the state machine
	for rf.killed() == false {
		time.Sleep(AppliedSleep * time.Millisecond)
		rf.mu.Lock()

		if rf.lastApplied >= rf.CommitIndex {
			rf.mu.Unlock()
			continue
		}

		Messages := make([]ApplyMsg, 0)
		for rf.lastApplied < rf.CommitIndex && rf.lastApplied < rf.getLastIndex() {
			rf.lastApplied++
			Messages = append(Messages, ApplyMsg{
				CommandValid:  true,
				SnapshotValid: false,
				CommandIndex:  rf.lastApplied,
				Command:       rf.restoreLog(rf.lastApplied).Command,
			})
		}
		rf.mu.Unlock()

		for _, messages := range Messages {
			rf.applyChan <- messages
		}
	}
}

//----------------------------------------------leader选举部分------------------------------------------------------------
func (rf *Raft) startElectionL() {
	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}
		// 开协程对每个结点发起选举
		go func(server int) {
			rf.mu.Lock()
			args := RequestVoteArgs{
				Term: rf.currentTerm, 
				CandidateId: rf.me, 
				LastLogIndex:  rf.getLastIndex(),
				LastLogTerm: rf.getLastTerm(),	
			}
			reply := RequestVoteReply{}
			rf.mu.Unlock()

			if ok := rf.sendRequestVote(server, &args, &reply); !ok {
				return
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()
			
			// 收到过期的RPC回复,不处理
			if args.Term < rf.currentTerm || rf.state != Candidate {
				return
			}

			if reply.Term > args.Term {
				rf.state = Follower
				rf.votedFor = -1
				rf.voteCount = 0
				rf.persist()
				return
			}

			if reply.VoteGranted {
				rf.voteCount++
				if rf.voteCount > len(rf.peers)/2 {
					// fmt.Println("新王登基，他的ID是:" ,rf.me)
					rf.voteCount = 1
					rf.state = Leader
					rf.voteCount = 0
					rf.persist()
					for i, _ := range rf.nextIndex {
						rf.nextIndex[i] = rf.getLastIndex() + 1 	// 初始化为leader的最后一个日志的下一个条目
						// fmt.Println("rf.nextIndex[i]:",rf.nextIndex[i])
					}
					rf.matchIndex[rf.me] = rf.getLastIndex()
					// rf.resetTimer()	// 重新设置时间
					rf.timer = time.Now()
				}
			} else {
				rf.convert2Follower(reply.Term)
			}
		}(i)
	}
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()	
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm	
	reply.VoteGranted = false	// 初始化
	// 1. 比自己还小直接返回不用看了
	if args.Term < rf.currentTerm {	
		return
	}

	if args.Term > rf.currentTerm {
		// 重置自身的状态
		rf.state = Follower
		rf.currentTerm = args.Term	// 这一步是为了更新成最新的term，就算自己投反对票，如果后续自己要变为candidate，也可以从最新term开始
		rf.voteCount = 0
		rf.votedFor = -1	// 新的term，可以进行新一轮投票了
		rf.persist()
	}

	// 如果votedFor为空或候选人Id，且候选人的日志至少与接收方的日志一样新，则同意投票（§5.2，§5.4）
	if rf.UpToDate(args.LastLogIndex, args.LastLogTerm) &&
		(rf.votedFor == -1 || rf.votedFor == args.CandidateId) {
		rf.state = Follower
		// rf.voteCount = 0
		rf.currentTerm = args.Term
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		// rf.resetTimer()	// 只有投赞成票才重置时间
		rf.timer = time.Now()
		rf.persist()
	}	
}

//----------------------------------------------日志增量/心跳------------------------------------------------------------
func (rf *Raft) leaderAppendEntries() {
	for index := range rf.peers {
		if index == rf.me {
			continue
		}
		go func(server int) {
			rf.mu.Lock()
			if rf.state != Leader {
				rf.mu.Unlock()
				return
			}
			// follower的日志落后于快照，需要发送快照
			if rf.nextIndex[server] - 1 < rf.lastIncludedIndex {
				go rf.leaderSendSnapShot(server)
				rf.mu.Unlock()
				return
			}
			prevLogIndex, prevLogTerm := rf.getPrevLogInfo(server)
			args := AppendEntriesArgs{
				Term: rf.currentTerm, 
				LeaderId: rf.me, 
				PrevLogIndex: prevLogIndex,
				PrevLogTerm: prevLogTerm, 
				LeaderCommit: rf.CommitIndex}
			if rf.getLastIndex() >= rf.nextIndex[server] {		// 刚成为leader的时候更新过 所以第一次entry为空
				entries := rf.log[rf.nextIndex[server]-rf.lastIncludedIndex:]			//如果日志小于leader的日志的话直接拷贝日志
				args.Entries = make([]logEntry, len(entries))
				copy(args.Entries, entries)
			} else {
				args.Entries = []logEntry{}
			}

			// fmt.Println("写入的主机是:", server,"len(rf.log):", len(rf.log), "PrevLogIndex:", args.PrevLogIndex, "rf.nextIndex[i]:", rf.nextIndex[server], "rf.currentTerm:", rf.currentTerm, "rf.CommitIndex:", rf.CommitIndex)
			reply := AppendEntriesReply{}
			rf.mu.Unlock()

			if ok := rf.sendAppendEntries(server, &args, &reply); !ok {
				return
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()

			if rf.state != Leader {
				return 
			}
		
			// 自己的term没别人的大，变为follower
			if rf.currentTerm < reply.Term {
				rf.convert2Follower(reply.Term)
				return
			}

			if reply.Success {
				rf.CommitIndex = rf.lastIncludedIndex	// 初始化
				rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
				rf.nextIndex[server] = rf.matchIndex[server] + 1

				for index := rf.getLastIndex(); index >= rf.lastIncludedIndex + 1; index-- {
					count := 1	// 自己先有一票
					for i := 0; i < len(rf.peers); i++ {
						if i == rf.me {
							continue
						}
						if rf.matchIndex[i] >= index {
							count++
						}
					}	
					if count >= len(rf.peers)/2+1 && rf.restoreLogTerm(index) == rf.currentTerm {
						rf.CommitIndex = index
						break
					}
				} 
			} else {	// conflict
				if reply.CommitIndex != -1 {	// 空日志
					rf.nextIndex[server] = reply.CommitIndex
				}
			}
		}(index)
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// defer fmt.Println("接收到心跳，我是:", rf.me)

	reply.Term = rf.currentTerm
	reply.Success = false
	reply.CommitIndex  = -1
	// 收到rpc的term比自己的小 (§5.1)
	if args.Term < rf.currentTerm {	// 并通知leader变为follower
		return 
	} 
	
	reply.Success = true

	// 承认对方是leader
	rf.convert2Follower(args.Term)


	// 快照比日志大，得从快照下一个坐标开始
	if args.PrevLogIndex < rf.lastIncludedIndex {
		reply.Success = false
		reply.CommitIndex = rf.getLastIndex() + 1
		return
	}
	
	// 日志缺失
	if rf.getLastIndex() < args.PrevLogIndex {
		reply.Success = false
		reply.CommitIndex = rf.getLastIndex()
		return
	} else {
		// 日志冲突
		if rf.restoreLogTerm(args.PrevLogIndex) != args.PrevLogTerm {
			reply.Success = false
			curTerm := rf.restoreLogTerm(args.PrevLogIndex)
			for index := args.PrevLogIndex; index >= rf.lastIncludedIndex; index-- {
				if rf.restoreLogTerm(index) != curTerm {
					reply.CommitIndex = index + 1
					break
				}
			}
			return
		}
	}

	// 将多余的日志截取,并将传来的日志加入
	rf.log = append(rf.log[:args.PrevLogIndex + 1 - rf.lastIncludedIndex], args.Entries...)
	rf.persist()

	// 更新commit，提交日志，同时防止日志下标溢出
	if args.LeaderCommit > rf.CommitIndex {
		rf.CommitIndex = min(args.LeaderCommit, rf.getLastIndex())
	}
}

//----------------------------------------------持久化---------------------------------------------------------
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	rf.persister.SaveRaftState(rf.encodeState())
}

func (rf *Raft) encodeState() []byte {
	w := new(bytes.Buffer)	// 创建缓冲区，实际上是一个字节切片
	e := labgob.NewEncoder(w)	// 创建一个新的labgob编码器，编码后的内容存放在w中
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	raftstate := w.Bytes()	// 将缓冲区的内容赋值给raftstate
	return raftstate
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var(
		currentTerm int
		votedFor int
		log []logEntry
		lastIncludedIndex int
		lastIncludedTerm int
	)

	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil ||
		d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&lastIncludedTerm) != nil {
			fmt.Println("decode error!")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
		rf.lastIncludedIndex = lastIncludedIndex
		rf.lastIncludedTerm = lastIncludedTerm
	}
}

//----------------------------------------------日志压缩(快照）部分---------------------------------------------------------
func (rf *Raft) leaderSendSnapShot(server int) {
	rf.mu.Lock()
	args := InstallSnapshotArgs{
		Term: rf.currentTerm,
		LeaderId: rf.me,
		LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm: rf.lastIncludedTerm,
		Data: rf.persister.ReadSnapshot(),
	}
	rf.mu.Unlock()
	reply := InstallSnapshotReply{}
	if ok := rf.sendSnapShot(server, &args, &reply); !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Leader || rf.currentTerm != args.Term {
		return
	}
	if reply.Term > rf.currentTerm {
		rf.convert2Follower(reply.Term)
		return
	}

	rf.matchIndex[server] = args.LastIncludedIndex
	rf.nextIndex[server] = rf.matchIndex[server] + 1
}

// follower更新leader的快照
func (rf *Raft) InstallSnapShot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		return
	}
	reply.Term = args.Term
	rf.convert2Follower(args.Term)
	// 已经覆盖了这个快照了，不用再写入快照
	if rf.lastIncludedIndex >= args.LastIncludedIndex {
		return
	}

	// 将快照后的log切割，快照前的提交
	index := args.LastIncludedIndex
	tempLog := make([]logEntry, 0)
	tempLog = append(tempLog, logEntry{})
	for i := index+1; i <= rf.getLastIndex(); i++ {
		tempLog = append(tempLog, rf.restoreLog(i))
	}
	rf.log = tempLog

	rf.lastIncludedIndex = index
	rf.lastIncludedTerm = args.LastIncludedTerm

	if index > rf.CommitIndex {
		rf.CommitIndex = index
	}
	if index > rf.lastApplied {
		rf.lastApplied = index
	}
	rf.persister.Save(rf.encodeState(), args.Data)
	applyMsg := ApplyMsg{
		SnapshotValid: true,
		Snapshot: args.Data,
		SnapshotTerm: rf.lastIncludedTerm,
		SnapshotIndex: rf.lastIncludedIndex,
	}
	rf.applyChan <- applyMsg
}
// index代表是快照apply应用的index,而snapshot代表的是上层service传来的快照字节流，包括了Index之前的数据
// 这个函数的目的是把安装到快照里的日志抛弃，并安装快照数据，同时更新快照下标，属于peers自身主动更新，与leader发送快照不冲突
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	
	// 如果下标大于自身commit，说明没被提交，不能执行快照，若自身快照大于index则说明已经执行过快照，也不需要
	if rf.lastIncludedIndex >= index || index > rf.CommitIndex {
		return
	}

	// 裁剪日志
	sLog := make([]logEntry, 0)
	sLog = append(sLog, logEntry{})
	for i := index + 1; i <= rf.getLastIndex(); i++ {
		sLog = append(sLog, rf.restoreLog(i))
	}
	
	// 更新快照下标和快照Term
	if index == rf.getLastIndex() + 1 {
		rf.lastIncludedTerm = rf.getLastTerm()
	} else {
		rf.lastIncludedTerm = rf.restoreLogTerm(index)
	}

	rf.lastIncludedIndex = index
	rf.log = sLog

	// 需要重置commitIndex、lastApplied下标
	if index > rf.CommitIndex {
		rf.CommitIndex = index
	}
	if index > rf.lastApplied {
		rf.lastApplied = index
	}

	rf.persister.Save(rf.encodeState(), snapshot)
}

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)

	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}