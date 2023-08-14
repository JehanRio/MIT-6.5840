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
	"math/rand"
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
	commitIndex	int				// 最新一次提交的日志条目编号
	lastApplied	int				// 最新一次回应 client 的日志条目编号

	// leader状态
	nextIndex []int				// 对于每一个server，需要发送给他下一个日志条目的索引，初始化为index+1
	matchIndex []int			// 对于每一个server，已经复制给该server的最后日志条目下标

	// 快照状态
	lastIncludedIndex 	int		// 快照最大的logIndex
	lastIncludedTerm 	int		// 最后一条压缩日志的term，不是压缩时的Term
	snapshotOffset		int		// 快照可能需要分批次传输
	snapshot			[]byte	// 快照数据

	// 自己的一些变量状态
	state int					// follower/candidate/leader
	timer Timer					// 自己定义的定时器
	voteCount int				// 票数

	applyChan chan ApplyMsg
}

type Timer struct {
	timer *time.Ticker
}

func (t *Timer) reset() {
	randomTime := time.Duration(150 + rand.Intn(200))*time.Millisecond	// 150~350
	t.timer.Reset(randomTime)	// 重置时间
}

// 心跳时间，leader发送给别的节点
func (t *Timer) resetHeartBeat() {
	heartbeatTimeout := time.Millisecond * time.Duration(50+rand.Intn(50))
	t.timer.Reset(heartbeatTimeout)
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var isleader bool = false
	rf.mu.Lock()	
	defer rf.mu.Unlock()
	if rf.state == Leader {
		isleader = true
		// fmt.Println("Leader是:", rf.me)
	}	
	return rf.currentTerm, isleader
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
	rf.persister.Save(rf.encodeState(), rf.persister.ReadSnapshot())	
}

func (rf *Raft) encodeState() []byte {
	w := new(bytes.Buffer)	// 创建缓冲区，实际上是一个字节切片
	e := labgob.NewEncoder(w)	// 创建一个新的labgob编码器，编码后的内容存放在w中
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
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
	)
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil {
			fmt.Println("decode error!")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
	}
}

//----------------------------------------------日志压缩(快照）部分---------------------------------------------------------
type InstallSnapshotArgs struct {
	Term 				int		// 发送请求的Term
	LeaderId			int		// 请求方的Id
	lastIncludedIndex	int		// 快照最后applied的日志下标
	lastIncludedTerm	int		// 快照最后applied时的Term
	data				[]byte	// 快照区块的原始字节流数据
	// offset				int		// 次传输chunk在快照文件的偏移量，快照文件可能很大，因此需要分chunk，此次不分片
	// Done 				bool	// true表示是最后一个chunk
}

type InstallSnapshotReply struct {
	Term				int		// 让leader自己更新的
}


// index代表是快照apply应用的index,而snapshot代表的是上层service传来的快照字节流，包括了Index之前的数据
// 这个函数的目的是把安装到快照里的日志抛弃，并安装快照数据，同时更新快照下标，属于peers自身主动更新，与leader发送快照不冲突
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 如果下标大于自身commit，说明没被提交，不能执行快照，若自身快照大于index则说明已经执行过快照，也不需要
	if rf.lastIncludedIndex >= index || index > rf.commitIndex {
		return
	}

	// 裁剪日志
	// slog := make([]logEntry, 0)
	// for i := index + 1; i <= rf.getLastIndex(); i++ {
	// 	slog = append(slog, rf.restoreLog(i))	// 通过快照日志下标转移到真实日志下标
	// }
	rf.log = rf.log[index-rf.lastIncludedIndex:]
	// 更新快照下标Term
	if index == rf.getLastIndex() + 1 {
		rf.lastIncludedTerm = rf.restoreLogTerm(index)
	}
	rf.lastIncludedIndex = index

	if index > rf.lastApplied {
		rf.lastApplied = index
	}

	rf.persister.Save(rf.encodeState(), snapshot)
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
	if rf.lastIncludedIndex >= args.lastIncludedIndex {
		return
	}

	// 将快照后的log切割，快照前的提交
	index := args.lastIncludedIndex
	// for i := index+1; i <= rf.getLastIndex(); i++ {
	// 	tempLog = append(tempLog, rf.restoreLog(i))
	// }
	rf.log = rf.log[index-rf.lastIncludedIndex:]
	rf.lastIncludedIndex = index
	rf.lastIncludedTerm = args.lastIncludedTerm
	if index > rf.commitIndex {
		rf.commitIndex = index
	}
	if index > rf.lastApplied {
		rf.lastApplied = index
	}
	rf.persister.Save(rf.persister.raftstate, args.data)
	applyMsg := ApplyMsg{
		SnapshotValid: true,
		Snapshot: args.data,
		SnapshotTerm: rf.lastIncludedTerm,
		SnapshotIndex: rf.lastIncludedIndex,
	}
	// 异步写，更快
	go func() {
		rf.applyChan <- applyMsg
	}()
}

func (rf *Raft) sendSnapShot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	if ok := rf.peers[server].Call("Raft.InstallSnapShot", args, reply); !ok {
		return 
	}
	rf.mu.Lock()
	rf.mu.Unlock()
	if reply.Term > rf.currentTerm {
		rf.convert2Follower(reply.Term)
	}
	rf.matchIndex[server] = args.lastIncludedIndex
	rf.nextIndex[server] = rf.matchIndex[server] + 1
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

// 获取对应log的Term
func (rf *Raft) LogTerm(index int) int {
	if index < 0 {
		return -1
	}
	return rf.log[index].Term
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()	// 死锁了
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
	}

	// 如果votedFor为空或候选人Id，且候选人的日志至少与接收方的日志一样新，则同意投票（§5.2，§5.4）
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) &&
		(args.LastLogTerm > rf.LogTerm(len(rf.log)-1) || (args.LastLogTerm == rf.LogTerm(len(rf.log)-1) && args.LastLogIndex >= len(rf.log)-1) ) {
			rf.state = Follower
			rf.voteCount = 0
			rf.currentTerm = args.Term
			rf.votedFor = args.CandidateId
			reply.VoteGranted = true
			rf.timer.reset()	// 只有投赞成票才重置时间
	}
	rf.persist()
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	// 一直请求rpc，直到成功
	if ok := rf.peers[server].Call("Raft.RequestVote", args, reply); !ok {
		// fmt.Println("RPC投票连接失败，他的id是:", server)
		return false
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 收到过期的RPC回复,不处理
	if args.Term < rf.currentTerm {
		return false
	}

	// 如果选择投票
	if reply.VoteGranted {
		rf.voteCount++
		if rf.voteCount > len(rf.peers)/2 {
			// fmt.Println("新王登基，他的ID是:" ,rf.me)
			rf.voteCount = 1
			rf.state = Leader
			// fmt.Println("leader是:", rf.me)
			for i, _ := range rf.nextIndex {
				rf.nextIndex[i] = len(rf.log) 	// 初始化为leader的最后一个日志的下一个条目
				// fmt.Println("rf.nextIndex[i]:",rf.nextIndex[i])
			}
			rf.timer.resetHeartBeat()	// 改为设置心跳时间
		}
	} else {
		rf.convert2Follower(reply.Term)
	}
	
	return true
}

//----------------------------------------------心跳部分------------------------------------------------------------
type AppendEntriesArgs struct {
	Term 			int	// leader's term
	LeaderId 		int	// Leader的id
	PrevLogIndex	int	// 前一个日志的日志号
	PrevLogTerm 	int	// 前一个日志的任期号
	Entries			[]logEntry	// 当前日志体
	LeaderCommit	int	// leader的已提交日志号	若leadercommit>commitIndex，那么把commitIndex设为min(若leadercommit, index of last new entry)
}

type AppendEntriesReply struct {
	Term	int	// 自己当前的任期号
	Success	bool	// 如果follower包括前一个日志，则true
	CommitIndex  int	// 用于返回与Leader.Term的匹配项,方便同步日志(帮助我们更快定位)
}

// 将日志写入管道
func (rf *Raft) applyLogs() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.commitIndex > rf.getLastIndex() {
		fmt.Println("出现错误 : raft.go commitlogs()")
	}
	// fmt.Println("开始写入管道")
	// 初始化是-1
	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {	
		rf.applyChan <- ApplyMsg{
			CommandValid: true,
			Command: rf.log[i - rf.lastIncludedIndex].Command,
			CommandIndex: i + 1,
		}
	}
	rf.lastApplied = rf.commitIndex
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// 一直请求rpc，直到成功
	if ok := rf.peers[server].Call("Raft.AppendEntries", args, reply); !ok {
		// fmt.Println("RPC心跳连接失败,他的id是:", server)
		return 
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != Leader || args.Term < rf.currentTerm || rf.currentTerm != args.Term {
		return 
	}

	// 自己的term没别人的大，变为follower
	if rf.currentTerm < reply.Term {
		rf.convert2Follower(reply.Term)
		return
	}

	if reply.Success {
		rf.nextIndex[server] = reply.CommitIndex + 1	// CommitIndex为对端确定两边相同的index 加上1就是下一个需要发送的日志
		rf.matchIndex[server] = rf.nextIndex[server] - 1
		if rf.nextIndex[server]  > len(rf.log)  + rf.lastIncludedIndex{	
			rf.nextIndex[server] = len(rf.log) + rf.lastIncludedIndex
			rf.matchIndex[server] = rf.nextIndex[server] - 1
		}

		commitCount := 1	// 自己
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			if rf.matchIndex[i] >= rf.matchIndex[server] {
				// fmt.Println("机器数量为:", len(rf.peers), "commitCount:", commitCount)
				commitCount++
			}
		}
		// fmt.Printf("commitCount:%d\ncommitIndex: %d, rf.matchIndex[server]: %d\nrf.log[rf.matchIndex[server]].Term: %d, rf.currentTerm: %d", commitCount,rf.commitIndex,rf.matchIndex[server],rf.log[rf.matchIndex[server]].Term,rf.currentTerm )
		if commitCount >= len(rf.peers)/2+1 && // 超过一半的数量接收日志了
			rf.commitIndex < rf.matchIndex[server] && 	// 保证幂等性 即同一条日志正常只会commit一次
			rf.restoreLogTerm(rf.matchIndex[server]) == rf.currentTerm {
				rf.commitIndex = rf.matchIndex[server]
				go rf.applyLogs()	// 提交日志
			}
	} else {
		rf.nextIndex[server] = reply.CommitIndex + 1
		if rf.nextIndex[server] > len(rf.log) + rf.lastApplied {
			rf.nextIndex[server] = len(rf.log) + rf.lastIncludedIndex
		}
	}
	rf.persist()
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.Success = false
	reply.CommitIndex  = 0
	// 收到rpc的term比自己的小 (§5.1)
	if args.Term < rf.currentTerm {	// 并通知leader变为follower
		return 
	} 
	
	if args.Term > rf.currentTerm {	// 承认对方是leader
		rf.convert2Follower(args.Term)
	}
	
	if args.PrevLogIndex >= rf.lastIncludedIndex &&	// 首先leader要有日志
			(rf.getLastIndex() < args.PrevLogIndex ||	// 1. preLogIndex大于当前日志最大下标，说明缺失日志，拒绝附加
			rf.restoreLogTerm(args.PrevLogIndex) != args.PrevLogTerm) {	// 2. 在相同index下日志不同
		reply.CommitIndex  = rf.getLastIndex()
		if reply.CommitIndex  > args.PrevLogIndex {
			reply.CommitIndex  = args.PrevLogIndex	// 多出来的日志会被舍弃掉，需要和leader同步
		}
		if reply.CommitIndex >= rf.lastIncludedIndex {
			curTerm := rf.restoreLogTerm(reply.CommitIndex)
			for reply.CommitIndex  >= rf.lastIncludedIndex {
				if rf.restoreLogTerm(reply.CommitIndex) == curTerm {	// speed up，更快找到下标
					reply.CommitIndex--
				} else {
					break
				}
			}
		}
		
		reply.Success = false	// 返回false说明此节点日志没有跟上leader，或者有多余日志，或者日志有冲突
	} else if args.Entries == nil {	// heartbeat 用于更新状态
		if rf.lastApplied < args.LeaderCommit {
			rf.commitIndex = args.LeaderCommit
			go rf.applyLogs()	// 提交日志
		}
		reply.CommitIndex  = rf.getLastIndex()	// 用于leader更新对应主机的nextIndex
		reply.Success = true
	} else {	// 需要同步日志
		// fmt.Println("同步日志开始, lastApplied:", rf.lastApplied, "LeaderCommit: ", args.LeaderCommit)
		rf.log = rf.log[:args.PrevLogIndex + 1 - rf.lastIncludedIndex]	// 第一次调用的时候prevlogIndex为-1
		rf.log = append(rf.log, args.Entries...)	// 将args中后面的日志一次性全部添加进log

		if rf.lastApplied < args.LeaderCommit {
			rf.commitIndex = args.LeaderCommit	// 与leader同步信息
			go rf.applyLogs()
		}

		reply.CommitIndex  = rf.getLastIndex()	// 用于leader更新对应主机的nextIndex
		if args.LeaderCommit > rf.commitIndex && args.LeaderCommit < rf.getLastIndex(){
			reply.CommitIndex  = args.LeaderCommit	// 令 commitIndex 等于 leaderCommit 和 新日志条目索引值中较小的一个
		}
	}	
	rf.persist()
	rf.timer.reset()	
}



func (rf *Raft) convert2Follower(term int) {
	rf.currentTerm = term
	rf.state = Follower
	rf.voteCount = 0
	rf.votedFor = -1
	rf.persist()
	rf.timer.reset()
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

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != Leader {
		return index, term, false
	}
	// 添加新日志
	e := logEntry{command, rf.currentTerm}
	rf.log = append(rf.log, e)

	index = rf.getLastIndex()+1
	term = rf.currentTerm
	rf.persist()
	// fmt.Printf("lastapplied:%d, index:%d\n", rf.lastApplied, rf.commitIndex)
	return index, term, true
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

func (rf *Raft) ticker() {
	for rf.killed() == false {
		select {
		case <-rf.timer.timer.C:
			rf.mu.Lock()
			switch rf.state {
			case Follower: 	// follower->candidate
				rf.state = Candidate
				fallthrough
			case Candidate:	// 成为候选人，开始拉票
				rf.currentTerm++
				rf.voteCount = 1
				rf.timer.reset()
				rf.votedFor = rf.me

				// 开始拉票选举
				for i := 0; i < len(rf.peers); i++ {
					if rf.me == i {	// 排除自己
						continue
					}
					args := RequestVoteArgs{Term: rf.currentTerm, CandidateId: rf.me, LastLogIndex:  rf.getLastIndex()}
					if len(rf.log) > 0 {
						args.LastLogTerm = rf.getLastTerm()
					}

					reply := RequestVoteReply{}
					// fmt.Println("发送选举,选举人是:",rf.me, "follower是:", i, "票数为：", rf.voteCount)
					go rf.sendRequestVote(i, &args, &reply)
				}
			case Leader:
				rf.timer.resetHeartBeat()
				for i := 0; i < len(rf.peers); i++ {
					if i == rf.me {
						continue
					}
		
					// installSnapshot，如果rf.nextIndex[i]-1小于等lastIncludeIndex,说明followers的日志小于自身的快照状态，将自己的快照发过去
					// 同时要注意的是比快照还小时，已经算是比较落后
					// fmt.Printf("lastIncludedIndex:%d, nextIndex[i]:%d\n", rf.lastIncludedIndex, rf.nextIndex[i])
					if rf.nextIndex[i] > 0 && rf.nextIndex[i] - 1 < rf.lastIncludedIndex {
						fmt.Println("快照")
						args := InstallSnapshotArgs{
							Term: rf.currentTerm,
							LeaderId: rf.me,
							lastIncludedIndex: rf.lastIncludedIndex,
							lastIncludedTerm: rf.lastIncludedTerm,
							data: rf.persister.ReadSnapshot(),
						}
						reply := InstallSnapshotReply{}
						go rf.sendSnapShot(i, &args, &reply)
						continue
					}
					prevLogIndex, prevLogTerm := rf.getPrevLogInfo(i)
					// fmt.Println("prevLogIndex:", prevLogIndex, " prevLogTerm:", prevLogTerm)
					args := AppendEntriesArgs{Term: rf.currentTerm, LeaderId: rf.me, PrevLogIndex: prevLogIndex,
						PrevLogTerm: prevLogTerm, Entries: nil, LeaderCommit: rf.commitIndex}
					if rf.nextIndex[i] - rf.lastIncludedIndex < len(rf.log) {		// 刚成为leader的时候更新过 所以第一次entry为空
						entries := rf.log[rf.nextIndex[i]-rf.lastIncludedIndex:]			//如果日志小于leader的日志的话直接拷贝日志
						args.Entries = make([]logEntry, len(entries))
						copy(args.Entries, entries)
					}

					// fmt.Println("写入的主机是:", i,"len(rf.log):", len(rf.log), "PrevLogIndex:", args.PrevLogIndex, "rf.nextIndex[i]:", rf.nextIndex[i], "rf.currentTerm:", rf.currentTerm, "rf.commitIndex:", rf.commitIndex)
					reply := AppendEntriesReply{}
					go rf.sendAppendEntries(i, &args, &reply)
				}
				
			}
			rf.persist()
			rf.mu.Unlock()
		}
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
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.applyChan = applyCh
	rf.log = make([]logEntry, 0)

	rf.commitIndex = -1
	rf.lastApplied = -1

	rf.lastIncludedIndex = 0
	rf.lastIncludedTerm = 0

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	rf.state = Follower
	rf.voteCount = 0
	rf.timer = Timer{timer: time.NewTicker(time.Duration(150 + rand.Intn(200))*time.Millisecond)}

	
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	// start ticker goroutine to start elections
	go rf.ticker()

	
	return rf
}


