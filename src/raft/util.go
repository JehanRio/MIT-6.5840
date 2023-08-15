package raft

import (
	// "fmt"
	// "fmt"
	// "fmt"
	"log"
	"math/rand"
	"time"
)

const (
	// MoreVoteTime MinVoteTime 定义随机生成投票过期时间范围:(MoreVoteTime+MinVoteTime~MinVoteTime)
	MoreVoteTime = 200
	MinVoteTime  = 150
	// HeartbeatSleep 心脏休眠时间,要注意的是，这个时间要比选举低，才能建立稳定心跳机制
	HeartbeatSleep = 120
	AppliedSleep   = 30
)

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func (rf *Raft) convert2Follower(term int) {
	rf.currentTerm = term
	rf.state = Follower
	rf.voteCount = 0
	rf.votedFor = -1
	rf.persist()
	rf.timer = time.Now()
}

// UpToDate paper中投票RPC的rule2
func (rf *Raft) UpToDate(index int, term int) bool {
	lastIndex := rf.getLastIndex()
	lastTerm := rf.getLastTerm()
	return term > lastTerm || (term == lastTerm && index >= lastIndex)
}

// 获取最后的快照日志下标（这个值更大）
func (rf *Raft) getLastIndex() int {
	return len(rf.log) - 1 + rf.lastIncludedIndex
}

func (rf *Raft) getLastTerm() int {

	return rf.log[len(rf.log)-1].Term
	
}

// 通过快照偏移还原真实日志条目
func (rf *Raft) restoreLog(curIndex int) logEntry {
	// fmt.Println("curIndex:", curIndex, " lastIncludedIndex: ", rf.lastIncludedIndex)
	return rf.log[curIndex - rf.lastIncludedIndex]
}

// 通过快照偏移还原真实日志任期
func (rf *Raft) restoreLogTerm(curIndex int) int {
	// 如果当前index与快照一致/日志为空，直接返回快照/快照初始化信息，否则根据快照计算
	// fmt.Println("curIndex:", curIndex, " lastIncludedIndex: ", rf.lastIncludedIndex)
	if curIndex-rf.lastIncludedIndex == 0 {
		return rf.lastIncludedTerm
	}
	return rf.log[curIndex-rf.lastIncludedIndex].Term
}

// 通过快照偏移还原真实PrevLogInfo
func (rf *Raft) getPrevLogInfo(server int) (int, int) {
	newEntryBeginIndex := rf.nextIndex[server] - 1
	lastIndex := rf.getLastIndex()
	// fmt.Println("newEntryBeginIndex:", newEntryBeginIndex, " lastIndex:", lastIndex)
	// 初始化时传入了一个空日志
	if newEntryBeginIndex == lastIndex+1 {
		newEntryBeginIndex = lastIndex
	}
	return newEntryBeginIndex, rf.restoreLogTerm(newEntryBeginIndex)
}

func min(num int, num1 int) int {
	if num > num1 {
		return num1
	} else {
		return num
	}
}

// 通过不同的随机种子生成不同的过期时间
func generateOverTime(server int64) int {
	rand.Seed(time.Now().Unix() + server)
	return rand.Intn(MoreVoteTime) + MinVoteTime
}

//----------------------------------------------rpc接口------------------------------------------------------------
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendSnapShot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapShot", args, reply)
	return ok
}


