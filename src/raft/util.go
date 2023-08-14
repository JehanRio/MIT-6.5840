package raft

import (
	// "fmt"
	"log"
)

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

// 获取最后的快照日志下标（这个值更大）
func (rf *Raft) getLastIndex() int {
	return len(rf.log) - 1 + rf.lastIncludedIndex
}

func (rf *Raft) getLastTerm() int {
	// 因为初始有填充一个，否则最直接len == 0
	if len(rf.log)-1 == 0 {
		return rf.lastIncludedTerm
	} else {
		return rf.log[len(rf.log)-1].Term
	}
}

// 通过快照偏移还原真实日志条目
func (rf *Raft) restoreLog(curIndex int) logEntry {
	return rf.log[curIndex - rf.lastIncludedIndex]
}

// 通过快照偏移还原真实日志任期
func (rf *Raft) restoreLogTerm(curIndex int) int {
	// 如果当前index与快照一致/日志为空，直接返回快照/快照初始化信息，否则根据快照计算
	if curIndex-rf.lastIncludedIndex == 0 {
		return rf.lastIncludedTerm
	}
	// fmt.Printf("[GET] curIndex:%v,rf.lastIncludeIndex:%v\n", curIndex, rf.lastIncludedIndex)
	return rf.log[curIndex-rf.lastIncludedIndex].Term
}

// 通过快照偏移还原真实PrevLogInfo
func (rf *Raft) getPrevLogInfo(server int) (int, int) {
	newEntryBeginIndex := rf.nextIndex[server] - 1	
	if newEntryBeginIndex-rf.lastIncludedIndex >= 0 {
		return newEntryBeginIndex, rf.restoreLogTerm(newEntryBeginIndex)
	}
	return newEntryBeginIndex, 0
}