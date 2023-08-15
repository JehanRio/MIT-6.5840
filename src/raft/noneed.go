package raft

// func (rf *Raft) ticker() {
// 	for rf.killed() == false {
// 		select {
// 		case <-rf.timer.timer.C:
// 			rf.mu.Lock()
// 			switch rf.state {
// 			case Follower: 	// follower->candidate
// 				rf.state = Candidate
// 				fallthrough
// 			case Candidate:	// 成为候选人，开始拉票
// 				rf.currentTerm++
// 				rf.voteCount = 1
// 				rf.timer.reset()
// 				rf.votedFor = rf.me

// 				// 开始拉票选举
// 				for i := 0; i < len(rf.peers); i++ {
// 					if rf.me == i {	// 排除自己
// 						continue
// 					}
// 					args := RequestVoteArgs{Term: rf.currentTerm, CandidateId: rf.me, LastLogIndex:  rf.getLastIndex()}
// 					if len(rf.log) > 0 {
// 						args.LastLogTerm = rf.getLastTerm()
// 					}

// 					reply := RequestVoteReply{}
// 					fmt.Println("发送选举,选举人是:",rf.me, "follower是:", i, "票数为：", rf.voteCount)
// 					go rf.sendRequestVote(i, &args, &reply)
// 				}
// 			case Leader:
// 				rf.timer.resetHeartBeat()
// 				for i := 0; i < len(rf.peers); i++ {
// 					if i == rf.me {
// 						continue
// 					}
// 					// installSnapshot，如果rf.nextIndex[i]-1小于等lastIncludeIndex,说明followers的日志小于自身的快照状态，将自己的快照发过去
// 					// 同时要注意的是比快照还小时，已经算是比较落后
// 					// fmt.Printf("lastIncludedIndex:%d, nextIndex[i]:%d\n", rf.lastIncludedIndex, rf.nextIndex[i])
// 					if rf.nextIndex[i] > 0 && rf.nextIndex[i] - 1 < rf.lastIncludedIndex {
// 						fmt.Println("快照")
// 						args := InstallSnapshotArgs{
// 							Term: rf.currentTerm,
// 							LeaderId: rf.me,
// 							lastIncludedIndex: rf.lastIncludedIndex,
// 							lastIncludedTerm: rf.lastIncludedTerm,
// 							data: rf.persister.ReadSnapshot(),
// 						}
// 						reply := InstallSnapshotReply{}
// 						go rf.sendSnapShot(i, &args, &reply)
// 						continue
// 					}

// 					prevLogIndex, prevLogTerm := rf.getPrevLogInfo(i)
// 					fmt.Println("我开始发送心跳了:", rf.me)
// 					args := AppendEntriesArgs{Term: rf.currentTerm, LeaderId: rf.me, PrevLogIndex: prevLogIndex,
// 						PrevLogTerm: prevLogTerm, Entries: nil, LeaderCommit: rf.commitIndex}
// 					if rf.nextIndex[i] - rf.lastIncludedIndex < len(rf.log) {		// 刚成为leader的时候更新过 所以第一次entry为空
// 						entries := rf.log[rf.nextIndex[i]-rf.lastIncludedIndex:]			//如果日志小于leader的日志的话直接拷贝日志
// 						args.Entries = make([]logEntry, len(entries))
// 						copy(args.Entries, entries)
// 					}

// 					// fmt.Println("写入的主机是:", i,"len(rf.log):", len(rf.log), "PrevLogIndex:", args.PrevLogIndex, "rf.nextIndex[i]:", rf.nextIndex[i], "rf.currentTerm:", rf.currentTerm, "rf.commitIndex:", rf.commitIndex)
// 					reply := AppendEntriesReply{}
// 					go rf.sendAppendEntries(i, &args, &reply)
// 				}
				
// 			}
// 			rf.persist()
// 			rf.mu.Unlock()
// 		}
// 	}
// }

// func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
// 	// 一直请求rpc，直到成功
// 	if ok := rf.peers[server].Call("Raft.RequestVote", args, reply); !ok {
// 		// fmt.Println("RPC投票连接失败，他的id是:", server)
// 		return false
// 	}
// 	rf.mu.Lock()
// 	defer rf.mu.Unlock()
// 	// 收到过期的RPC回复,不处理
// 	if args.Term < rf.currentTerm {
// 		return false
// 	}

// 	// 如果选择投票
// 	if reply.VoteGranted {
// 		rf.voteCount++
// 		if rf.voteCount > len(rf.peers)/2 {
// 			// fmt.Println("新王登基，他的ID是:" ,rf.me)
// 			rf.voteCount = 1
// 			rf.state = Leader
// 			fmt.Println("leader是:", rf.me)
// 			for i, _ := range rf.nextIndex {
// 				rf.nextIndex[i] = len(rf.log) 	// 初始化为leader的最后一个日志的下一个条目
// 				// fmt.Println("rf.nextIndex[i]:",rf.nextIndex[i])
// 			}
// 			rf.timer.resetHeartBeat()	// 改为设置心跳时间
// 		}
// 	} else {
// 		rf.convert2Follower(reply.Term)
// 	}
// 	return true
// }

// func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
// 	// 一直请求rpc，直到成功
// 	if ok := rf.peers[server].Call("Raft.AppendEntries", args, reply); !ok {
// 		// fmt.Println("RPC心跳连接失败,他的id是:", server)
// 		return 
// 	}
// 	rf.mu.Lock()
// 	defer rf.mu.Unlock()
// 	if rf.state != Leader || args.Term < rf.currentTerm || rf.currentTerm != args.Term {
// 		return 
// 	}

// 	// 自己的term没别人的大，变为follower
// 	if rf.currentTerm < reply.Term {
// 		rf.convert2Follower(reply.Term)
// 		return
// 	}

// 	if reply.Success {
// 		rf.nextIndex[server] = reply.CommitIndex + 1	// CommitIndex为对端确定两边相同的index 加上1就是下一个需要发送的日志
// 		rf.matchIndex[server] = rf.nextIndex[server] - 1
// 		if rf.nextIndex[server]  > len(rf.log)  + rf.lastIncludedIndex{	
// 			rf.nextIndex[server] = len(rf.log) + rf.lastIncludedIndex
// 			rf.matchIndex[server] = rf.nextIndex[server] - 1
// 		}

// 		commitCount := 1	// 自己
// 		for i := 0; i < len(rf.peers); i++ {
// 			if i == rf.me {
// 				continue
// 			}
// 			if rf.matchIndex[i] >= rf.matchIndex[server] {
// 				// fmt.Println("机器数量为:", len(rf.peers), "commitCount:", commitCount)
// 				commitCount++
// 			}
// 		}
// 		// fmt.Printf("commitCount:%d\ncommitIndex: %d, rf.matchIndex[server]: %d\nrf.log[rf.matchIndex[server]].Term: %d, rf.currentTerm: %d", commitCount,rf.commitIndex,rf.matchIndex[server],rf.log[rf.matchIndex[server]].Term,rf.currentTerm )
// 		if commitCount >= len(rf.peers)/2+1 && // 超过一半的数量接收日志了
// 			rf.commitIndex < rf.matchIndex[server] && 	// 保证幂等性 即同一条日志正常只会commit一次
// 			rf.restoreLogTerm(rf.matchIndex[server]) == rf.currentTerm {
// 				rf.commitIndex = rf.matchIndex[server]
// 				rf.applyLogs()	// 提交日志
// 			}
// 	} else {
// 		rf.nextIndex[server] = reply.CommitIndex + 1
// 		if rf.nextIndex[server] > len(rf.log) + rf.lastApplied {
// 			rf.nextIndex[server] = len(rf.log) + rf.lastIncludedIndex
// 		}
// 	}
// 	rf.persist()
// }