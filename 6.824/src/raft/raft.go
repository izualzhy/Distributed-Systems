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

import "sync"
import "labrpc"
import "sort"
import "time"
import "math/rand"

import "bytes"
import "encoding/gob"

const heart_beat_interval_ms = 100
const min_election_timeout_ms = 500

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

//
// A Go object implementing a single Raft peer.
//
const (
    follower int = iota
    candidate
    leader
)

//LogEntry是AppendEntriesArgs的成员，因此需要大写首字母
type LogEntry struct {
    Command interface{}
    Term int
}

type ChangeToFollower struct {
    term int
    votedFor int
    fromRole int
    isLogEntry bool
}

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
    // 2A
    //CurrentTerm VotedFor Logs需要gob压缩，因此首字母大写
    role int
    //lastest term server has seen(initialized to 0 on first boot, increases monotonically)
    CurrentTerm int
    //candidatedId that received vote in current term(or null if none)
    VotedFor int
    changeToFollower chan ChangeToFollower
    changeToFollowerDone chan bool
    receivedQuit chan bool
    quitCheckRoutine chan bool
    // 2B
    //index of highest log entry known to be committed(initialize to 0, increases monotonically)
    commitIndex int
    //log entries;each entry contains command for state machine, and term when entry was received by leader(first index is 1)
    Logs []LogEntry
    //for each server, index of the next log entry to send to that server(initialized to leader last log index + 1)
    nextIndex []int
    followerAppendEntries chan bool
    //for each server, index of highest log entry known to be replicated on server(initialized to 0, increases monotonically)
    //这里我省略了lastApplied，使用matchIndex[rf.me]替代
    matchIndex []int
    applyCh chan ApplyMsg

    followerTimeout *time.Timer
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).
    rf.mu.Lock()
    defer rf.mu.Unlock()
    term = rf.CurrentTerm
    isleader = (rf.role == leader)
    DPrintf("GetState me:%d term:%d votedFor:%d isleader:%v", rf.me, term, rf.VotedFor, isleader)
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
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
    w := new(bytes.Buffer)
    e := gob.NewEncoder(w)
    e.Encode(rf.CurrentTerm)
    e.Encode(rf.VotedFor)
    e.Encode(rf.Logs)
    data := w.Bytes()
    rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

    rf.mu.Lock()
    defer rf.mu.Unlock()

    r := bytes.NewBuffer(data)
    d := gob.NewDecoder(r)
    d.Decode(&rf.CurrentTerm)
    d.Decode(&rf.VotedFor)
    d.Decode(&rf.Logs)
    DPrintf("[readPersist] me:%v CurrentTerm:%v VotedFor:%v Logs:%v", rf.me, rf.CurrentTerm, rf.VotedFor, rf.Logs)
}

//
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

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
    Term int
    VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
    DPrintf("[RequestVote] me:%v currentTerm:%v args:%v", rf.me, rf.CurrentTerm, args)
    rf.mu.Lock()
    defer rf.mu.Unlock()

    //1. Reply false if term < currentTerm
    if args.Term < rf.CurrentTerm {
        reply.Term = rf.CurrentTerm
        reply.VoteGranted = false
        return
    }

    //2. if votedFor is null or candidateId, and candidate's log is at least as up-to-date as receiver's log, grant vote.
    //Raft determines which of two logs is more up-to-date by comparing the index and term of the last entries in the logs.
    //If the logs have last entries with different terms, then the log with the later term is more up-to-date.
    //If the logs end with the same term, then whichever log is longer is more up-to-date.
    lastLogIndex := len(rf.Logs) - 1
    lastLogTerm := rf.Logs[lastLogIndex].Term

    if args.LastLogTerm < lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex < lastLogIndex) {
        reply.Term = args.Term
        reply.VoteGranted = false
        DPrintf("[RequestVote] not up-to-date me:%d rf.Logs:%v args:%v", rf.me, rf.Logs, args)
    } else if rf.CurrentTerm < args.Term {
        reply.Term = args.Term
        reply.VoteGranted = true
        DPrintf("[RequestVote] me:%d votedFor:%d VoteGranted:true", rf.me, rf.VotedFor)
    } else {
        reply.Term = rf.CurrentTerm
        reply.VoteGranted = false
    }

    //If RPC request or response contains term T > currentTerm,
    //set currentTerm = T, convert to follower
    if rf.CurrentTerm < args.Term {
        DPrintf("[RequestVote] me:%d changeToFollower:{%v, %v}", rf.me, args.Term, args.CandidateId)
        rf.PushChangeToFollower(args.Term, args.CandidateId, false)
    }
}

//AppendEntries
type AppendEntriesArgs struct {
    //2A
    Term int
    LeaderId int

    //2B
    PrevLogIndex int
    PrevLogTerm int
    Entries []LogEntry
    LeaderCommitIndex int
}

type AppendEntriesReply struct {
    //2A
    Term int
    Success bool

    ConflictIndex int
    ConflictTerm int
}

func (rf *Raft) NotifyApplyCh(last_commit int) {
    commitIndex := rf.commitIndex
    //持久化，注意这里不判断commitIndex != last_commit才persist
    //因为对follower而言，这次AppendEntriesArgs可能是新的日志 + 新日志之前的commitIndex
    //在follower返回true之后，leader commit了新的日志，如果follower不持久化而重启导致丢失了这些新的log
    //之后leader重启，该follower同意选取其他日志较少的leader，新的leader可能覆盖掉之前commit的内容。
    rf.persist()

    //这里不能设置为异步push数据到rf.applyCh
    //因为同一server可能连续调用两次，而config里的检查需要对Index有顺序要求
    for i := last_commit + 1; i <= commitIndex; i++ {
        DPrintf("[NotifyApplyCh] me:%d push to applyCh, Index:%v Command:%v", rf.me, i, rf.Logs[i].Command)
        rf.applyCh <- ApplyMsg{Index:i, Command:rf.Logs[i].Command}
    }
}

func (rf *Raft) AppendEntries(request *AppendEntriesArgs, response *AppendEntriesReply) {
    rf.mu.Lock()
    defer rf.mu.Unlock()
    DPrintf("[AppendEntries] me:%d currentTerm:%v received AppendEntriesArgs:%v rf.Logs:%v", rf.me, rf.CurrentTerm, request, rf.Logs)

    //1. Reply false if term < currentTerm
    if rf.CurrentTerm > request.Term {
        response.Term = rf.CurrentTerm
        response.Success = false
    } else {
        log_len := len(rf.Logs)
        //2. Reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
        log_is_less := log_len < request.PrevLogIndex + 1
        log_dismatch := !log_is_less && request.PrevLogIndex > 0 && (rf.Logs[request.PrevLogIndex].Term != request.PrevLogTerm)
        if log_is_less || log_dismatch {
            response.Term = rf.CurrentTerm
            response.Success = false

            if log_is_less {
                response.ConflictTerm = -1
                response.ConflictIndex = log_len
            } else if log_dismatch {
                response.ConflictTerm = rf.Logs[request.PrevLogIndex].Term
                for i := 0; i < log_len; i++ {
                    if rf.Logs[i].Term == response.ConflictTerm {
                        response.ConflictIndex = i
                        break
                    }
                }
            }
        } else {
            //3. If an existing entry conflicts with a new one(same index but different terms), delete the existing entry and all that follow it.
            if len(rf.Logs) - 1 != request.PrevLogIndex {
                rf.Logs = rf.Logs[:request.PrevLogIndex + 1]
            }
            //4. Append any new entries not already in the log
            rf.Logs = append(rf.Logs, request.Entries...)

            last_commit_index := rf.commitIndex
            //5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
            last_new_entry := len(rf.Logs) - 1
            if last_new_entry > request.LeaderCommitIndex {
                rf.commitIndex = request.LeaderCommitIndex
            } else {
                rf.commitIndex = last_new_entry
            }
            rf.NotifyApplyCh(last_commit_index);

            response.Term = rf.CurrentTerm
            response.Success = true
        }

        //if RPC request or response contains term T > currentTerm:
        //set currentTerm = T, convert to follower
        DPrintf("[AppendEntries] me:%d changeToFollower <- {%v, %v} response:%v log_is_less:%v log_dismatch:%v", rf.me, request.Term, request.LeaderId, response, log_is_less, log_dismatch)
        rf.PushChangeToFollower(request.Term, request.LeaderId, true)
    }
    DPrintf("[AppendEntries] me:%d currentTerm:%d votedFor:%d logs:%v", rf.me, rf.CurrentTerm, rf.VotedFor, rf.Logs)
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
    // DPrintf("[sendAppendEntries] to server:%d request:%v", server, args)
    ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
    return ok
}

func (rf *Raft) PushChangeToFollower(term int, leaderId int, isLogEntry bool) {
    rf.changeToFollower <- ChangeToFollower{term, leaderId, rf.role, isLogEntry}
    <- rf.changeToFollowerDone
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

func (rf *Raft) makeAppendEntryRequest(server_index int) (*AppendEntriesArgs, int) {
    rf.mu.Lock()
    defer rf.mu.Unlock()

    if rf.role == follower {
        return nil, -1
    }
    newLogEntryStartIndex := rf.nextIndex[server_index]
    endIndex := len(rf.Logs)

    request := &AppendEntriesArgs{}
    request.Term = rf.CurrentTerm
    request.LeaderId = rf.me

    prevLogIndex := newLogEntryStartIndex - 1
    if prevLogIndex < 0 {
        prevLogIndex = 0
    }
    request.PrevLogIndex = prevLogIndex
    request.PrevLogTerm = rf.Logs[prevLogIndex].Term

    if newLogEntryStartIndex != -1 && endIndex > 0 {
        request.Entries = rf.Logs[newLogEntryStartIndex:endIndex]
    }
    request.LeaderCommitIndex = rf.commitIndex

    return request, endIndex
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
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
    rf.mu.Lock()
    defer rf.mu.Unlock()
    DPrintf("[Start] me:%v command:%v rf.role:%v", rf.me, command, rf.role)

    isLeader = (rf.role == leader)

    if isLeader {
        index = rf.nextIndex[rf.me]
        term = rf.CurrentTerm

        //If command received from client: append entry to local log, respond after entry applied to state machine(push to ApplyMsg)
        rf.Logs = append(rf.Logs, LogEntry{command, term})
        rf.nextIndex[rf.me]++
        rf.matchIndex[rf.me] = rf.nextIndex[rf.me] - 1

        rf.persist()

        DPrintf("[Start] me:%d command:%v index:%v rf.Logs:%v", rf.me, command, index, rf.Logs)
    }

    return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
    DPrintf("[Kill] me:%d", rf.me)
    //加锁避免AppendEntries线程里写了ApplyMsg并返回response，但是未来得及持久化
    //该线程Kill然后Make
    rf.mu.Lock()
    close(rf.receivedQuit)
    close(rf.quitCheckRoutine)
    rf.mu.Unlock()
    DPrintf("[Kill] me:%d return", rf.me)
}

func (rf *Raft) InitNextIndex()  {
    for i := 0; i < len(rf.peers); i++ {
        rf.nextIndex[i] = 1
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
    rf.changeToFollower = make(chan ChangeToFollower)
    rf.changeToFollowerDone = make(chan bool)
    rf.receivedQuit = make(chan bool)
    rf.quitCheckRoutine = make(chan bool)
    rf.followerAppendEntries = make(chan bool)
	// Your initialization code here (2A, 2B, 2C).
    rf.CurrentTerm = 0
    rf.VotedFor = -1

    rf.applyCh = applyCh
    rf.nextIndex = make([]int, len(peers))
    rf.Logs = append(rf.Logs, LogEntry{-1, rf.CurrentTerm})
    rf.InitNextIndex()
    rf.matchIndex = make([]int, len(peers))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
    rf.followerTimeout = time.NewTimer(time.Duration(rf.ElectionTimeout()) * time.Millisecond)

    //When servers start up, they begin as followers.
    go rf.BeFollower()
    go rf.CheckMatchIndexAndSetCommitIndex()

    DPrintf("[Make] me:%d return", rf.me)

	return rf
}

func (rf *Raft) ElectionTimeout() int {
    r := rand.New(rand.NewSource(time.Now().UnixNano()))
    return r.Intn(min_election_timeout_ms) + min_election_timeout_ms
}

func (rf *Raft) BeCandidate() {
    DPrintf("[BeCandidate] me:%v begin.", rf.me)
    rf.role = candidate
    for {
        vote_ended := make(chan bool, len(rf.peers))
        go rf.StartElection(vote_ended)

        select {
        case v := <- rf.changeToFollower:
            //If AppendEntries RPC received from new leader:convert to follower
            DPrintf("[BeCandidate] me:%d changeToFollower:%v", rf.me, v)
            go rf.TransitionToFollower(v)
            return
        case <- rf.receivedQuit:
            DPrintf("[BeCandidate] me:%d quit", rf.me)
            return
        case win := <- vote_ended:
            DPrintf("[BeCandidate] me:%d CurrentTerm:%v win:%v", rf.me, rf.CurrentTerm, win)
            //If vote received from majority of servers:become leader
            if win {
                go rf.BeLeader()
                return
            }
        case <- time.After(time.Duration(rf.ElectionTimeout()) * time.Millisecond):
            //If election timeout elapses:start new election
            DPrintf("[BeCandidate] election timeout, start new election. me:%v CurrentTerm:%v", rf.me, rf.CurrentTerm)
        }
    }
}

func (rf *Raft) BeLeader() {
    //异步避免 AE/RV里get lock后尝试push channel
    //而这里尝试getlock后才pop channel
    go func() {
        rf.mu.Lock()
        defer rf.mu.Unlock()
        if rf.role != candidate {
            return
        }
        //When a leader first comes to power
        //it initializes all nextIndex values to the index just after the last one in its log.
        for i := 0; i < len(rf.nextIndex); i++ {
            rf.nextIndex[i] = len(rf.Logs)
        }
        rf.matchIndex[rf.me] = rf.nextIndex[rf.me] - 1

        //放在最后一步，在rf.SendLogEntryMessageToAll前判断是否是leader角色
        rf.role = leader
    }()

    for {
        select {
        case v := <- rf.changeToFollower:
            //turn to follower
            DPrintf("[BeLeader] me:%d changeToFollower:%v", rf.me, v)
            go rf.TransitionToFollower(v)
            return
        case <- rf.receivedQuit:
            DPrintf("[BeLeader] me:%d quit", rf.me)
            return
        default:
            DPrintf("[BeLeader] me:%d default. rf.role:%v", rf.me, rf.role)
            //等待直到leader状态初始化完成
            if rf.role == leader {
                //Upon election: send initial empty AppendEntries RPCs(heartbeat) to each server;
                //repeat during idle periods to prevent election timeouts.
                rf.SendLogEntryMessageToAll()
                //Hint: The tester requires that the leader send heartbeat RPCs no more than then times persecond
                time.Sleep(heart_beat_interval_ms * time.Millisecond)
            }
        }
    }
}

func (rf* Raft) BeFollower() {
    DPrintf("[BeFollower] me:%d before for looooooooop", rf.me)
    rf.role = follower

    for {
        DPrintf("[BeFollower] me:%d begin wait select", rf.me)

        select {
        case v := <- rf.changeToFollower:
            //A server remains in follower state as long as it receives valid RPCs from a leader or candidate.
            // continue BeFollower with another leader(maybe)
            DPrintf("[BeFollower] me:%d CurrentTerm:%v changeToFollower:%v", rf.me, rf.CurrentTerm, v)
            if v.term > rf.CurrentTerm {
                go rf.TransitionToFollower(v)
                return
            }
            rf.changeToFollowerDone <- true
            if v.isLogEntry {
                rf.followerTimeout.Reset(time.Duration(rf.ElectionTimeout()) * time.Millisecond)
            }
        case <- rf.followerTimeout.C:
            //If a follower receives no communication over a period of time called the election timeout,
            //then it assumes thers is no viable leader and begins an election to choose a new leader.
            DPrintf("[BeFollower] me:%d timeout", rf.me)
            go rf.BeCandidate()
            return
        case <- rf.receivedQuit:
            DPrintf("[BeFollower] me:%d quit", rf.me)
            return
        }
    }
}

func (rf *Raft) TransitionToFollower(changeToFollower ChangeToFollower) {
    DPrintf("[TransitionToFollower] me:%d enter before lock changeToFollower:%v CurrentTerm:%d", rf.me, changeToFollower, rf.CurrentTerm)
    rf.role = follower
    rf.CurrentTerm = changeToFollower.term
    if changeToFollower.votedFor != -1 {
        rf.VotedFor = changeToFollower.votedFor
    }
    //为什么重置nextIndex?
    //避免本身是旧leader，makeAppendEntryRequest还在使用nextIndex + rf.Logs构造AppendEntriesArgs
    //而如果rf.Logs被新的leader截断，那么可能出现nextIndex > len(rf.Logs)情况，导致makeAppendEntryRequest里index out of range
    //为什么重置nextIndex为0?
    //注意nextIndex不能设置为len(logs)，比如以下场景：
    //1. 发送AppendEntries时response，转化为follower，此时rf.Logs未修改，nextIndex = len(rf.Logs)
    //2. 接着收到新leader的AppendEntries，可能删减rf.Logs
    //3. BeFollower里的follower的case v:= <- changeToFollower触发，调用go rf.TransitionToFollower(v)后返回，释放AppendEntries函数里的锁
    //4. makeAppendEntryRequest使用删减后的rf.Logs 与 未修改的nextIndex，可能出错
    //5. go rf.TransitionToFollower(v)异步运行到这里，才设置nextIndex = len(rf.Logs)
    rf.InitNextIndex()
    rf.persist()
    rf.changeToFollowerDone <- true

    if changeToFollower.fromRole != follower || changeToFollower.isLogEntry {
        rf.followerTimeout.Reset(time.Duration(rf.ElectionTimeout()) * time.Millisecond)
    }

    rf.BeFollower()
}

func CheckIfWinHalfVote(voted []bool, server_count int) bool {
    voted_count := 0
    for  i := 0; i < server_count; i++ {
        if voted[i] {
            voted_count++
        }
    }
    //win the vote if iut receives votes from a majority of the servers in the ful cluster for the same term.
    return voted_count >= (server_count/2 + 1)
}

func (rf *Raft) StartElection(win chan bool) {
    //On conversion to candidate, start election:
    //1. Increment currentTerm
    //2. Vote for self
    //3. Reset election timer
    //4. Send RequestVote RPCs to all other servers
    server_count := len(rf.peers)
    voted := make([]bool, server_count)

    rf.mu.Lock()

    //如果拿到锁后发现已经不是candidate，不再发起选举。
    if rf.role != candidate {
        rf.mu.Unlock()
        return
    }

    rf.CurrentTerm++
    rf.VotedFor = rf.me
    rf.persist()
    voted[rf.me] = true

    var request RequestVoteArgs
    request.Term = rf.CurrentTerm
    request.CandidateId = rf.me
    request.LastLogIndex = len(rf.Logs) - 1
    request.LastLogTerm = rf.Logs[request.LastLogIndex].Term
    rf.mu.Unlock()

    //send request vote RPCs to all other servers
    for i := 0; i < server_count; i++ {
        if i != rf.me {
            //issues RequestVote RPCs in parallel to each of the other servers in the cluster
            go func(server_index int, voted []bool) {
                var reply RequestVoteReply
                DPrintf("[StartElection] sendRequestVote from me:%d to server_index:%d request:%v", rf.me, server_index, request)
                send_ok := rf.sendRequestVote(server_index, &request, &reply)
                DPrintf("[StartElection] sendRequestVote from me:%d to server_index:%d currentTerm:%d send_ok:%v reply:%v", rf.me, server_index, rf.CurrentTerm, send_ok, reply)

                voted[server_index] = send_ok && reply.VoteGranted

                if CheckIfWinHalfVote(voted, server_count) {
                    DPrintf("[StartElection] rf.me:%d voted:%v rf.Logs:%v rf.CurrentTerm:%v request:%v", rf.me, voted, rf.Logs, rf.CurrentTerm, request)
                    win <- true
                }
            }(i, voted)
        }
    }
}

func (rf *Raft) HandleInconsistency(server_index int, response *AppendEntriesReply) {
    rf.mu.Lock()
    DPrintf("[HandleInconsistency] before log inconsistency: me:%d server_index:%d rf.nextIndex:%v", rf.me, server_index, rf.nextIndex)

    nextIndex := -1
    for j := 1; j < len(rf.Logs) - 1; j++ {
        if rf.Logs[j].Term == response.Term && rf.Logs[j + 1].Term != response.Term {
            nextIndex = j + 1
            break
        }
    }
    if response.ConflictTerm == -1 || nextIndex == -1 {
        rf.nextIndex[server_index] = response.ConflictIndex
    } else {
        rf.nextIndex[server_index] = nextIndex
    }
    DPrintf("[HandleInconsistency] after log inconsistency: me:%d server_index:%d rf.nextIndex:%v", rf.me, server_index, rf.nextIndex)
    rf.mu.Unlock()
}

func (rf *Raft) SendLogEntryMessageToAll() {
    DPrintf("[SendLogEntryMessageToAll] me:%d begin", rf.me)
    server_count := len(rf.peers)

    for i := 0; i < server_count; i++ {
        if i != rf.me {
            go func(server_index int) {
                //makeAppendEntryRequest需要加锁
                //因此需要放到该异步函数里
                //如果放到外面，由于对于leader，SendLogEntryMessageToAll是一个同步函数
                //例如以下场景会出现deadlock: AppendEntries获取了锁，尝试changeToFollower <- true
                //如果同步的话，则需要先获取锁，然后case <- changeToFollower
                request, max_log_entry_index := rf.makeAppendEntryRequest(server_index)
                if request == nil {
                    return
                }
                var response AppendEntriesReply
                DPrintf("[SendLogEntryMessageToAll] from me:%d to server_index:%d request:%v", rf.me, server_index, request)
                send_ok := rf.sendAppendEntries(server_index, request, &response)

                if send_ok {
                    //If successful: update nextIndex and matchIndex for follower.
                    if response.Success {
                        rf.nextIndex[server_index] = max_log_entry_index
                        rf.matchIndex[server_index] = max_log_entry_index - 1
                        rf.followerAppendEntries <- true
                    } else {
                        //If AppendEntries fails because of log inconsistency:
                        //decrement nextIndex and retry.
                        if response.Term == rf.CurrentTerm {
                            rf.HandleInconsistency(server_index, &response)
                        } else if response.Term > rf.CurrentTerm {
                            //If RPC request or response contains term T > currentTerm
                            //set currentTerm = T, convert to follower.
                            DPrintf("[SendLogEntryMessageToAll] server:%v push change me:%v response:%v currentTerm:%d", server_index, rf.me, response, rf.CurrentTerm)
                            //we don't know who is leader, set -1.
                            rf.PushChangeToFollower(response.Term, -1, false)
                        }
                    }
                }
                DPrintf("[SendLogEntryMessageToAll] from me:%d to server_index:%d send_ok:%t response:%v rf.nextIndex:%v", rf.me, server_index, send_ok, response, rf.nextIndex)
            }(i)
        }
    }
    DPrintf("[SendLogEntryMessageToAll] me:%d end", rf.me)
}

func (rf *Raft) CheckMatchIndexAndSetCommitIndex() {
    DPrintf("[CheckMatchIndexAndSetCommitIndex] rf.me:%v begin.", rf.me)
    majorityIndex := (len(rf.peers) - 1) / 2
    for {
        select {
        case <- rf.quitCheckRoutine:
            DPrintf("[CheckMatchIndexAndSetCommitIndex] rf.me:%v quit.", rf.me)
            return
        case <- rf.followerAppendEntries:
        }

        rf.mu.Lock()
        //If there exists an N such that N > commitIndex, a majority of matchIndex[i] >= N,
        //and log[N].term == currentTerm: set commitIndex = N
        last_commit_index := rf.commitIndex
        matchIndex := append([]int{}, rf.matchIndex...)
        sort.Ints(matchIndex)

        commit_index_majority := matchIndex[majorityIndex]
        new_commit_on_majority := (last_commit_index < commit_index_majority) && (commit_index_majority < len(rf.Logs)) && (rf.Logs[commit_index_majority].Term == rf.CurrentTerm)
        //for example, if that entry is stored on every server
        commit_index_all := matchIndex[0]
        new_commit_on_all := (last_commit_index < commit_index_all) && (commit_index_all < len(rf.Logs))
        DPrintf("[CheckMatchIndexAndSetCommitIndex] rf.me:%v matchIndex:%v last_commit_index:%v", rf.me, rf.matchIndex, last_commit_index)

        if new_commit_on_majority {
            DPrintf("[CheckMatchIndexAndSetCommitIndex] commitIndex:%v->%v by majority me:%d currentTerm:%v rf.Logs:%v", last_commit_index, commit_index_majority, rf.me, rf.CurrentTerm, rf.Logs)
            rf.commitIndex = commit_index_majority
            rf.NotifyApplyCh(last_commit_index)
        } else if new_commit_on_all {
            DPrintf("[CheckMatchIndexAndSetCommitIndex] commitIndex:%v->%v by all me:%d currentTerm:%v rf.Logs:%v", last_commit_index, commit_index_all, rf.me, rf.CurrentTerm, rf.Logs)
            rf.commitIndex = commit_index_all
            rf.NotifyApplyCh(last_commit_index)
        }
        rf.mu.Unlock()
    }
    DPrintf("[CheckMatchIndexAndSetCommitIndex] rf.me:%v end.", rf.me)
}
