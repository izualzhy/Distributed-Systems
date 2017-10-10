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

// import "bytes"
// import "encoding/gob"

const heart_beat_interval_ms = 100
const min_election_timeout_ms = 400

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
    role int
    currentTerm int
    votedFor int
    changeToFollower chan ChangeToFollower
    changeToFollowerDone chan bool
    logInconsistency chan bool
    receivedQuit chan bool
    // 2B
    commitIndex int
    logs []LogEntry
    nextIndex []int
    followerAppendEntries chan bool
    matchIndex []int
    applyCh chan ApplyMsg
    receivedCommand chan bool
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).
    rf.mu.Lock()
    defer rf.mu.Unlock()
    term = rf.currentTerm
    isleader = (rf.role == leader)
    DPrintf("GetState me:%d term:%d votedFor:%d isleader:%d", rf.me, term, rf.votedFor, isleader)
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
    DPrintf("[RequestVote] me:%v currentTerm:%v args:%v", rf.me, rf.currentTerm, args)
    rf.mu.Lock()
    defer rf.mu.Unlock()

    //1. Reply false if term < currentTerm
    if args.Term < rf.currentTerm {
        reply.Term = rf.currentTerm
        reply.VoteGranted = false
        return
    }

    //2. if votedFor is null or candidateId, and candidate's log is at least as up-to-date as receiver's log, grant vote.
    //Raft determines which of two logs is ore up-to-date by comparing the index and term of the last entries int the logs.
    //If the logs have last entries with different terms, then the log with the later term is more up-to-date.
    //If the logs end with the same term, then whichever log is longer is more up-to-date.
    lastLogIndex := len(rf.logs) - 1
    lastLogTerm := rf.logs[lastLogIndex].Term

    if args.LastLogTerm < lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex < lastLogIndex) {
        reply.Term = args.Term
        reply.VoteGranted = false
        DPrintf("[RequestVote] not up-to-date me:%d rf.logs:%v args:%v", rf.me, rf.logs, args)
    } else if rf.currentTerm < args.Term {
        reply.Term = args.Term
        reply.VoteGranted = true
        DPrintf("[RequestVote] me:%d votedFor:%d VoteGranted:true", rf.me, rf.votedFor)
    } else {
        reply.Term = rf.currentTerm
        reply.VoteGranted = false
    }

    //If RPC request or response contains term T > currentTerm,
    //set currentTerm = T, convert to follower
    if rf.currentTerm < args.Term {
        DPrintf("[RequestVote] me:%d changeToFollower:{%v, %v}", rf.me, args.Term, args.CandidateId)
        rf.PushChangeToFollower(args.Term, args.CandidateId)
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
    //这里不能设置为异步，因为同一server可能连续调用两次，而config里的检查需要对Index有顺序要求
    // go func() {
        for i := last_commit + 1; i <= commitIndex; i++ {
            DPrintf("[NotifyApplyCh] me:%d push to applyCh, Index:%v Command:%v", rf.me, i, rf.logs[i].Command)
            rf.applyCh <- ApplyMsg{Index:i, Command:rf.logs[i].Command}
        }
    // }()
}

func (rf *Raft) AppendEntries(request *AppendEntriesArgs, response *AppendEntriesReply) {
    DPrintf("[AppendEntries] me:%d currentTerm:%v received AppendEntriesArgs:%v rf.logs:%v", rf.me, rf.currentTerm, request, rf.logs)
    rf.mu.Lock()
    defer rf.mu.Unlock()

    //1. Reply false if term < currentTerm
    if rf.currentTerm > request.Term {
        response.Term = rf.currentTerm
        response.Success = false
    } else {
        log_len := len(rf.logs)
        //2. Reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
        log_is_less := log_len < request.PrevLogIndex + 1
        log_dismatch := !log_is_less && request.PrevLogIndex > 0 && (rf.logs[request.PrevLogIndex].Term != request.PrevLogTerm)
        if log_is_less || log_dismatch {
            response.Term = rf.currentTerm
            response.Success = false

            if log_is_less {
                response.ConflictTerm = -1
                response.ConflictIndex = log_len
            } else if log_dismatch {
                response.ConflictTerm = rf.logs[request.PrevLogIndex].Term
                for i := 0; i < log_len; i++ {
                    if rf.logs[i].Term == response.ConflictTerm {
                        response.ConflictIndex = i
                        break
                    }
                }
            }
        } else {
            //3. If an existing entry conflicts with a new one(same index but different terms), delete the existing entry and all that follow it.
            if len(rf.logs) - 1 != request.PrevLogIndex {
                rf.logs = rf.logs[:request.PrevLogIndex + 1]
            }
            //4. Append any new entries not already in the log
            rf.logs = append(rf.logs, request.Entries...)

            last_commit_index := rf.commitIndex
            //5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
            last_new_entry := len(rf.logs) - 1
            if last_new_entry > request.LeaderCommitIndex {
                rf.commitIndex = request.LeaderCommitIndex
            } else {
                rf.commitIndex = last_new_entry
            }
            rf.NotifyApplyCh(last_commit_index);

            response.Term = rf.currentTerm
            response.Success = true
        }

        //if RPC request or response contains term T > currentTerm:
        //set currentTerm = T, convert to follower
        DPrintf("[AppendEntries] me:%d changeToFollower <- {%v, %v} response:%v log_is_less:%v log_dismatch:%v", rf.me, request.Term, request.LeaderId, response, log_is_less, log_dismatch)
        rf.PushChangeToFollower(request.Term, request.LeaderId)
    }
    DPrintf("[AppendEntries] me:%d currentTerm:%d votedFor:%d logs:%v", rf.me, rf.currentTerm, rf.votedFor, rf.logs)
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
    // DPrintf("[sendAppendEntries] to server:%d request:%v", server, args)
    ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
    return ok
}

func (rf *Raft) PushChangeToFollower(term int, leaderId int) {
    rf.changeToFollower <- ChangeToFollower{term, leaderId}
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

func (rf *Raft) makeAppendEntryRequest(newLogEntryStartIndex int, endIndex int) *AppendEntriesArgs {
    // DPrintf("[makeAppendEntryRequest] me:%d rf.nextIndex:%v newLogEntryStartIndex:%v endIndex:%v rf.logs:%v", rf.me, rf.nextIndex, newLogEntryStartIndex, endIndex, rf.logs)
    request := &AppendEntriesArgs{}
    request.Term = rf.currentTerm
    request.LeaderId = rf.me

    prevLogIndex := newLogEntryStartIndex - 1
    if prevLogIndex < 0 {
        prevLogIndex = 0
    }
    request.PrevLogIndex = prevLogIndex
    request.PrevLogTerm = rf.logs[prevLogIndex].Term

    if newLogEntryStartIndex != -1 && endIndex > 0 {
        request.Entries = rf.logs[newLogEntryStartIndex:endIndex]
    }
    request.LeaderCommitIndex = rf.commitIndex
    // DPrintf("[makeAppendEntryRequest] me:%v prevLogIndex:%v newLogEntryStartIndex:%v endIndex:%v request:%v", rf.me, prevLogIndex, newLogEntryStartIndex, endIndex, request)

    return request
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

    DPrintf("[Start] me:%v command:%v rf.role:%v", rf.me, command, rf.role)
	// Your code here (2B).
    rf.mu.Lock()
    defer rf.mu.Unlock()

    isLeader = (rf.role == leader)

    if isLeader {
        index = rf.nextIndex[rf.me]
        term = rf.currentTerm

        rf.logs = append(rf.logs, LogEntry{command, term})
        rf.nextIndex[rf.me]++
        rf.matchIndex[rf.me] = rf.nextIndex[rf.me] - 1

        go func() {
            rf.receivedCommand <- true
        }()
    }

    DPrintf("[Start] rf.me:%d command:%v index:%v rf.logs:%v", rf.me, command, index, rf.logs)
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
    rf.receivedQuit <- true
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
    rf.logInconsistency = make(chan bool)
    rf.receivedQuit = make(chan bool)
    rf.followerAppendEntries = make(chan bool)
    rf.receivedCommand = make(chan bool, 1)
	// Your initialization code here (2A, 2B, 2C).
    rf.currentTerm = 0
    rf.votedFor = -1

    rf.applyCh = applyCh
    rf.nextIndex = make([]int, len(peers))
    rf.logs = append(rf.logs, LogEntry{-1, rf.currentTerm})
    for i := 0; i < len(peers); i++ {
        rf.nextIndex[i] = 1
    }
    rf.matchIndex = make([]int, len(peers))

    go rf.TransitionToCandidate()
    go rf.CheckMatchIndexAndSetCommitIndex()

	// initialize from state persisted before a crash
    DPrintf("[Make] me:%d return", rf.me)
	rf.readPersist(persister.ReadRaftState())

	return rf
}

func (rf *Raft) TransitionToCandidate() {
    rf.role = candidate
    for {
        select {
        case v := <- rf.changeToFollower:
            DPrintf("[TransitionToCandidate] me:%d changeToFollower:%v", rf.me, v)
            //turn to follower
            go rf.WaitAsFollower(v)
            return
        case <- rf.receivedQuit:
            DPrintf("[TransitionToCandidate] me:%d quit", rf.me)
            return
        case <- time.After((time.Duration(rand.Intn(200)) + min_election_timeout_ms) * time.Millisecond):
            DPrintf("[TransitionToCandidate] me:%d wakeup, before_votedFor:%d currentTerm:%d", rf.me, rf.votedFor, rf.currentTerm)
            win_vote := rf.StartElection()
            if win_vote {
                DPrintf("[TransitionToCandidate] me:%d win vote in term:%d", rf.me, rf.currentTerm)
                go rf.BecomeLeader()
                return
            } else {
                DPrintf("[TransitionToCandidate] me:%d don't win vote", rf.me)
            }
        }
    }
}

func (rf *Raft) BecomeLeader() {
    rf.role = leader
    //When a leader first comes to power
    //it initializes all nextIndex values to the index just after the last one in its log.
    for i := 0; i < len(rf.nextIndex); i++ {
        rf.nextIndex[i] = len(rf.logs)
        rf.matchIndex[i] = 0
    }
    for {
        select {
        case v := <- rf.changeToFollower:
            //turn to follower
            DPrintf("[BecomeLeader] me:%d changeToFollower:%v", rf.me, v)
            go rf.WaitAsFollower(v)
            return
        case <- rf.receivedQuit:
            DPrintf("[BecomeLeader] me:%d quit", rf.me)
            return
        case <- rf.logInconsistency:
            DPrintf("[BecomeLeader] me:%d logInconsistency", rf.me)
            rf.SendLogEntryMessageToAll()
        case <- rf.receivedCommand:
            DPrintf("[BecomeLeader] me:%d receivedCommand. begin rf.SendLogEntryMessageToAll()", rf.me)
            rf.SendLogEntryMessageToAll()
        default:
            DPrintf("[BecomeLeader] me:%d default", rf.me)
            //Upon election: send initial empty AppendEntries RPCs(heartbeat) to each server;
            //repeat during idle periods to prevent election timeouts.
            rf.SendLogEntryMessageToAll()
            //Hint: The tester requires that the leader send heartbeat RPCs no more than then times persecond
            time.Sleep(heart_beat_interval_ms * time.Millisecond)
        }
    }
}

func (rf *Raft) WaitAsFollower(changeToFollower ChangeToFollower) {
    DPrintf("[WaitAsFollower] me:%d enter before lock changeToFollower:%v rf.currentTerm:%d", rf.me, changeToFollower, rf.currentTerm)
    rf.role = follower
    rf.currentTerm = changeToFollower.term
    if changeToFollower.votedFor != -1 {
        rf.votedFor = changeToFollower.votedFor
    }
    rf.changeToFollowerDone <- true

    DPrintf("[WaitAsFollower] me:%d before for looooooooop", rf.me)
    for {
        DPrintf("[WaitAsFollower] me:%d begin wait select", rf.me)

        select {
        case v := <- rf.changeToFollower:
            // continue WaitAsFollower with another leader(maybe)
            DPrintf("[WaitAsFollower] me:%d currentTerm:%v changeToFollower:%v", rf.me, rf.currentTerm, v)
            if v.term > rf.currentTerm {
                go rf.WaitAsFollower(v)
                return
            }
            rf.changeToFollowerDone <- true
        case <- time.After(1.5 * heart_beat_interval_ms * time.Millisecond):
            DPrintf("[WaitAsFollower] me:%d timeout", rf.me)
            go rf.TransitionToCandidate()
            return
        case <- rf.logInconsistency:
            DPrintf("[WaitAsFollower] me:%d logInconsistency, maybe bcz of the async rpc goroutine in SendLogEntryMessageToAll")
        case <- rf.receivedQuit:
            DPrintf("[WaitAsFollower] me:%d quit", rf.me)
            return
        }
    }
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

func (rf *Raft) StartElection() bool {
    server_count := len(rf.peers)
    voted := make([]bool, server_count)
    voted_majority := make(chan bool, 1)
    var term int

    //increment currentTerm
    rf.currentTerm++
    //vote for self
    rf.votedFor = rf.me
    voted[rf.me] = true
    term = rf.currentTerm

    //send request vote RPCs to all other servers
    for i := 0; i < server_count; i++ {
        if i != rf.me {
            server_index := i
            var request RequestVoteArgs
            request.Term = term
            request.CandidateId = rf.me
            request.LastLogIndex = len(rf.logs) - 1
            request.LastLogTerm = rf.logs[request.LastLogIndex].Term

            //issues RequestVote RPCs in parallel to each of the other servers in the cluster
            go func(index int, server_count int, voted []bool, rf* Raft, request RequestVoteArgs) {
                var reply RequestVoteReply

                DPrintf("[StartElection] sendRequestVote from me:%d to server:%d currentTerm:%d", rf.me, index, rf.currentTerm)

                send_ok := rf.sendRequestVote(server_index, &request, &reply)
                DPrintf("[StartElection] sendRequestVote from me:%d to server:%d currentTerm:%d send_ok:%t voted:%t", rf.me, index, rf.currentTerm, send_ok, reply.VoteGranted)

                voted[index] = send_ok && reply.VoteGranted

                if CheckIfWinHalfVote(voted, server_count) {
                    DPrintf("[StartElection] rf.me:%d voted:%v rf.logs:%v rf.currentTerm:%v", rf.me, voted, rf.logs, rf.currentTerm)
                    voted_majority <- true
                }

                if !reply.VoteGranted {
                    // DPrintf("[StartElection] %d don't vote for %d in term:%d", index, rf.me, rf.currentTerm)
                }
            }(server_index, server_count, voted, rf, request)
        }
    }

    select {
        case <- voted_majority:
            return true
        case <- time.After(min_election_timeout_ms * time.Millisecond):
            //split votes
            return false
    }
}

func (rf *Raft) SendLogEntryMessageToAll() {
    DPrintf("[SendLogEntryMessageToAll] me:%d begin", rf.me)
    server_count := len(rf.peers)
    max_log_entry_index := len(rf.logs)

    for i := 0; i < server_count; i++ {
        if i != rf.me {
            request := rf.makeAppendEntryRequest(rf.nextIndex[i], max_log_entry_index)
            go func(server_index int, request *AppendEntriesArgs) {
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
                        if response.Term == rf.currentTerm {
                            rf.mu.Lock()
                            DPrintf("[SendLogEntryMessageToAll] before log inconsistency: me:%d server_index:%d rf.nextIndex:%v", rf.me, server_index, rf.nextIndex)

                            nextIndex := -1
                            for j := 1; j < len(rf.logs) - 1; j++ {
                                if rf.logs[j].Term == response.Term && rf.logs[j + 1].Term != response.Term {
                                    nextIndex = j + 1
                                    break
                                }
                            }
                            if response.ConflictTerm == -1 || nextIndex == -1 {
                                rf.nextIndex[server_index] = response.ConflictIndex
                            } else {
                                rf.nextIndex[server_index] = nextIndex
                            }
                            DPrintf("[SendLogEntryMessageToAll] after log inconsistency: me:%d server_index:%d rf.nextIndex:%v", rf.me, server_index, rf.nextIndex)
                            //重试解决log inconsistency
                            rf.logInconsistency <- true
                            rf.mu.Unlock()
                        } else if response.Term > rf.currentTerm {
                            //If RPC request or response contains term T > currentTerm
                            //set currentTerm = T, convert to follower.
                            DPrintf("[SendLogEntryMessageToAll] server:%v push change me:%v response:%v currentTerm:%d", server_index, rf.me, response, rf.currentTerm)
                            //we don't know who is leader, set -1.
                            rf.PushChangeToFollower(response.Term, -1)
                        } else {
                            DPrintf("This line should not print!!! response:%v rf.currentTerm:%v", response, rf.currentTerm)
                        }
                    }
                }
                DPrintf("[SendLogEntryMessageToAll] from me:%d to server_index:%d send_ok:%t response:%v rf.nextIndex:%v", rf.me, server_index, send_ok, response, rf.nextIndex)
            }(i, request)
        }
    }
}

func (rf *Raft) CheckMatchIndexAndSetCommitIndex() {
    DPrintf("[CheckMatchIndexAndSetCommitIndex] rf.me:%v begin.", rf.me)
    majorityIndex := (len(rf.peers) - 1) / 2
    for {
        <- rf.followerAppendEntries

        rf.mu.Lock()
        //If there exists an N such that N > commitIndex, a majority of matchIndex[i] >= N,
        //and log[N].term == currentTerm: set commitIndex = N
        last_commit_index := rf.commitIndex
        matchIndex := append([]int{}, rf.matchIndex...)
        sort.Ints(matchIndex)
        new_commit_index := matchIndex[majorityIndex]
        DPrintf("[CheckMatchIndexAndSetCommitIndex] rf.me:%v matchIndex:%v last_commit_index:%v new_commit_index:%v", rf.me, rf.matchIndex, last_commit_index, new_commit_index)

        if last_commit_index != new_commit_index && rf.logs[new_commit_index].Term == rf.currentTerm {
            DPrintf("[CheckMatchIndexAndSetCommitIndex] commitIndex:%v -> %v me:%d currentTerm:%v rf.logs:%v", last_commit_index, new_commit_index, rf.me, rf.currentTerm, rf.logs)
            rf.commitIndex = new_commit_index
            rf.NotifyApplyCh(last_commit_index)
        }
        rf.mu.Unlock()
    }
}
