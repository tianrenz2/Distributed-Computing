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
import (
	"labrpc"
	"bytes"
	"labgob"
	"math/rand"
	"time"
	"fmt"
	"strconv"
)

// import "bytes"
// import "labgob"

type State int
const (
	follower State = 0
	candidate State = 1
	leader State = 2
)

//
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

//
// A Go object implementing a single Raft peer.
//

type LogEntry struct {
	term int
	command string
}

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	//latest term server has seen
	currentTerm int
	votedFor *int
	logs [] LogEntry

	//Index of highest log entry known to be committed
	commitId int

	//index of highest log entry applied to state machine
	lastApplied int

	//the number of votes the Raft has received as a candidate
	voteNum int

	state State
	timer time.Duration

	appendedEn chan bool
	reqVote chan bool
	timeout chan bool
	elecTimeout chan bool

	elecTimer time.Duration

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	//fmt.Println("server " + strconv.Itoa(rf.me) + ", getstate votes: " + strconv.Itoa(rf.voteNum))
	isLeader := (rf.state == leader)
	//fmt.Println(isLeader)
	//fmt.Printf("Is leader: " + strconv.FormatBool(isLeader) + "\n")
	return rf.currentTerm, isLeader
}


//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(&(rf.currentTerm))
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
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
}


//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term int
	CandidateId *int
	lastLogIndex int
	lastLogTerm int
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
	//reqArgs := RequestVoteArgs{Term: rf.currentTerm, CandidateId:rf.votedFor, lastLogIndex:len(rf.logs) - 1, lastLogTerm: rf.logs[len(rf.logs) - 1].term}
	if rf.currentTerm < args.Term {
		rf.state = follower
		rf.votedFor = nil
	}
	if args.Term < rf.currentTerm{
		reply.VoteGranted = false
	}else if (rf.votedFor == nil && args.CandidateId != nil ){
		rf.votedFor = args.CandidateId
		fmt.Println("Agree vote request: " + strconv.Itoa(rf.me))
		fmt.Printf(strconv.Itoa(args.Term) + ", term:" + strconv.Itoa(rf.currentTerm) + "\n" )
		rf.currentTerm = args.Term
		reply.VoteGranted = true
	}else if(rf.votedFor != nil){
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
	}
	return
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
	//fmt.Printf("Server: %d\n", server)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	//if ok{
	//	fmt.Println("Got request vote " + strconv.Itoa(rf.me) + ", from " + strconv.Itoa(server) + ", granted: " + strconv.FormatBool(reply.VoteGranted))
	//	//rf.elecTimeout <- true
	//}
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

type Entry struct {
	term int
	leaderId int
	//preLogIndex int
	//preLogTerm int
	//entries[] int
	//leaderCommit int
}

type EntryResult struct {
	term int
	success bool
}

func (rf *Raft)SendHeartBeats() {
	fmt.Println(strconv.Itoa(rf.me) + " sends beats to peers")
	for i := 0; i < len(rf.peers); i++ {
		go func() {
			var res = EntryResult{0, true}
			rf.sendAppendEntryReq(i, &Entry{rf.currentTerm, rf.commitId }, &res)
		}()
	}
	rf.resetTimer(100, 150)
	time.Sleep(rf.timer)
}


func (rf *Raft)AppendEntries(entry *Entry, entryRes *EntryResult) {
	if (entry.term < rf.currentTerm) {
		entryRes.success = false
		entryRes.term = rf.currentTerm
		return
	}
	if (entry.term > rf.currentTerm) {
		rf.currentTerm = entry.term
		rf.state = follower
	}
	entryRes.success = true
	entryRes.term = rf.currentTerm
	go func() {
		//DPrintf("server %d(Term = %d) received AppendEntries from LEADER %d(Term = %d)\n",
		//	rf.me, rf.currentTerm, entry.LeaderId, args.Term)
		fmt.Println("server")
		rf.appendedEn <- true
	}()
}

func (rf *Raft) sendAppendEntryReq(server int, args *Entry, reply *EntryResult) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
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

func (rf *Raft)resetTimer(min int, max int) {
	waitTime := rand.Int() % max + min
	rf.timer = time.Duration(waitTime)* time.Millisecond
}

func (rf *Raft)resetElectTimer()  {
	waitTime := rand.Int() % 1500 + 1500
	rf.elecTimer = time.Duration(waitTime)* time.Millisecond
}

func (rf *Raft)waitForVote()  {
	time.Sleep(rf.elecTimer)
	rf.elecTimeout <- true
}

func (rf *Raft)checkWithLeader()  {
	//fmt.Printf("check Time out\n")
	time.Sleep(rf.timer)
	//fmt.Printf("Timeout already")
	//rf.mu.Lock()
	rf.timeout <- true
	//rf.mu.Unlock()
}

func (rf *Raft)startElection()  {
	rf.mu.Lock()
	rf.currentTerm += 1

	rf.votedFor = &rf.me
	rf.voteNum += 1
	rf.resetTimer(1500, 3000)
	rf.mu.Unlock()
	//fmt.Println("start election " + string(rf.commitId))

	var args = RequestVoteArgs{rf.currentTerm, &rf.me, 0, 0 }

	defer func() {
		for i := 0; i < len(rf.peers); i++ {
			if (i == rf.me) {
				continue
			}
			// every reply is different, so put it in goroutine
			go func (server int)  {
				var reply RequestVoteReply
				if (rf.sendRequestVote(server, &args, &reply)) {
					//fmt.Println(strconv.Itoa(rf.me) + " Got request from " + strconv.FormatBool(reply.VoteGranted) )
					if reply.VoteGranted == true {
						var win bool
						rf.mu.Lock()
						rf.voteNum += 1
						fmt.Println(strconv.Itoa(rf.me) + " received vote: "  +
							", voteNum: " + strconv.Itoa(rf.voteNum))
						if rf.voteNum > len(rf.peers) / 2{
							win = true
						}
						rf.mu.Unlock()

						if win == true {
							rf.state = leader
							fmt.Println(strconv.Itoa(rf.me) + " has been selected as leader ")
						}
					} else {
						// response contains Term > currentTerm
						if rf.currentTerm < reply.Term{
							rf.currentTerm = reply.Term
							rf.resetTimer(1500, 3000)
							rf.state = follower
						}
					}
				}
			}(i)

		}
	}()

	if rf.state != leader {
		fmt.Println(strconv.Itoa(rf.me) + " give up candidation")
		rf.state = follower
	}else {
		fmt.Println(strconv.Itoa(rf.me) + " has been selected as leader ")
	}

}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.state = follower

	rf.appendedEn = make(chan bool)
	rf.timeout = make(chan bool)
	rf.reqVote = make(chan bool)

	rf.resetTimer(1500, 3000)
	go rf.checkWithLeader()
	//fmt.Printf("initialize %d, %d\n", rf.me, rf.state)
	// Your initialization code here (2A, 2B, 2C).
	//go func() {
	go func() {
		for {
			fmt.Printf("%d's character %d\n", rf.me, rf.state)
			if rf.state == follower {
				select {
					case <-rf.appendedEn:
						fmt.Printf("Received append entry\n")
						rf.resetTimer(1500, 3000)
						rf.checkWithLeader()
					case <-rf.reqVote:
						fmt.Printf("\n")
						rf.resetTimer(1500, 3000)
						rf.checkWithLeader()
					case <-rf.timeout:
						rf.state = candidate

				}

			}else if rf.state == candidate{
				//fmt.Printf("Start election: server %d\n", rf.me)
				go rf.startElection()
				select {
					case <-rf.appendedEn:
						rf.state = follower
						DPrintf("server %d become FOLLOWER", rf.me)
					//case <-rf.timeout:
					//	fmt.Printf("Start election\n")

				}
			}else if rf.state == leader{
				rf.SendHeartBeats()
			}
		}
	}()
	//}()


	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())


	return rf
}
