package omnipaxos

import (
	"cs651/labgob"
	"cs651/labrpc"
	"fmt"

	//"fmt"
	"bytes"
	"sync"
	"sync/atomic"
	"time"
)

// A Go object implementing a single OmniPaxos peer.
type OmniPaxos struct {
    mu            sync.Mutex          // Lock to protect shared access to this peer's state
    peers         []*labrpc.ClientEnd // RPC end points of all peers
    persister     *Persister          // Object to hold this peer's persisted state
    me            int                 // This peer's index into peers[]
    dead          int32               // Set by Kill()
    enableLogging int32
    leader        int
    l             Ballot                
    r             int
    b             Ballot
    qc            bool
    ballots       []Ballot
    contactedNodes [100]bool
    log           []interface{}       // Log entries as []interface{}
    promisedRnd   Ballot                // The round a server has promised not to accept entries from any lower round
    acceptedRnd   Ballot              // The latest round a server has accepted entries in
    decidedIdx    int                 
    state         string             
    currentRnd    Ballot                // The round that this server is leading in
    promises      []Promise      
    maxProm       Promise             // The highest promise received during the prepare phase
    accepted      [100]int               // The accepted index per server
    buffer        []interface{}       // Buffer for client requests during the prepare phase
	applyCh      chan ApplyMsg
	LinkDrop      bool 
	missedHeartBeats int
}

type Promise struct {
    N       Ballot
    AccRnd  Ballot  
    LogIdx  int  
    DecIdx  int   
    Sfx     []interface{}
    F       int
}

type Ballot struct {
	BallotNum int
	Pid       int
	Qc        bool
}

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type PrepareReq struct {
	F int
}

type HBRequest struct {
	Rnd    int
	S int
}

type Accepted struct{
	N     Ballot
	LogIdx int
	F int
}

type LeaderReply struct{
	Rq bool
}

type Accept struct{
	N Ballot
	C interface{}
}

type Decide struct{
	N     Ballot
	DecIdx int
}

type Prepare struct {
	N     Ballot 
	AccRnd    Ballot
	LogIdx    int 
	DecidedIdx int 
	L int
}

type Leader struct {
	S    int
	N     Ballot
}

type AcceptSync struct {
    N       Ballot
    Sfx     []interface{}
    SyncIdx int
    L       int
}

type DummyReply struct{}

type HBReply struct {
	Rnd    int
	Ballot Ballot
	Q      bool
}

func (op *OmniPaxos) suffix(idx int) []interface{} {
    // Handle case where idx is out of bounds or negative
    if idx >= len(op.log) || idx < 0 {
        return []interface{}{}
    }
    // Return the suffix of the log starting from idx to the end
    return op.log[idx:]
}

// Sample code for sending an RPC to another server
func (op *OmniPaxos) sendHeartBeats(server int, args *HBRequest, reply *HBReply) bool {
	//fmt.Printf("%d try to call %d's handler\n",op.me,server)
	ok := op.peers[server].Call("OmniPaxos.HeartBeatHandler", args, reply)
	
	return ok
}

func (op *OmniPaxos) HeartBeatHandler(args *HBRequest, reply *HBReply) {
	op.mu.Lock()
	defer op.mu.Unlock()
	//fmt.Printf("%d gets %d's ballot: %d with qc= %v\n",args.S,op.me,op.b.BallotNum,op.b.Qc)
	reply.Rnd = args.Rnd
	reply.Ballot = op.b
	reply.Q = op.qc
}

// GetState Return the current leader's ballot and whether this server believes it is the leader.
func (op *OmniPaxos) GetState() (int, bool) {
	op.mu.Lock()
	defer op.mu.Unlock()

	ballot := op.b.BallotNum
	isLeader := (op.leader == op.me)

	return ballot, isLeader
}

func (op *OmniPaxos) startTimer(delay time.Duration) {
    op.mu.Lock()
    op.ballots = append(op.ballots, op.b)
    
    if len(op.ballots) >= len(op.peers)/2+1 {
		//fmt.Printf("%d checking leader\n",op.me)
        op.checkLeader()
    } else {
        op.qc = false
		//fmt.Print("ops!\n")
        op.b.Qc = false
    }
   
    op.ballots = []Ballot{}
    op.r++
    op.mu.Unlock()

    // Track how many peers responded in this cycle
    responsesReceived := 0

    for i := 0; i < len(op.peers); i++ {
        if i != op.me {
            args := &HBRequest{
                Rnd: op.r,
				S: op.me,
            }
            reply := &HBReply{}

            go func(i int) {
                ok := op.sendHeartBeats(i, args, reply)
                op.mu.Lock()
                defer op.mu.Unlock()

                if ok {
					if !op.contactedNodes[i] {
                        op.contactedNodes[i] = true // Mark this node as contacte
                        prepareReq := &PrepareReq{F: op.me}
                        go func(peer int) {
                            var reply DummyReply
							//fmt.Printf("%d successfully sent PrepareReq to node %d\n", op.me, peer)
                            ok := op.peers[peer].Call("OmniPaxos.PrepareReqHandler", prepareReq, &reply)
                            if !ok {
                               
                            } else {
                              
                            }
                        }(i)
                    }
                    responsesReceived++ 
                    op.missedHeartBeats = 0 
                    if op.LinkDrop {
                        op.LinkDrop = false
                        op.state = "FOLLOWER_RECOVER"
                        //fmt.Printf("%d is back\n", op.me)
                        prepareReq := &PrepareReq{F: op.me}
                        for i := 0; i < len(op.peers); i++ {
							//.Printf("%d is prepareReqing\n", op.me)
                            if i != op.me {
                                var reply DummyReply
                                go func(peer int) {
                                    ok := op.peers[peer].Call("OmniPaxos.PrepareReqHandler", prepareReq, &reply)
                                    if !ok {
                                        // Handle RPC failure
                                    }
                                }(i)
                            }
                        }
                    }
                    if reply.Rnd == op.r {
                        op.ballots = append(op.ballots, reply.Ballot)
						//fmt.Printf("%d append %d\n",op.me,reply.Ballot.Pid)
                    }
                }else{
					op.contactedNodes[i]=false
				}
            }(i)
        }
    }

    go func() {
        time.Sleep(delay)
        op.mu.Lock()
        if responsesReceived == 0 {
            op.missedHeartBeats++
            if op.missedHeartBeats >= 3 {
                //fmt.Printf("%d is down\n", op.me)
				op.state = "FOLLOWER_DOWN"
                op.LinkDrop = true
            }
        } else {
            op.missedHeartBeats = 0 
        }
		op.mu.Unlock()
        op.startTimer(delay)
    }()
}

func (op *OmniPaxos) PrepareReqHandler(args *PrepareReq, reply *DummyReply) {
	op.mu.Lock()
	defer op.mu.Unlock()
	
	if (op.state!="LEADER_PREPARE"&&op.state!="LEADER_ACCEPT"){
		 return
	}
	
	args1 := &Prepare{
		N:      op.currentRnd,
		AccRnd:     op.acceptedRnd,
		LogIdx:     len(op.log),
		DecidedIdx: op.decidedIdx,
		L: op.me,
	}
	// Since no reply is needed, we use DummyReply
	var reply1 DummyReply
	//fmt.Printf("%d send prepare to %d:%v\n",op.me,args.F,args1)
	go func(peer int) {
		
		ok := op.peers[peer].Call("OmniPaxos.PrepareHandler", args1, &reply1)
		if !ok {
			// Handle the case where the RPC failed
		}
	}(args.F)
}
	
func (op *OmniPaxos) checkLeader() {
	candidates := []Ballot{}
	for _, ballot := range op.ballots {
		if ballot.Qc {
			candidates = append(candidates, ballot)
		}
	}
	//fmt.Printf("%d ballots:Candidates with data: %+v,l is %v\n",op.me, candidates,op.l)
	if len(candidates) > 0 {
		maxBallot := max(candidates)
		if maxBallot.IsGreaterThan(op.l) {
			op.l.BallotNum = maxBallot.BallotNum
			op.l.Pid=maxBallot.Pid
			op.l.Qc=true
			op.leader=maxBallot.Pid
			leaderArgs := &Leader{
				S:  maxBallot.Pid,
				N: maxBallot,
			}
			//fmt.Printf("%d'sleader's ballot is %d, maxis %d,leader is %d,op.dec is %d\n",op.me,op.l.BallotNum,maxBallot.BallotNum,op.leader,op.decidedIdx)
			op.persist()
			if (op.me==op.leader){
				op.Leader(leaderArgs,false)
			}
			// go func() {
			// 	op.mu.Lock()
			// 	defer op.mu.Unlock()	
			// 		if (op.leader != op.me && (op.state=="LEADER_ACCEPT"||op.state=="LEADER_PREPARE")){
			// 			prepareReq := &PrepareReq{F: op.me}
			// 			//fmt.Printf("%d is prepareReqing\n", op.me)
			// 			for i := 0; i < len(op.peers); i++ {
			// 				if i != op.me {
			// 					var reply DummyReply
			// 					go func(peer int) {
			// 						ok := op.peers[peer].Call("OmniPaxos.PrepareReqHandler", prepareReq, &reply)
			// 						if !ok {
			// 							// Handle RPC failure
			// 						}
			// 					}(i)
			// 				}
			// 			}
			// 		}
				
			// }()

		} else if op.l.IsGreaterThan(maxBallot){ 	 	
			op.increment(op.l.BallotNum)
			op.qc = true
			op.b.Qc= true
			//fmt.Printf("leader disconnected\n,ready for election")
		}else{
		}
	} else{
			op.increment(op.l.BallotNum)
			op.qc = true
			op.b.Qc= true
	}
}

// The service or tester wants to create an OmniPaxos server.
func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *OmniPaxos {
    op := &OmniPaxos{}
    op.peers = peers
    op.persister = persister
	op.me = me
	op.leader = -1        
	op.l.BallotNum = -1      
	op.l.Pid = -1
	op.l.Qc = true
	op.r = 0             
	op.b = Ballot{BallotNum: 0, Pid: op.me, Qc: true} 
	op.qc = true         
	op.ballots = make([]Ballot, 0) 
	op.log = make([]interface{}, 0) 
	op.promisedRnd = Ballot{BallotNum: -1, Pid: -1, Qc: false} 
	op.acceptedRnd = Ballot{BallotNum: -1, Pid: -1, Qc: false} 
	op.decidedIdx = -1
	op.state = "FOLLOWER_PREPARE"
	op.currentRnd = Ballot{BallotNum: 0, Pid: -1, Qc: false} 
	op.promises = make([]Promise, 0) 
	op.maxProm = Promise{} 
	op.accepted = [100]int{}
	op.buffer = make([]interface{}, 0) 
	op.applyCh = applyCh
	op.LinkDrop   =   false
	op.missedHeartBeats = 0
	op.contactedNodes= [100]bool{false}
	persistedData := persister.ReadOmnipaxosState()
    op.readPersist(persistedData)
	//fmt.Printf("%d's log is %v,leader is %d\n",op.me,op.log,op.l.Pid)
    go func() {
        delay :=  150 * time.Millisecond  
        op.startTimer(delay)
    }()

    return op
}

func (b Ballot) IsGreaterThan(other Ballot) bool {
    if b.BallotNum > other.BallotNum {
        return true
    } else if b.BallotNum == other.BallotNum {
        return b.Pid > other.Pid 
    }
    return false
}

func (op *OmniPaxos) Leader(args *Leader,trigger bool) {
	
	fmt.Printf("%d start leader im leader\n",op.me)
	// for i, ballot := range op.ballots {
    //     fmt.Printf("Ballot %d - Pid: %d, Qc: %v, Rnd: %d\n", i+1, ballot.Pid, ballot.Qc, ballot.BallotNum)
    // }
	if (args.S == op.me && args.N.IsGreaterThan(op.promisedRnd))||trigger {
		//fmt.Printf("wll\n")
		op.currentRnd = Ballot{BallotNum: -1, Pid: -1, Qc: false} 
        op.promises =  make([]Promise, 0) 
    	op.maxProm = Promise{}
    	for i := range op.accepted {
        	op.accepted[i] = 0
    	}
    	op.buffer = []interface{}{}
		// Reset all volatile state of the leader
		op.state = "LEADER_PREPARE"
		op.currentRnd = args.N
		op.promisedRnd = args.N
		// Step 2: Insert the leader's own promise
		//op.acceptedRnd = args.N
		selfPromise := Promise{
			AccRnd:   op.acceptedRnd,
			LogIdx:   len(op.log),
			F:        args.S, 
			Sfx:      op.suffix(op.decidedIdx), 
			DecIdx: op.decidedIdx,
			N: op.currentRnd,
		}
		op.promises = append(op.promises, selfPromise)
		op.persist()
		// Step 3: Send Prepare message to all peers
		for i := 0; i < len(op.peers); i++ {
			if i != op.me {
				args := &Prepare{
					N:      op.currentRnd,
					AccRnd:     op.acceptedRnd,
					LogIdx:     len(op.log),
					DecidedIdx: op.decidedIdx,
					L: op.me,
				}
				// Since no reply is needed, we use DummyReply
				var reply DummyReply
	// 			fmt.Printf("PrepareRequest - N: %v, AccRnd: %v, LogIdx: %d, DecidedIdx: %d, L: %d,to:%d\n",
    // args.N, args.AccRnd, args.LogIdx, args.DecidedIdx, args.L,i)
				go func(peer int) {
					
					ok := op.peers[peer].Call("OmniPaxos.PrepareHandler", args, &reply)
					if !ok {
						// Handle the case where the RPC failed
					}
				}(i)
			}
		}
		
	}
	return
}

func (op *OmniPaxos) PrepareHandler(args *Prepare, reply *DummyReply) {
	op.mu.Lock()
	defer op.mu.Unlock()
	//fmt.Printf("imhere!|%d\n",op.me)
	// Step 1: Return if promisedRnd > n
	if op.promisedRnd.IsGreaterThan(args.N) {
		return 
	}
	//fmt.Printf("imhere!1|!!\n")
	// Step 2: Update state to FOLLOWER_PREPARE and update promisedRnd
	op.state = "FOLLOWER_PREPARE"
	op.promisedRnd = args.N

	// Step 3: Determine the suffix (sfx) based on acceptedRnd and accRnd
	var sfx []interface{}
	if op.acceptedRnd.IsGreaterThan(args.AccRnd) {
		sfx = op.suffix(args.DecidedIdx)
	} else if op.acceptedRnd == args.AccRnd {
		sfx = op.suffix(args.LogIdx)
	} else {
		sfx = []interface{}{}
	}
	op.persist()
	//.Printf("IM HERE5")
	// Step 4: Send a Promise back to the leader with updated values
	args1 := &Promise{
		N: args.N,
		AccRnd:   op.acceptedRnd,
		LogIdx:   len(op.log),
		F:        op.me, 
		DecIdx:  op.decidedIdx,
		Sfx:      sfx,
	}
	 fmt.Printf("Promise - N: %v, AccRnd: %v, LogIdx: %d, F: %d, DecIdx: %d, Sfx: %v\n",
    args1.N, args1.AccRnd, args1.LogIdx, args1.F, args1.DecIdx, args1.Sfx)

	go func(leader int, promiseArgs *Promise) {
        var promiseReply DummyReply
        ok := op.peers[leader].Call("OmniPaxos.PromiseHandler", promiseArgs, &promiseReply)
        if !ok {
            // Handle RPC failure (retry or log the failure if needed)
        }
    }(args.L, args1)
}

func (op *OmniPaxos) stopped() bool {
    if len(op.log) == 0 {
        return false 
    }
    lastEntry := op.log[len(op.log)-1] 
    if lastEntry == "SS" {
        return true
    }
    return false
}

func (op *OmniPaxos) PromiseHandler(args *Promise, reply *DummyReply) {
    op.mu.Lock()
    defer op.mu.Unlock()
    // Step 1: Return if args.N (the round) is not equal to currentRnd
    if args.N != op.currentRnd {
        return
    }
    for i, p := range op.promises {
		if p.F == args.F {
			op.promises = append(op.promises[:i], op.promises[i+1:]...)
			break 
		}
	}
	op.promises = append(op.promises, *args)
	//fmt.Printf("promises:%v\n",op.promises)
    // Handle based on the current state
    switch op.state {
    case "LEADER_PREPARE":{
		if len(op.promises) < len(op.peers)/2+1 {
			return
		}
        // Step P2: Find maxProm - the promise with the highest AccRnd in promises
        for _, p := range op.promises {
            if p.AccRnd.IsGreaterThan(op.maxProm.AccRnd) || (p.AccRnd == op.maxProm.AccRnd && p.LogIdx > op.maxProm.LogIdx) {
                op.maxProm = p
            }
        }
		//fmt.Printf("op.maxProm - N: %d, AccRnd: %d, LogIdx: %d, F: %d, DecIdx: %d, Sfx: %v\n",
    //op.maxProm.N, op.maxProm.AccRnd, op.maxProm.LogIdx, op.maxProm.F, op.maxProm.DecIdx, op.maxProm.Sfx)
        // Step P3: If maxProm.AccRnd is not equal to acceptedRnd, update the log prefix
        if op.maxProm.AccRnd != op.acceptedRnd {
			if (op.decidedIdx>0){
            	op.log = op.log[:op.decidedIdx] 
				fmt.Printf("%d prefix %v\n",op.me,op.log)
			}
        }
		fmt.Printf("%d sufix %v\n",op.me, op.maxProm.Sfx)
        // Step P4: Append maxProm.Sfx to the log
        op.log = append(op.log, op.maxProm.Sfx...)
		fmt.Printf("log:%v\n",op.log)
		fmt.Printf("%v\n",op.promises)
        // Step P5: If the server was stopped, clear the buffer and add buffer to the log
        if op.stopped() {
            op.buffer = []interface{}{} // Clear the buffer
        } else {
            op.log = append(op.log, op.buffer[:]...) // Append buffer to the log
			op.buffer = []interface{}{} 
        }
		//fmt.Println(op.log)
        // Step P6: Update acceptedRnd and state to LEADER_ACCEPT
        op.acceptedRnd = op.currentRnd
        op.accepted[op.me] = len(op.log)
        op.state = "LEADER_ACCEPT"
		op.persist()
		// for idx := op.decidedIdx+1 ; idx <=len( op.log) ; idx++ {
		// 	if ((idx-1 < len(op.log))&&(idx-1>=0)) {
		// 		msg := ApplyMsg{
		// 			CommandValid: true,
		// 			Command:      op.log[idx-1], 
		// 			CommandIndex: idx-1,
		// 		}
		// 		op.applyCh <- msg 
		// 	}
		// }
		// op.decidedIdx = len( op.log)
		// op.persist()
        // Step P9: Send AcceptSync messages to all peers based on the promises
        for _, p := range op.promises {
            var syncIdx int
            if p.AccRnd == op.maxProm.AccRnd {
                syncIdx = p.LogIdx
            } else {
                syncIdx = p.DecIdx
            }
		
            acceptArgs := &AcceptSync{
                N:         op.currentRnd,
                Sfx:       op.suffix(syncIdx),
                SyncIdx:   syncIdx,
				L: op.me,
            }
			op.persist()
            go func(peer int, acceptArgs *AcceptSync) {
				var acceptReply DummyReply
				ok := op.peers[peer].Call("OmniPaxos.AcceptSyncHandler", acceptArgs, &acceptReply)
				if !ok {
					// Handle RPC failure (retry or log the failure if needed)
				}
			}(p.F, acceptArgs) 
        }
		
	}
    case "LEADER_ACCEPT":
        // Handle the state when in the LEADER_ACCEPT phase
        // Step A1: Determine the syncIdx based on maxProm
        
        // fmt.Printf("%v\n",op.maxProm)
        var syncIdx int
        if op.maxProm.AccRnd == args.AccRnd {
            syncIdx = op.maxProm.LogIdx
        } else {
            syncIdx = args.DecIdx
        }

        // Step A2: Send AcceptSync messages to all peers
        acceptArgs := &AcceptSync{
            N:         op.currentRnd,
            Sfx:       op.suffix(syncIdx),
            SyncIdx:   syncIdx,
			L: op.me,
        }
		op.persist()
		// fmt.Printf("AcceptSync - N: %v, Sfx: %v, SyncIdx: %d, L: %d,F:%d\n", 
        //    acceptArgs.N, acceptArgs.Sfx, acceptArgs.SyncIdx, acceptArgs.L,args.F)
		var wg sync.WaitGroup

		wg.Add(1)  
		go func(peer int, acceptArgs *AcceptSync) {
			defer wg.Done()  
			var acceptReply DummyReply
			ok := op.peers[peer].Call("OmniPaxos.AcceptSyncHandler", acceptArgs, &acceptReply)
			if !ok {
				// Handle RPC failure (retry or log the failure if needed)
			}
		}(args.F, acceptArgs)
		wg.Wait()

		if op.decidedIdx > args.DecIdx {
			//fmt.Printf("ask %d to decide %d\n", args.F, op.decidedIdx)
			decideArgs := &Decide{
				N:      op.currentRnd,
				DecIdx: op.decidedIdx,
			}
			var reply DummyReply
			go func(peer int) {
				ok := op.peers[peer].Call("OmniPaxos.DecideHandler", decideArgs, &reply)
				if !ok {
					// Handle RPC failure if necessary
				}
			}(args.F)
		}

    default:
        // If the state is not recognized, do nothing
        return
    }
}

func (op *OmniPaxos) AcceptSyncHandler(args *AcceptSync, reply *DummyReply) {
    op.mu.Lock()
    defer op.mu.Unlock()

    // Step 1: 
    if op.promisedRnd != args.N || op.state != "FOLLOWER_PREPARE" {
        return
    }

    // Step 2: 
    op.acceptedRnd = args.N
    op.state = "FOLLOWER_ACCEPT"
	fmt.Printf(" %d ⟨AcceptSync⟩ was calld\n",op.me)

    // Step 3: Update the log
	if (args.SyncIdx>=0&&args.SyncIdx<=len(op.log)){
    op.log = op.log[:args.SyncIdx]
	}
    // Append sfx to the log
    op.log = append(op.log, args.Sfx...)
	fmt.Printf("%d's Log updated(syn): %v\n", op.me,op.log)
	op.persist()

    // Step 4: Send an Accepted message back to the leader with the log length
    acceptedArgs := &Accepted{
        N:     args.N,
        LogIdx: len(op.log),
		F:    op.me,
    }
    go func() {
        var acceptedReply DummyReply
        ok := op.peers[args.L].Call("OmniPaxos.AcceptedHandler", acceptedArgs, &acceptedReply)
        if !ok {
            // Handle RPC failure (retry or log the failure if needed)
        }
    }()
}

func (op *OmniPaxos) Proposal(command interface{}) (int, int, bool) {
	index := -1
	ballot := -1
	isLeader := false
	op.mu.Lock()
	defer op.mu.Unlock()

	if op.stopped() {
		//fmt.Print("imhere")
		return -1, op.b.BallotNum, false
	}

	isLeader = (op.leader == op.me)
	if !isLeader {
		//fmt.Printf("imhere1,%dnot leader\n",op.me)
		return op.me, op.b.BallotNum, false
	}
	//fmt.Printf("state:%v\n",op.state)

	switch op.state {

	case "LEADER_PREPARE":
		{
			op.buffer = append(op.buffer, command)
			return len(op.log) - 1, op.b.BallotNum, true
		}

	case "LEADER_ACCEPT":
		{	
			op.log = append(op.log, command)
			op.accepted[op.me] = len(op.log)

			acceptArgs := &Accept{
				N: op.currentRnd,
				C: command,
			}
			//fmt.Printf("%d's log: %+v\n", op.me,op.log)

			var acceptCount int32 = 1 
			for _, promise := range op.promises {
				go func(peer int) {
					var acceptReply DummyReply
					ok := op.peers[peer].Call("OmniPaxos.AcceptHandler", acceptArgs, &acceptReply)
					if ok {
						fmt.Printf("%d get it\n",promise.F)
						atomic.AddInt32(&acceptCount, 1)
					}
				}(promise.F) 
			}
			fmt.Printf("proposal:index=%d__rnd=%d\n", len(op.log)-1, op.b.BallotNum)
			for i := 0; i < 10; i++ { 
				time.Sleep(10 * time.Millisecond) 
				currentCount := atomic.LoadInt32(&acceptCount)
				if currentCount > int32(len(op.peers)/2) {
					break 
				}
			}
			
			op.persist()

			if atomic.LoadInt32(&acceptCount) > int32(len(op.peers)/2) {
				
				for idx := op.decidedIdx+1; idx < len(op.log)+1; idx++ {
					if (idx-1 < len(op.log)) && (idx-1 >= 0) {
						fmt.Printf("applied sth\n")
						msg := ApplyMsg{
							CommandValid: true,
							Command:      op.log[idx-1],
							CommandIndex: idx - 1,
						}
						fmt.Printf("applied%v\n",msg)
						op.applyCh <- msg
						//fmt.Printf("applied%v\n",msg)
					}
				}
				op.decidedIdx = len(op.log)
				//fmt.Printf("%d is gonna return\n",op.me)
				op.persist()
				
				decideArgs := &Decide{
					N:      op.currentRnd,
					DecIdx: op.decidedIdx,
				}
				for _, promise := range op.promises {
					go func(peer int) {
						var decideReply DummyReply
						ok := op.peers[peer].Call("OmniPaxos.DecideHandler", decideArgs, &decideReply)
						if !ok {
						}
					}(promise.F)
				}
			}else{ 
				fmt.Printf("not enough")

			
			}

			//fmt.Printf("%d is gonna return\n",op.me)
			return len(op.log) - 1, op.b.BallotNum, true
		}

	default:
		return -1, op.b.BallotNum, false
	}
	return index, ballot, isLeader
}



func (op *OmniPaxos) AcceptedHandler(args *Accepted, reply *DummyReply) {
    op.mu.Lock()
    defer op.mu.Unlock()
	fmt.Printf("Accepted was called with arg: %+v\n", args)
    if op.currentRnd != args.N || op.state != "LEADER_ACCEPT" {
        return
    }

    op.accepted[args.F] = args.LogIdx

    majority := len(op.peers)/2 + 1
    count := 1
    for _, acceptedIdx := range op.accepted {
        if acceptedIdx >= args.LogIdx {
            count++
        }
    }

    if count >= majority && args.LogIdx > op.decidedIdx {
		for idx := op.decidedIdx+1; idx <  args.LogIdx+1; idx++ {
			if (idx-1 < len(op.log)) && (idx-1 >= 0) {
				msg := ApplyMsg{
					CommandValid: true,
					Command:      op.log[idx-1],
					CommandIndex: idx - 1,
				}
				fmt.Printf("%d :Committed log entry: %+v\n",op.me, op.log[idx-1])
				op.applyCh <- msg
				
				//fmt.Printf("Committed log entry: %+v\n", op.log)
			}
		}
        op.decidedIdx =  args.LogIdx
		op.persist()
		for _, i := range op.promises {
        
            if i.F != op.me {
                decideArgs := &Decide{
                    N:        op.currentRnd,
                    DecIdx: op.decidedIdx,
                }
                var reply DummyReply
                go func(peer int) {
                    ok := op.peers[peer].Call("OmniPaxos.DecideHandler", decideArgs, &reply)
                    if !ok {
                    
                    }
                }(i.F)
            }
		}
        
    }
}

func (op *OmniPaxos) DecideHandler(args *Decide, reply *DummyReply) {
	op.mu.Lock()
    defer op.mu.Unlock()
	
	if (op.promisedRnd==args.N && op.state=="FOLLOWER_ACCEPT"){
		for idx := op.decidedIdx+1 ; idx <= args.DecIdx; idx++ {
			if ((idx-1 < len(op.log))&&(idx-1>=0)) {
				msg := ApplyMsg{
					CommandValid: true,
					Command:      op.log[idx-1], 
					CommandIndex: idx-1,
				}
				op.applyCh <- msg 
				//fmt.Printf("%d decide %v\n",op.me,msg)
			}
		}
		op.decidedIdx=args.DecIdx
		 //fmt.Printf("%d decide %d\n",op.me,op.decidedIdx)
		// fmt.Printf("accrnd:%d op:%d\n",op.acceptedRnd.BallotNum,op.me)
		op.persist()
	}
}

func (op *OmniPaxos) AcceptHandler(args *Accept, reply *Accepted) {
	op.mu.Lock()
    defer op.mu.Unlock()
	
	if (op.promisedRnd!=args.N||op.state!="FOLLOWER_ACCEPT") {
		fmt.Printf("%d's op.promisedRnd: %v,but args.N%v\n", op.me,op.promisedRnd,args.N)
		return
	}
	
	op.log=append(op.log, args.C)
	
	fmt.Printf("%d Log updated, current log: %v\n", op.me,op.log)
	
	op.persist()
	reply = &Accepted{
        N:      args.N,
        LogIdx: len(op.log),
        F:      op.me,
    }
}



func max(ballots []Ballot) Ballot {
	maxBallot := ballots[0]
	for _, ballot := range ballots {
		if ballot.BallotNum > maxBallot.BallotNum || 
		   (ballot.BallotNum == maxBallot.BallotNum && ballot.Pid > maxBallot.Pid) {
			maxBallot = ballot
		}
	}
	return maxBallot
}

func (op *OmniPaxos) increment(b int) {
	op.b.BallotNum = b + 1
}

func (op *OmniPaxos) Kill() {
	atomic.StoreInt32(&op.dead, 1)
	atomic.StoreInt32(&op.enableLogging, 0)
}

func (op *OmniPaxos) killed() bool {
	z := atomic.LoadInt32(&op.dead)
	return z == 1
}

// save OmniPaxos's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 3 &4 for a description of what should be persistent.
func (op *OmniPaxos) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(op.log)
	e.Encode(op.promisedRnd)
	e.Encode(op.acceptedRnd)
	e.Encode(op.decidedIdx)
	e.Encode(op.accepted)
	e.Encode(op.l)
	data := w.Bytes()
	op.persister.SaveOmnipaxosState(data)
}

// restore previously persisted state.
func (op *OmniPaxos) readPersist(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var log []interface{} 
	var promisedRnd, acceptedRnd Ballot
	var decidedIdx int
	var accepted [100]int
	var l Ballot

	if d.Decode(&log) != nil || 
	   d.Decode(&promisedRnd) != nil || 
	   d.Decode(&acceptedRnd) != nil || 
	   d.Decode(&decidedIdx) != nil || 
	   d.Decode(&accepted) != nil || 
	   d.Decode(&l) != nil {
	} else {
	
		op.log = log
		op.promisedRnd = promisedRnd
		op.acceptedRnd = acceptedRnd
		
		for idx := op.decidedIdx+1 ; idx <= decidedIdx ; idx++ {
			if ((idx-1 < len(op.log))&&(idx-1>=0)) {
				msg := ApplyMsg{
					CommandValid: true,
					Command:      op.log[idx-1], 
					CommandIndex: idx-1,
				}
				op.applyCh <- msg 
			}
		}
		op.decidedIdx = decidedIdx 
		op.accepted = accepted
		op.l = l
	}
}