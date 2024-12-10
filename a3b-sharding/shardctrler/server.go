package shardctrler

import (
	omnipaxos "cs651/a2-omnipaxos"
	"cs651/labgob"
	"cs651/labrpc"
	"sync"
	"time"
	"sort"
	//"fmt"
)

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	op      *omnipaxos.OmniPaxos
	applyCh chan omnipaxos.ApplyMsg

	// Your data here.
	clientRecords map[int64]int64 

	configs []Config // indexed by config num
}

type Op struct {
	Type      string     
    Args      interface{} 
    ClientID  int64       
    RequestID int64      
	// Your data here.
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	op := Op{
		Type:      "Join",
		Args:      *args,
		ClientID:  args.ClientID,   
		RequestID: args.RequestID,  
	}

	_, _, ok := sc.op.Proposal(op)
	if !ok {
		reply.WrongLeader = true
		return
	}

	// 等待日志应用
	timeout := time.After(500 * time.Millisecond)
	for {
		select {
		case <-timeout:
			reply.Err = "Timeout"
			return
		default:
			sc.mu.Lock()
			if lastReqID, exists := sc.clientRecords[args.ClientID]; exists && lastReqID >= args.RequestID {
				reply.Err = OK
				sc.mu.Unlock()
				return
			}
			sc.mu.Unlock()
		}
	}
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	op := Op{
		Type:      "Leave",
		Args:      *args,
		ClientID:  args.ClientID,
		RequestID: args.RequestID,
	}

	_, _, ok := sc.op.Proposal(op)
	if !ok {
		reply.WrongLeader = true
		return
	}

	timeout := time.After(500 * time.Millisecond)
	for {
		select {
		case <-timeout:
			reply.Err = "Timeout"
			return
		default:
			sc.mu.Lock()
			if lastReqID, exists := sc.clientRecords[args.ClientID]; exists && lastReqID >= args.RequestID {
				reply.Err = OK
				sc.mu.Unlock()
				return
			}
			sc.mu.Unlock()
		}
	}
}
func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	op := Op{
		Type:      "Move",
		Args:      *args,
		ClientID:  args.ClientID,
		RequestID: args.RequestID,
	}

	_, _, ok := sc.op.Proposal(op)
	if !ok {
		reply.WrongLeader = true
		return
	}
	
	timeout := time.After(500 * time.Millisecond)
	for {
		select {
		case <-timeout:
			reply.Err = "Timeout"
			return
		default:
			sc.mu.Lock()
			if lastReqID, exists := sc.clientRecords[args.ClientID]; exists && lastReqID >= args.RequestID {
				reply.Err = OK
				sc.mu.Unlock()
				return
			}
			sc.mu.Unlock()
		}
	}
}
func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	op := Op{
		Type:      "Query",
		Args:      *args,
		ClientID:  args.ClientID,
		RequestID: args.RequestID,
	}

	_, _, ok := sc.op.Proposal(op)
	if !ok {
		reply.WrongLeader = true
		return
	}

	timeout := time.After(500 * time.Millisecond)
	for {
		select {
		case <-timeout:
			reply.Err = "Timeout"
			return
		default:
			sc.mu.Lock()
			if lastReqID, exists := sc.clientRecords[args.ClientID]; exists && lastReqID >= args.RequestID {
				queryArgs := op.Args.(QueryArgs)
				if queryArgs.Num == -1 || queryArgs.Num >= len(sc.configs) {
					reply.Config = sc.configs[len(sc.configs)-1]
				} else {
					reply.Config = sc.configs[queryArgs.Num]
				}
				reply.Err = OK
				sc.mu.Unlock()
				return
			}
			sc.mu.Unlock()
		}
	}
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.op.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) OmniPaxos() *omnipaxos.OmniPaxos {
	return sc.op
}
func (sc *ShardCtrler) listenApplyCh() {
	for msg := range sc.applyCh {
		if op, ok := msg.Command.(Op); ok {
			sc.mu.Lock()
			if sc.clientRecords == nil {
				sc.clientRecords = make(map[int64]int64)
			}
			if lastReqID, exists := sc.clientRecords[op.ClientID]; exists && lastReqID >= op.RequestID {
				sc.mu.Unlock()
				continue
			}

			switch op.Type {
			case "Join":
				args := op.Args.(JoinArgs)
				sc.handleJoin(args)
			case "Leave":
				args := op.Args.(LeaveArgs)
				sc.handleLeave(args)
			case "Move":
				args := op.Args.(MoveArgs)
				sc.handleMove(args)
			case "Query":
			}

			sc.clientRecords[op.ClientID] = op.RequestID

			sc.mu.Unlock()
		}
	}
}
func (sc *ShardCtrler) handleJoin(args JoinArgs) {
	lastConfig := sc.configs[len(sc.configs)-1]
	newConfig := Config{
		Num:    lastConfig.Num + 1,
		Shards: lastConfig.Shards,
		Groups: make(map[int][]string),
	}

	for gid, servers := range lastConfig.Groups {
		newConfig.Groups[gid] = servers
	}

	for gid, servers := range args.Servers {
		newConfig.Groups[gid] = servers
	}

	rebalanceShards(&newConfig)

	sc.configs = append(sc.configs, newConfig)
}

func (sc *ShardCtrler) handleLeave(args LeaveArgs) {
	lastConfig := sc.configs[len(sc.configs)-1]

	newConfig := Config{
		Num:    lastConfig.Num + 1,       
		Shards: lastConfig.Shards,        
		Groups: make(map[int][]string),    
	}

	for gid, servers := range lastConfig.Groups {
		newConfig.Groups[gid] = servers
	}
	for _, gid := range args.GIDs {
		delete(newConfig.Groups, gid)
	}

	rebalanceShards(&newConfig)

	sc.configs = append(sc.configs, newConfig)
}
func rebalanceShards(config *Config) {
	numGroups := len(config.Groups)
	if numGroups == 0 {
		for i := 0; i < NShards; i++ {
			config.Shards[i] = 0
		}
		return
	}

	gids := make([]int, 0, numGroups)
	for gid := range config.Groups {
		gids = append(gids, gid)
	}
	sort.Ints(gids)
	target := NShards / numGroups
	remainder := NShards % numGroups

	shardCount := make(map[int]int)
	for _, gid := range gids {
		shardCount[gid] = 0
	}

	for i, gid := range config.Shards {
		if _, exists := shardCount[gid]; exists {
			shardCount[gid]++
		} else {
			config.Shards[i] = gids[0]
			shardCount[gids[0]]++
		}
	}

	for i, gid := range config.Shards {
		if shardCount[gid] > target {
			for _, newGid := range gids {
				if shardCount[newGid] < target || (shardCount[newGid] == target && remainder > 0) {
					config.Shards[i] = newGid
					shardCount[gid]--
					shardCount[newGid]++
					if shardCount[newGid] == target && remainder > 0 {
						remainder--
					}
					break
				}
			}
		}
	}
}
func (sc *ShardCtrler) handleMove(args MoveArgs) {
	
	lastConfig := sc.configs[len(sc.configs)-1]

	newConfig := Config{
		Num:    lastConfig.Num + 1,        
		Shards: lastConfig.Shards,        
		Groups: make(map[int][]string),    
	}

	for gid, servers := range lastConfig.Groups {
		newConfig.Groups[gid] = servers
	}

	newConfig.Shards[args.Shard] = args.GID

	sc.configs = append(sc.configs, newConfig)
}
// servers[] contains the ports of the set of
// servers that will cooperate via OmniPaxos to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *omnipaxos.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan omnipaxos.ApplyMsg)
	sc.op = omnipaxos.Make(servers, me, persister, sc.applyCh)
	sc.configs[0] = Config{
		Num:    0,                         
		Shards: [NShards]int{},             
		Groups: make(map[int][]string),     
	}
    labgob.Register(QueryArgs{})
    labgob.Register(QueryReply{})
    labgob.Register(JoinArgs{})
    labgob.Register(JoinReply{})
    labgob.Register(LeaveArgs{})
    labgob.Register(LeaveReply{})
    labgob.Register(MoveArgs{})
    labgob.Register(MoveReply{})

	// Your code here.
	go sc.listenApplyCh()
	return sc
}

