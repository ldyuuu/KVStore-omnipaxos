package kvpaxos

import (
	"fmt"
	"log"
	"sync"
	"sync/atomic"

	omnipaxos "cs651/a2-omnipaxos"
	"cs651/labgob"
	"cs651/labrpc"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Name string
	Key string
	Value string
	ClientID int64
	RequestID int64
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *omnipaxos.OmniPaxos
	applyCh chan omnipaxos.ApplyMsg
	dead    int32 // set by Kill()

	enableLogging int32
	store   map[string]string
	// Your definitions here.
}
func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
    
    op := Op{
        Name:      args.Op,
        Key:       args.Key,
        Value:     args.Value,
        ClientID:  args.ClientID,
        RequestID: args.RequestID,
    }

    // 向 Raft 提交 Proposal
    _, _, ok := kv.rf.Proposal(op)
    if !ok {
        reply.Err = ErrWrongLeader
        return
    }
	fmt.Printf("%d is here waiting!!!!!!!!!!!!!\n",kv.me)
    // 等待 applyCh 中的响应
    for msg := range kv.applyCh {

        fmt.Printf("Received from applyCh: %v at index %d\n", msg.Command, msg.CommandIndex)

        if appliedOp, ok := msg.Command.(Op); ok && appliedOp.ClientID == op.ClientID && appliedOp.RequestID == op.RequestID {
            kv.mu.Lock()
            if args.Op == "Put" {
                kv.store[args.Key] = args.Value
            } else if args.Op == "Append" {
                kv.store[args.Key] += args.Value
            }
            kv.mu.Unlock()
            reply.Err = OK
			fmt.Printf("%dstopped\n",kv.me)
            return
        }
    }
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
    op := Op{
        Name:      "Get",
        Key:       args.Key,
        Value:     "",
        ClientID:  args.ClientID,
        RequestID: args.RequestID,
    }

    // 提交 Get 请求
    _, _, ok := kv.rf.Proposal(op)
    if !ok {
        reply.Err = ErrWrongLeader
        return
    }
	fmt.Printf("%d is here waiting!!!!!!!!!!!!!\n",kv.me)
    // 等待 applyCh 中的响应
    for msg := range kv.applyCh {
        fmt.Printf("Received from applyCh: %v at index %d\n", msg.Command, msg.CommandIndex)

        if appliedOp, ok := msg.Command.(Op); ok && appliedOp.ClientID == op.ClientID && appliedOp.RequestID == op.RequestID {
			fmt.Printf("Received from applyCh: %v at index %d\n", msg.Command, msg.CommandIndex)
            kv.mu.Lock()
            reply.Value = kv.store[args.Key]
            kv.mu.Unlock()
            reply.Err = OK
            return
        }
    }
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via OmniPaxos to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying OmniPaxos
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the OmniPaxos state along with the snapshot.
// the k/v server should snapshot when OmniPaxos's saved state exceeds maxomnipaxosstate bytes,
// in order to allow OmniPaxos to garbage-collect its log. if maxomnipaxosstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *omnipaxos.Persister, maxomnipaxosstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	// to enable logging
	// set to 0 to disable
	kv.enableLogging = 1

	// You may need initialization code here.
	kv.store = make(map[string]string)
	kv.applyCh = make(chan omnipaxos.ApplyMsg)
	kv.rf = omnipaxos.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.

	return kv
}
