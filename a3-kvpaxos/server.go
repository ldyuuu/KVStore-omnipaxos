package kvpaxos

import (
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time" // 引入time包
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
	Name      string
	Key       string
	Value     string
	ClientID  int64
	RequestID int64
}

type KVServer struct {
	mu           sync.Mutex
	me           int
	rf           *omnipaxos.OmniPaxos
	applyCh      chan omnipaxos.ApplyMsg
	dead         int32 // set by Kill()

	enableLogging int32
	store         map[string]string
	lastRequest   map[int64]int64       
	lastResponse  map[int64]interface{} 
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	op := Op{
		Name:      "Get",
		Key:       args.Key,
		Value:     "",
		ClientID:  args.ClientID,
		RequestID: args.RequestID,
	}
    
	_, _, ok := kv.rf.Proposal(op)
	if !ok {
		reply.Err = ErrWrongLeader
		return
	}

	timeout := time.After(500 * time.Millisecond)
	for {
		select {
		case <-timeout:
			return
		default:
			kv.mu.Lock()
			if lastReqID := kv.lastRequest[args.ClientID]; lastReqID >= args.RequestID {
				reply.Value = kv.store[args.Key]
                fmt.Printf("%d just get %v\n",kv.me,reply.Value)
				reply.Err = OK
				kv.mu.Unlock()
				return
			}
			kv.mu.Unlock()
		}
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	op := Op{
		Name:      args.Op,
		Key:       args.Key,
		Value:     args.Value,
		ClientID:  args.ClientID,
		RequestID: args.RequestID,
	}

	kv.mu.Lock()
	if lastReqID := kv.lastRequest[args.ClientID]; lastReqID >= args.RequestID {
		reply.Err = OK
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	_, _, ok := kv.rf.Proposal(op)
	if !ok {
		reply.Err = ErrWrongLeader
		return
	}
	timeout := time.After(500 * time.Millisecond)
	for {
		select {
		case <-timeout:
			return
		default:
			kv.mu.Lock()
			if lastReqID := kv.lastRequest[args.ClientID]; lastReqID >= args.RequestID {
				reply.Err = OK
				kv.mu.Unlock()
				return
			}
			kv.mu.Unlock()
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
func (kv *KVServer) listenApplyCh() {
	for msg := range kv.applyCh {
		if op, ok := msg.Command.(Op); ok {
			kv.mu.Lock()
			if lastReqID:= kv.lastRequest[op.ClientID]; lastReqID >= op.RequestID {
				kv.mu.Unlock()
				continue
			}
			if op.Name == "Put" {
				kv.store[op.Key] = op.Value
			} else if op.Name == "Append" {
				kv.store[op.Key] += op.Value
			}
            fmt.Printf("%d just %v with %v %v\n",kv.me,op.Name,op.ClientID,op.RequestID)
			kv.lastRequest[op.ClientID] = op.RequestID

			kv.mu.Unlock()
		}
	}
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
	kv.applyCh = make(chan omnipaxos.ApplyMsg,50000)
	kv.rf = omnipaxos.Make(servers, me, persister, kv.applyCh)
   
    kv.lastRequest=make(map[int64]int64)
    kv.lastResponse = make(map[int64]interface{})
	// You may need initialization code here.
    go kv.listenApplyCh()
	return kv
}
