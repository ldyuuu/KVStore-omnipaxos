package kvpaxos

import (
	"crypto/rand"
	"fmt"
	"math/big"
	//"time"
	"cs651/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	ClerkID   int64
	RequestID int64
	
	lastLeader int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers

	ck.ClerkID = nrand()       // 随机生成 ClerkID
	ck.RequestID = 1           // 初始化 RequestID
	ck.lastLeader = 0          // 从第一个服务器开始
	return ck
	// You'll have to add code here.
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	//time.Sleep(100*time.Millisecond)
	args := GetArgs{
		Key:      key,
		ClientID: ck.ClerkID,
		RequestID: ck.RequestID,
	}
	ck.RequestID++

	for {
		for i := 0; i < len(ck.servers); i++ {
			server := (ck.lastLeader + i) % len(ck.servers)
			var reply GetReply
			ok := ck.servers[server].Call("KVServer.Get", &args, &reply)
			if ok && reply.Err == OK {
				ck.lastLeader = server // 更新上次成功访问的服务器
				return reply.Value
			}
		}
	}
	// You will have to modify this function.
	return ""
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	//time.Sleep(100*time.Millisecond)
	args := PutAppendArgs{
		Key:       key,
		Value:     value,
		Op:        op,
		ClientID:  ck.ClerkID,
		RequestID: ck.RequestID,
	}
	ck.RequestID++
	fmt.Printf("%v\n",args)
	for {
		for i := 0; i < len(ck.servers); i++ {
			server := (ck.lastLeader + i) % len(ck.servers)
			var reply PutAppendReply
			//fmt.Printf("sever %d\n",i)
			ok := ck.servers[server].Call("KVServer.PutAppend", &args, &reply)
			if ok && reply.Err == OK {
				ck.lastLeader = server // 更新上次成功访问的服务器
				return
			}
		}
	}
	// You will have to modify this function.
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
