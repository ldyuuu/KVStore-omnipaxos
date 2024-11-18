package kvpaxos

import (
	"crypto/rand"
	"fmt"
	"math/big"
	"time" 
	"cs651/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
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
	ck.ClerkID = nrand()
	ck.RequestID = 1
	ck.lastLeader = 0
	return ck
}

func (ck *Clerk) Get(key string) string {
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

			done := make(chan bool, 1)
			go func() {
				ok := ck.servers[server].Call("KVServer.Get", &args, &reply)
				if ok && reply.Err == OK {
					done <- true
				} else {
					done <- false
				}
			}()

			select {
			case success := <-done:
				if success {
					ck.lastLeader = server
					return reply.Value
				}
			case <-time.After(500 * time.Millisecond): 
				fmt.Printf("Get request timed out for server %d, retrying...\n", server)
			}
		}
	}
}

func (ck *Clerk) PutAppend(key string, value string, op string) {
	args := PutAppendArgs{
		Key:       key,
		Value:     value,
		Op:        op,
		ClientID:  ck.ClerkID,
		RequestID: ck.RequestID,
	}
	ck.RequestID++
	//fmt.Printf("client print:%v\n", args)

	for {
		for i := 0; i < len(ck.servers); i++ {
			server := (ck.lastLeader + i) % len(ck.servers)
			var reply PutAppendReply

			done := make(chan bool, 1)
			go func() {
				ok := ck.servers[server].Call("KVServer.PutAppend", &args, &reply)
				if ok  {
					done <- true
				} else {
					done <- false
				}
			}()

			select {
			case success := <-done:
				if success &&reply.Err==OK{
					ck.lastLeader = server
					return
				}else if success &&reply.Err== ErrWrongLeader{
					break
				}
			case <-time.After(500 * time.Millisecond): 
				fmt.Printf("PutAppend request timed out for server %d, retrying...\n", server)
			}
		}
	}
}


func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
