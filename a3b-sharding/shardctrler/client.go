package shardctrler

//
// Shardctrler clerk.
//

import (
	"crypto/rand"
	"cs651/labrpc"
	//"fmt"
	"math/big"
	"time"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	ClientID  int64               
	RequestID int64               

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
	ck.ClientID = nrand() 
	ck.RequestID = 0      
	// Your code here.
	return ck
}

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{
		ClientID:  ck.ClientID,
		RequestID: ck.RequestID,
	}
	ck.RequestID++
	// Your code here.
	args.Num = num
	
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply QueryReply
			ok := srv.Call("ShardCtrler.Query", args, &reply)
			if ok && reply.WrongLeader == false {
				//fmt.Printf("Client:%v\n",reply.Config)
				return reply.Config
				
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{
		ClientID:  ck.ClientID,
		RequestID: ck.RequestID,
	}
	ck.RequestID++
	// Your code here.
	args.Servers = servers
	//fmt.Printf("Client Join:%v\n",servers)
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply JoinReply
			ok := srv.Call("ShardCtrler.Join", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{
		ClientID:  ck.ClientID,
		RequestID: ck.RequestID,
	}
	ck.RequestID++
	//fmt.Printf("Client Leave:%v\n",gids)
	// Your code here.
	args.GIDs = gids

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply LeaveReply
			ok := srv.Call("ShardCtrler.Leave", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{
		ClientID:  ck.ClientID,
		RequestID: ck.RequestID,}
	ck.RequestID++
	// Your code here.
	args.Shard = shard
	args.GID = gid
	//fmt.Printf("Client Move:%d %d\n",shard,gid)
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply MoveReply
			ok := srv.Call("ShardCtrler.Move", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
