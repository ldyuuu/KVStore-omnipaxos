package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardctrler to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import (
	"crypto/rand"
	"cs651/a3b-sharding/shardctrler"
	"cs651/labrpc"
	"math/big"
	"time"
	//"fmt"
)

// which shard is a key in?
// please use this function,
// and please do not change it.
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardctrler.NShards
	return shard
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	sm       *shardctrler.Clerk
	config   shardctrler.Config
	make_end func(string) *labrpc.ClientEnd
	clientID int64 // 唯一标识客户端
	requestID int  // 请求编号，用于确保请求唯一
	preferred map[int]int // Preferred server for each shard
	// You will have to modify this struct.
	
}

// the tester calls MakeClerk.
//
// ctrlers[] is needed to call shardctrler.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
func MakeClerk(ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.sm = shardctrler.MakeClerk(ctrlers)
	ck.make_end = make_end
	ck.clientID = nrand()
	ck.requestID = 0
	ck.preferred= make(map[int]int)
	// You'll have to add code here.
	return ck
}

func (ck *Clerk) Get(key string) string {
	args := GetArgs{}
	args.Key = key
	args.ClientID = ck.clientID

	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			// Use preferred server if available for this shard
			
			start := ck.preferred[shard] // Defaults to 0 if not present
	

			// Try each server for the shard, starting from the preferred one
			for si := 0; si < len(servers); si++ {
				serverIndex := (start + si) % len(servers)
				srv := ck.make_end(servers[serverIndex])
				var reply GetReply
				ok := srv.Call("ShardKV.Get", &args, &reply)

				if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
					// Update preferred server on success
					
					ck.preferred[shard] = serverIndex
				
					return reply.Value
				}
				if ok && reply.Err == ErrWrongGroup {
					break
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
		// Ask the shard controller for the latest configuration
		ck.config = ck.sm.Query(-1)
	}
}

func (ck *Clerk) PutAppend(key string, value string, op string) {
	args := PutAppendArgs{}
	args.Key = key
	args.Value = value
	args.Op = op
	args.ClientID = ck.clientID
	args.RequestID = ck.requestID

	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			// Use preferred server if available for this shard
	
			start := ck.preferred[shard] // Defaults to 0 if not present


			// Try each server for the shard, starting from the preferred one
			for si := 0; si < len(servers); si++ {
				serverIndex := (start + si) % len(servers)
				srv := ck.make_end(servers[serverIndex])
				var reply PutAppendReply
				ok := srv.Call("ShardKV.PutAppend", &args, &reply)

				if ok && reply.Err == OK {
					// Update preferred server on success
	
					ck.preferred[shard] = serverIndex
		
					ck.requestID++ // Increment request ID on success
					return
				}
				if ok && reply.Err == ErrWrongGroup {
					break
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
		// Ask the shard controller for the latest configuration
		ck.config = ck.sm.Query(-1)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
