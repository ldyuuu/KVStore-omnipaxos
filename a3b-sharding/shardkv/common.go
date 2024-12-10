package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running OmniPaxos.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientID  int64               // 唯一客户端 ID
	RequestID int
}

type PutAppendReply struct {
	Err Err
}
type MigrateShardArgs struct {
    Shard int                // 分片编号
    Data  map[string]string  // 分片数据
    ConfigNum int            // 配置编号，确保数据属于正确的配置
}

type MigrateShardReply struct {
	Data  map[string]string
    Err Err // 返回的错误信息
}
type GetArgs struct {
	Key string
	ClientID  int64               // 唯一客户端 ID
	RequestID int
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
}
type ConfigStatusReply struct{
	CurrentConfigNum int
	IsReady          bool
}
type ConfigStatusArgs struct {
    TargetConfigNum int
}
type SyncConfigArgs struct{}

type SyncConfigReply struct {
    ConfigNum int
	Gid int
	Me int
	AllReady bool
}