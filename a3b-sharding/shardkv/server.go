package shardkv

import (
	omnipaxos "cs651/a2-omnipaxos"
	"cs651/labgob"
	"cs651/labrpc"
	"sync"
	"time"
	"cs651/a3b-sharding/shardctrler"
	//"fmt"
	"bytes"
)

type Op struct {
    Name      string
    Key       string
    Value     string
    ClientID  int64
    RequestID int
    Config    shardctrler.Config // 用于配置变更
    Shard     int                // 分片编号
    Data      map[string]string  // 分片数据
}

	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.


type ShardKV struct {
	mu            sync.Mutex
	me            int
	op            *omnipaxos.OmniPaxos
	applyCh       chan omnipaxos.ApplyMsg
	make_end      func(string) *labrpc.ClientEnd
	gid           int
	ctrlers       []*labrpc.ClientEnd
	maxpaxosstate int // snapshot if log grows this big
	
	
	peers        []*labrpc.ClientEnd
    backupData    map[int]map[string]string // 用于备份的旧分片数据
    shardStatus   map[int]string            // 每个分片的状态：ready/migrating
    backupReady   int
	isReconfiguring bool
	persister 	*omnipaxos.Persister
    config      shardctrler.Config
    data        map[int]map[string]string // 每个分片的数据
    lastRequest map[int64]int             // 客户端 ID -> 最新请求 ID
    mck         *shardctrler.Clerk        // 用于与 shardctrler 通信
    
	// Your definitions here.
}

func (kv *ShardKV) MigrateShard(args *MigrateShardArgs, reply *MigrateShardReply) {
    kv.mu.Lock()
    defer kv.mu.Unlock()
    

    if kv.config.Num<args.ConfigNum {
        reply.Err = "nit ready" // 表示备份未完成
        return
    }
    if kv.backupReady < args.ConfigNum {
        //fmt.Printf("[Server %d-%d] backup is %d,dot ready,asking for %d is \n", kv.gid, kv.me, kv.backupReady,args.ConfigNum)
        reply.Err = "bkupErrNotReady" // 表示备份未完成
        return
    }

    if data, exists := kv.backupData[args.Shard]; exists {
        // 从备份数据中提供分片
        dataCopy := make(map[string]string)
        for k, v := range data {
            dataCopy[k] = v
        }
        reply.Data = dataCopy
        reply.Err = OK
       // fmt.Printf("[Server %d-%d] Providing backup data for Shard %d: %+v\n", kv.gid, kv.me, args.Shard, dataCopy)
    } else {
        reply.Data = nil
        reply.Err = ErrNoKey
    }
}

func (kv *ShardKV) backupCurrentData() {
    kv.mu.Lock()
    defer kv.mu.Unlock()

    kv.backupData = make(map[int]map[string]string)
    for shard, data := range kv.data {
        kv.backupData[shard] = make(map[string]string)
        for k, v := range data {
            kv.backupData[shard][k] = v
        }
    }

    kv.backupReady = kv.config.Num 
   // fmt.Printf("[Server %d-%d] Backed up data for config %d: %+v\n", kv.gid, kv.me, kv.config.Num, kv.backupData)
}

func (kv *ShardKV) requestShardCopy(shard int, sourceGID int,oldConfig shardctrler.Config) {
    for {
        kv.mu.Lock()
        servers := oldConfig.Groups[sourceGID]
        if len(servers) == 0 {
            //fmt.Printf("[Server %d] No servers found for GID %d during Shard %d migration\n", kv.me, sourceGID, shard)
            kv.shardStatus[shard] = "ready" 
            kv.mu.Unlock()
            return
        }
        kv.mu.Unlock()

        for _, server := range servers {
            srv := kv.make_end(server)
            var reply MigrateShardReply
            args := MigrateShardArgs{
                Shard:     shard,
                ConfigNum: kv.config.Num,
            }
            ok := srv.Call("ShardKV.MigrateShard", &args, &reply)
            if ok && (reply.Err==OK||reply.Err==ErrNoKey)  {
                kv.mu.Lock()
                if reply.Data != nil {
                    // 如果有数据，更新到本地分片
                    if _, exists := kv.data[shard]; !exists {
                        kv.data[shard] = make(map[string]string)
                    }
                    for k, v := range reply.Data {
                        kv.data[shard][k] = v
                    }
                   // fmt.Printf("[Server %d-%d] Successfully pulled Shard %d data from GID %d: %+v\n", kv.gid,kv.me, shard, sourceGID, reply.Data)
                } else {
                    //fmt.Printf("[Server %d-%d] Shard %d has no data to pull from GID %dand saying%v\n", kv.gid, kv.me,shard, sourceGID,reply.Err)
                }
                kv.shardStatus[shard] = "ready" // 标记为迁移完成
                kv.mu.Unlock()
                return
            } else  {
                //fmt.Printf("[Server %d-%d] Shard %d has no data to pull from GID %dand saying%v\n", kv.gid, kv.me,shard, sourceGID,reply.Err)
             }
        }

        time.Sleep(100 * time.Millisecond)
    }
}

func (kv *ShardKV) migrateShard(shard int, sourceGID int) {
    for {
        kv.mu.Lock()
        if kv.shardStatus[shard] == "ready" {
            kv.mu.Unlock()
            return
        }

        servers := kv.config.Groups[sourceGID]
        kv.mu.Unlock()

        if len(servers) == 0 {
           // fmt.Printf("[Server %d] now config: %+v\n", kv.me, kv.config)
            //fmt.Printf("[Server %d] No servers found for GID %d during Shard %d migration\n", kv.me, sourceGID, shard)
            
            kv.mu.Lock()
            kv.shardStatus[shard] = "ready" 
            kv.mu.Unlock()

            return
        }

        for _, server := range servers {
            srv := kv.make_end(server)
            var reply MigrateShardReply
            args := MigrateShardArgs{
                Shard:     shard,
                ConfigNum: kv.config.Num,
            }
            ok := srv.Call("ShardKV.MigrateShard", &args, &reply)
            if ok && reply.Err == OK {
                kv.mu.Lock()
                if reply.Data != nil {
                    kv.data[shard] = reply.Data
                   // fmt.Printf("[Server %d-%d] Successfully migrated Shard %d with data %+vfor config %d\n", kv.gid,kv.me, shard, reply.Data,kv.config.Num)
                } else {
                    //fmt.Printf("[Server %d-%d] Successfully migrated Shard %d with no data\n", kv.gid,kv.me, shard)
                }
                kv.shardStatus[shard] = "ready"
                kv.mu.Unlock()
                return
            }
            //fmt.Printf("[Server %d-%d] Failed to migrate Shard %d from server %s with reply err:%v\n", kv.gid,kv.me, shard, server, reply.Err)
        }
        time.Sleep(100 * time.Millisecond)
    }
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
    shard := key2shard(args.Key)

    kv.mu.Lock()
	if kv.isReconfiguring {
        reply.Err = ErrWrongGroup
        kv.mu.Unlock()
        return
    }
    
    if !kv.isShardOwned(shard) {
        reply.Err = "not readddddy"
        kv.mu.Unlock()
        return
    }

    op := Op{
        Name:      "Get",
        Key:       args.Key,
        ClientID:  args.ClientID,
        RequestID: args.RequestID,
    }
    kv.mu.Unlock()

    _, _, ok := kv.op.Proposal(op)
    if !ok {
        reply.Err = ErrWrongLeader
        return
    }

    timeout := time.After(200 * time.Millisecond)
    for {
        select {
        case <-timeout:
            reply.Err = "ErrTimeout"
            return
        default:
            kv.mu.Lock()
            if lastReqID, exists := kv.lastRequest[args.ClientID]; exists && lastReqID >= args.RequestID {
                if value, found := kv.data[shard][args.Key]; found {
                    reply.Value = value
                    reply.Err = OK
                } else {
                    reply.Err = ErrNoKey
                }
                kv.mu.Unlock()
                return
            }
            kv.mu.Unlock()
        }
    }
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
    shard := key2shard(args.Key)
	kv.mu.Lock()
	
	if kv.isReconfiguring {
        reply.Err = ErrWrongGroup
       // fmt.Printf("server%d-%d isReconfiguring\n",kv.gid,kv.me)
        kv.mu.Unlock()

        return
    }
    
	
    if !kv.isShardOwned(shard) {
        reply.Err = ErrWrongGroup
        kv.mu.Unlock()
        return
    }

    op := Op{
        Name:      args.Op,
        Key:       args.Key,
        Value:     args.Value,
        ClientID:  args.ClientID,
        RequestID: args.RequestID,
    }
    kv.mu.Unlock()

    _, _, ok := kv.op.Proposal(op)
    if !ok {
        reply.Err = ErrWrongLeader
        return
    }

    timeout := time.After(200 * time.Millisecond)
    for {
        select {
        case <-timeout:
            reply.Err = "ErrTimeout"
            return
        default:
            kv.mu.Lock()
            if lastReqID, exists := kv.lastRequest[args.ClientID]; exists && lastReqID >= args.RequestID {
                // fmt.Printf("[Server %d] PutAppend success: %s -> key %s, value %s\n",
                //     kv.me, args.Op, args.Key, args.Value)
                
				reply.Err = OK
                kv.mu.Unlock()
                return
            }
            kv.mu.Unlock()
        }
    }
}

func (kv *ShardKV) checkAllServersSynced() bool {
    
    for _, peer := range kv.peers { // 遍历所有服务器
        srv := peer
        var reply SyncConfigReply
        ok := srv.Call("ShardKV.SyncConfigNum", &SyncConfigArgs{}, &reply)

        if !ok || reply.ConfigNum != kv.config.Num || !reply.AllReady {
			//fmt.Printf("server:%d find%d is not ready\n",kv.gid,peer)
            return false 
        }else{
			//fmt.Printf("server:%d find %d-%d is ready!!!!!!\n",kv.gid,reply.Gid,reply.Me)
		}

    }
    return true
}

func (kv *ShardKV) SyncConfigNum(args *SyncConfigArgs, reply *SyncConfigReply) {
    kv.mu.Lock()
    defer kv.mu.Unlock()
    reply.Gid=kv.gid
    reply.Me=kv.me
    reply.ConfigNum = kv.config.Num
    reply.AllReady = true

    for _, status := range kv.shardStatus {
        if status == "migrating" {
            reply.AllReady = false
            break
        }
    }
}

func (kv *ShardKV) pollConfig() {
    for {
        time.Sleep(100 * time.Millisecond)
        kv.mu.Lock()
        
        latestConfig := kv.mck.Query(kv.config.Num + 1) 
		if kv.config.Num == 0 && latestConfig.Num > kv.config.Num {
            kv.isReconfiguring = true
            kv.mu.Unlock()
            
            op := Op{Name: "Reconfigure", Config: latestConfig}
            kv.op.Proposal(op)
            continue
        }
        kv.mu.Unlock()
        if latestConfig.Num > kv.config.Num && kv.checkReady()&& kv.isReconfiguring==false{
            //fmt.Printf("server;%d-%d Ready for next config: %d, now: %v, next: %v\n",kv.gid,kv.me, latestConfig.Num, kv.config.Shards, latestConfig.Shards)
            kv.mu.Lock()
            kv.isReconfiguring = true
            op := Op{Name: "Reconfigure", Config: latestConfig}
            ok := false
            kv.mu.Unlock()
            retries := 0
            for !ok && retries < 3 { 
                _, _, ok = kv.op.Proposal(op)
            
                if !ok {
                    retries++
                    //fmt.Printf("server:%d Failed to propose reconfiguration (attempt %d), retrying...\n", kv.gid, retries)
                    time.Sleep(200 * time.Millisecond)
                }
            }
            
        } else {
            if latestConfig.Num == kv.config.Num && kv.checkReady() {
               // fmt.Printf("----------------------------------server;%d think everyone is ready on %d\n",kv.gid, kv.config.Num)
          
                kv.mu.Lock()
                kv.isReconfiguring = false
                kv.mu.Unlock()
            }
        }
    }
}

func (kv *ShardKV) applyLoop() {
    for msg := range kv.applyCh {
        if op, ok := msg.Command.(Op); ok {
            var handleReconfigure bool
            var config shardctrler.Config

            kv.mu.Lock()
            switch op.Name {
            case "Get", "Put", "Append":
                shard := key2shard(op.Key)
                if kv.isShardOwned(shard) {
                    if lastReqID, exists := kv.lastRequest[op.ClientID]; !exists || op.RequestID > lastReqID {
                        if _, exists := kv.data[shard]; !exists {
                            kv.data[shard] = make(map[string]string)
                        }
                         if op.Name == "Put" {
							kv.data[shard][op.Key] = op.Value
							//fmt.Printf("[Server %d-%d] Applied Put: key %s -> value %sin shard%d\n",
								//kv.gid,kv.me, op.Key, op.Value,shard)
						} else if op.Name == "Append" {
							if existingValue, exists := kv.data[shard][op.Key]; exists {
								kv.data[shard][op.Key] = existingValue + op.Value
							} else {
								kv.data[shard][op.Key] = op.Value
							}
							// fmt.Printf("[Server %d-%d] Applied Append: key %s -> value %s (existing: %s)\n",
							// 	kv.gid,kv.me, op.Key, op.Value, kv.data[shard][op.Key])
						}
                        kv.lastRequest[op.ClientID] = op.RequestID
                    }
                }
            case "Reconfigure":
				kv.isReconfiguring = true
				//time.Sleep(1000*time.Millisecond)
                goto ApplyReconfigure
				// 确保先清空所有其他操作
				// for {
				// 	select {
				// 	case msg := <-kv.applyCh:
				// 		if op, ok := msg.Command.(Op); ok && op.Name != "Reconfigure" {
				// 			kv.mu.Lock()
				// 			shard := key2shard(op.Key)
				// 			if kv.isShardOwned(shard) {
				// 				if lastReqID, exists := kv.lastRequest[op.ClientID]; !exists || op.RequestID > lastReqID {
				// 					if _, exists := kv.data[shard]; !exists {
				// 						kv.data[shard] = make(map[string]string)
				// 					}
				// 					if op.Name == "Put" {
				// 						kv.data[shard][op.Key] = op.Value
				// 					} else if op.Name == "Append" {
				// 						if existingValue, exists := kv.data[shard][op.Key]; exists {
				// 							kv.data[shard][op.Key] = existingValue + op.Value
				// 						} else {
				// 							kv.data[shard][op.Key] = op.Value
				// 						}
				// 					}
				// 					kv.lastRequest[op.ClientID] = op.RequestID
				// 				}
				// 			}
				// 			kv.mu.Unlock()
				// 		}
				// 	default:
				// 		goto ApplyReconfigure
				// 	}
				// }
			ApplyReconfigure:
				handleReconfigure = true
				config = op.Config
           
            }

            // if (kv.maxpaxosstate!=-1){
			// 	//fmt.Printf("snnappshoutting---------------------------------------------------------------------------------------\n")
            //     kv.createSnapshot()
            // }
            kv.mu.Unlock()

            if handleReconfigure {
                kv.handleReconfigure(config)
            }
        }
    }
}
func (kv *ShardKV) checkReady() bool {
    kv.mu.Lock()
    defer kv.mu.Unlock()

    for _, status := range kv.shardStatus {
        if status != "ready" {
           // fmt.Printf("[Server %d-%d] Shard %d is not ready (status: %s)\n", kv.gid, kv.me, shard, status)
            return false
        }
    }

   // fmt.Printf("[Server %d-%d] All shards are ready\n", kv.gid, kv.me)
    return true
}
func (kv *ShardKV) isShardOwned(shard int) bool {
    gid := kv.config.Shards[shard]
    return gid == kv.gid
}

func (kv *ShardKV) isShardReady(shard int) bool {
    return kv.isShardOwned(shard) && kv.shardStatus[shard] == "ready"
}


func (kv *ShardKV) handleReconfigure(newConfig shardctrler.Config) {
    kv.mu.Lock()
    if newConfig.Num <= kv.config.Num {
        kv.mu.Unlock()
        return 
    }

    //fmt.Printf("[Server %d-%d] Starting reconfiguration from config %d to %d\n", kv.gid, kv.me, kv.config.Num, newConfig.Num)

    kv.isReconfiguring = true
    oldConfig := kv.config
    kv.config = newConfig
    //kv.backupReady = false
    for shard := 0; shard < shardctrler.NShards; shard++ {
        kv.shardStatus[shard] = "migrating"
    }
    kv.mu.Unlock()

    kv.backupCurrentData()

    for shard := 0; shard < shardctrler.NShards; shard++ {
        sourceGID := oldConfig.Shards[shard]
        go kv.requestShardCopy(shard, sourceGID,oldConfig)
    }

    for {
        kv.mu.Lock()
        allPulled := true
        for shard := 0; shard < shardctrler.NShards; shard++ {
            if kv.shardStatus[shard] != "ready" {
              //  fmt.Printf("server%d-%d shard %d is not ready\n",kv.gid,kv.me,shard)
                allPulled = false
                break
            }
        }
        kv.mu.Unlock()

        if allPulled {
            break
        }
        time.Sleep(50 * time.Millisecond)
    }

    kv.mu.Lock()
    kv.isReconfiguring = false
    kv.mu.Unlock()
}

func (kv *ShardKV) createSnapshot() {
    w := new(bytes.Buffer)
    e := labgob.NewEncoder(w)
    
    e.Encode(kv.data)
    e.Encode(kv.config)
    e.Encode(kv.lastRequest)
    e.Encode(kv.shardStatus)
    
    snapshot := w.Bytes()
    kv.persister.SaveSnapshot(snapshot)
    //fmt.Printf("[Server %d] Created snapshot\n", kv.me)
}

func (kv *ShardKV) restoreSnapshot(snapshot []byte) {
    r := bytes.NewBuffer(snapshot)
    d := labgob.NewDecoder(r)
    
    var data map[int]map[string]string
    var config shardctrler.Config
    var lastRequest map[int64]int
    var shardStatus map[int]string
    
    if d.Decode(&data) == nil &&
        d.Decode(&config) == nil &&
        d.Decode(&lastRequest) == nil &&
        d.Decode(&shardStatus) == nil {
        kv.data = data
        kv.config = config
        kv.lastRequest = lastRequest
        kv.shardStatus = shardStatus
		kv.isReconfiguring = true
		//go kv.checkAndCatchUpConfig()
       // fmt.Printf("[Server %d] Restored from snapshot with cnfig %d\n", kv.me,kv.config.Num)
    } else{
       // fmt.Printf("[Server %d] Failed to restore snapshot\n", kv.me)
    }
}

func (kv *ShardKV) checkAndCatchUpConfig() {
    kv.mu.Lock()
    currentConfigNum := kv.config.Num
    kv.mu.Unlock()

    highestConfigNum := currentConfigNum
    allServersReady := true

    for _, peer := range kv.peers {
        if peer == kv.peers[kv.me] {
            continue // 跳过自身
        }
        var reply SyncConfigReply
        ok := peer.Call("ShardKV.SyncConfigNum", &SyncConfigArgs{}, &reply)
        if ok {
            if reply.ConfigNum > highestConfigNum {
                highestConfigNum = reply.ConfigNum
            }
            if !reply.AllReady {
                allServersReady = false
            }
        }
    }

    // 如果自身配置落后，需要触发 Reconfigure
    if highestConfigNum > currentConfigNum {
        // fmt.Printf("[Server %d] Config %d is outdated. Catching up to %d.\n",
        //     kv.me, currentConfigNum, highestConfigNum)
        kv.triggerReconfigureWithPeers(highestConfigNum, allServersReady)
    }
}

func (kv *ShardKV) triggerReconfigureWithPeers(targetConfigNum int, allReady bool) {
    kv.mu.Lock()
    if kv.isReconfiguring || kv.config.Num >= targetConfigNum {
        kv.mu.Unlock()
        return
    }
    kv.isReconfiguring = true
    kv.mu.Unlock()

    targetConfig := kv.mck.Query(targetConfigNum)
    if targetConfig.Num != targetConfigNum {
       // fmt.Printf("[Server %d] Failed to fetch config %d from shardctrler\n", kv.me, targetConfigNum)
        kv.mu.Lock()
        kv.isReconfiguring = false
        kv.mu.Unlock()
        return
    }

   // fmt.Printf("[Server %d] Starting reconfigure to config %d\n", kv.me, targetConfigNum)
        for shard, gid := range targetConfig.Shards {
            if gid == kv.gid && kv.config.Shards[shard] != kv.gid {
                kv.shardStatus[shard] = "migrating"
                go kv.migrateShard(shard, kv.config.Shards[shard])
            }
        }
    
		for {
			kv.mu.Lock()
			allReady := true
			for _,status := range kv.shardStatus {
				if status == "migrating" {
					allReady = false
					break
				}
			}
			kv.mu.Unlock()
	
			if allReady {
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
	
		kv.mu.Lock()
		kv.config = targetConfig
		//fmt.Printf("[Server %d-%d] now has new config %d\n", kv.gid, kv.me,targetConfig.Num)
				
		kv.mu.Unlock()
}
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	kv.op.Kill()
	// Your code here, if desired.
}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying OmniPaxos
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the OmniPaxos state along with the snapshot.
//
// the k/v server should snapshot when OmniPaxos's saved state exceeds
// maxOmniPaxosstate bytes, in order to allow OmniPaxos to garbage-collect its
// log. if maxOmniPaxosstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *omnipaxos.Persister, maxOmniPaxosstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	
	kv := new(ShardKV)
	kv.me = me
	kv.maxpaxosstate = maxOmniPaxosstate
	kv.make_end = make_end
	kv.gid = gid
	kv.peers=servers
	kv.ctrlers = ctrlers
    kv.isReconfiguring=false
    kv.backupReady=-1
	kv.data = make(map[int]map[string]string)
    kv.lastRequest = make(map[int64]int)
    kv.shardStatus = make(map[int]string) 
	kv.persister=persister

	for shard := 0; shard < shardctrler.NShards; shard++ {
        kv.shardStatus[shard] = "ready" 
    }
	// if snapshot := persister.ReadSnapshot(); snapshot != nil && len(snapshot) > 0 {
	// 	fmt.Printf(" restore-----------------------------------------1\n")
    //     kv.restoreSnapshot(snapshot)
    // }
	go kv.applyLoop()
	
	//if kv.me==0 {fmt.Printf("imrrhere")}
    // 初始化日志通道和 OmniPaxos 实例
    kv.applyCh = make(chan omnipaxos.ApplyMsg,50)
    //time.Sleep(100 * time.Millisecond)
	
	kv.mck = shardctrler.MakeClerk(ctrlers)
	
    
	
	kv.op = omnipaxos.Make(servers, me, persister, kv.applyCh)
 
    go kv.pollConfig()

    return kv
}