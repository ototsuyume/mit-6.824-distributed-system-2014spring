package shardkv

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "paxos"
import "sync"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "shardmaster"
import "strconv"

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type StateGet struct {
}

type StateReply struct {
	Key []string
	Val []string
}

type OpType int

const (
	OpGet OpType = iota
	OpPut
	OpJoin
	OpLeave
)

type Value struct {
	Val     string
	Version int
}

type Op struct {
	// Your definitions here.
	Opr  OpType
	Key  string
	Val  string
	Hash bool
	Pid  string
	Data map[string]Value
	Req  map[string]reqdata
	Num  int
}

type reqdata struct {
	Key string
	Val string
}

type Visited struct {
	data   map[string]reqdata
	reqmap map[string]string
	queue  []string
	head   int
	tail   int
	size   int
}

func (v *Visited) Init(size int) {
	v.data = make(map[string]reqdata)
	v.reqmap = make(map[string]string)
	v.size = size
	v.queue = make([]string, size)
}

func (v *Visited) Check(pid string) bool {
	_, ok := v.data[pid]
	return ok
}

func (v *Visited) Add(pid string, key string, val string) {
	if len(v.data) == v.size {
		DPrintf("Data overflow.\n")
		oldpid := v.queue[v.head]
		kv := v.data[oldpid]
		delete(v.data, pid)
		delete(v.reqmap, kv.Key)
		v.head = (v.head + 1) % v.size
	}

	v.queue[v.tail] = pid
	v.tail = (v.tail + 1) % v.size
	v.data[pid] = reqdata{key, val}
	v.reqmap[key] = pid
}

func (v *Visited) Get(pid string) (string, bool) {
	val, ok := v.data[pid]
	return val.Val, ok
}

func (v *Visited) GetByKey(key string) (string, string) {
	DPrintf("Get pid from visited data. pid:%s key:%s val;%s\n",
		v.reqmap[key], key, v.data[v.reqmap[key]].Val)
	return v.reqmap[key], v.data[v.reqmap[key]].Val
}

type ShardKV struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       bool // for testing
	unreliable bool // for testing
	sm         *shardmaster.Clerk
	px         *paxos.Paxos
	cfg        *shardmaster.Config
	cfglock    sync.Mutex
	data       map[string]Value
	gid        int64 // my replica group ID
	seq        int
	v          Visited
	// Your definitions here.
}

func (kv *ShardKV) wait(seq int) Op {
	to := 10 * time.Millisecond
	for {
		if ok, val := kv.px.Status(seq); ok {
			return val.(Op)
		}
		time.Sleep(to)
		if to < 10*time.Second {
			to *= 2
		}
	}
}

func (kv *ShardKV) apply(o Op) {

	switch o.Opr {
	case OpGet:
		//val, _ := kv.data[o.Key]
		//kv.v.Add(o.Pid, val)
		DPrintf("Get result key:%s ret:%s gid:%d seq:%d pid:%s\n",
			o.Key, kv.data[o.Key], kv.gid, kv.cfg.Num, o.Pid)

	case OpPut:
		oldv, _ := kv.data[o.Key]
		if o.Hash {
			newval := strconv.Itoa(int(hash(oldv.Val + o.Val)))
			kv.data[o.Key] = Value{newval, oldv.Version + 1}

			kv.v.Add(o.Pid, o.Key, oldv.Val)
			DPrintf("Set hash result key:%s val:%#v ret:%s gid:%d seq:%d pid:%s\n",
				o.Key, kv.data[o.Key], oldv.Val, kv.gid, kv.cfg.Num, o.Pid)
		} else {
			kv.data[o.Key] = Value{o.Val, oldv.Version + 1}
			DPrintf("Set result key:%s val:%s  gid:%d seq:%d\n",
				o.Key, kv.data[o.Key], kv.gid, kv.cfg.Num)
		}
	case OpJoin:
		for key, val := range o.Data {

			DPrintf("Join key:%s val:%s gid:%d seq:%d\n", key, val.Val, kv.gid, kv.cfg.Num)
			if val.Version > kv.data[key].Version {
				kv.data[key] = val
			}
		}

		for pid, v := range o.Req {
			DPrintf("Add visited pid:%s key:%s val:%s\n", pid, v.Key, v.Val)
			kv.v.Add(pid, v.Key, v.Val)
		}
	case OpLeave:
		newdata := make(map[string]Value)
		for key, val := range kv.data {
			newdata[key] = val
		}
		for key, _ := range o.Data {
			DPrintf("Leave key:%s val:%s gid:%d seq:%d\n", key, kv.data[key], kv.gid, kv.cfg.Num)
			delete(newdata, key)
		}
		kv.data = newdata
	}
}

func (kv *ShardKV) sync(o Op) {
	var ro Op
	if kv.v.Check(o.Pid) {
		return
	}
	//fmt.Printf("begin to sync %#v\n", o)
	for {
		ok, v := kv.px.Status(kv.seq + 1)
		if ok {
			ro = v.(Op)
		} else {
			kv.px.Start(kv.seq+1, o)
			ro = kv.wait(kv.seq + 1)
		}

		kv.apply(ro)
		kv.seq++
		kv.px.Done(kv.seq)
		if ro.Pid == o.Pid {
			break
		}
	}
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	DPrintf("Get %s  %d\n", args.Key, kv.gid)
	DPrintf("Get Shard:%#v seq:%d gid:%d hash:%d\n", kv.cfg.Shards,
		kv.cfg.Num, kv.gid, key2shard(args.Key))

	if kv.cfg.Shards[key2shard(args.Key)] != kv.gid {
		DPrintf("Get error. not my group %d\n", kv.gid)
		reply.Err = ErrWrongGroup
		return nil

	}

	reply.Err = OK
	kv.sync(Op{Opr: OpGet, Key: args.Key, Pid: args.Pid})
	reply.Value = kv.data[args.Key].Val
	DPrintf("Get result key:%s ret:%s gid:%d seq:%d\n", args.Key, reply.Value, kv.gid, kv.cfg.Num)
	return nil
}

func (kv *ShardKV) Put(args *PutArgs, reply *PutReply) error {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	DPrintf("Set %s %s gid:%d\n", args.Key, args.Value, kv.gid)
	DPrintf("Set Shard:%#v seq:%d gid:%d hash:%d\n", kv.cfg.Shards,
		kv.cfg.Num, kv.gid, key2shard(args.Key))

	if kv.cfg.Shards[key2shard(args.Key)] != kv.gid {
		DPrintf("Set error group %d\n", kv.gid)
		reply.Err = ErrWrongGroup
		return nil

	}

	reply.Err = OK
	reply.PreviousValue = ""
	kv.sync(Op{Opr: OpPut, Key: args.Key, Val: args.Value,
		Pid: args.Pid, Hash: args.DoHash})
	if v, ok := kv.v.Get(args.Pid); ok {
		reply.PreviousValue = v
	}

	return nil
}

type GetShardReq struct {
	Num   int
	Shard int
	Gid   int64
}

type GetShardRsp struct {
	Data    map[string]Value
	ReqData map[string]reqdata
}

func (kv *ShardKV) GetShard(req *GetShardReq, rsp *GetShardRsp) error {
	kv.mu.Lock()

	// Use a fake operation to synchorize the data
	fake := Op{Opr: OpGet, Key: "tmp", Pid: kv.genPid(int(rand.Int31()), int(rand.Int31()))}
	kv.sync(fake)

	op := Op{Opr: OpLeave, Data: make(map[string]Value)}
	rsp.Data = make(map[string]Value)
	rsp.ReqData = make(map[string]reqdata)
	for k, v := range kv.data {

		if req.Shard == key2shard(k) {
			DPrintf("Reshard: %s %#v gid:%d seq:%d\n", k, v, kv.gid, kv.cfg.Num)
			rsp.Data[k] = v
			op.Data[k] = v
			pid, val := kv.v.GetByKey(k)
			rsp.ReqData[pid] = reqdata{k, val}
		}
	}
	DPrintf("Shard:%#v seq:%d gid:%d\n", kv.cfg.Shards, kv.cfg.Num, kv.gid)
	kv.cfg.Shards[req.Shard] = req.Gid
	/*
		if len(op.Data) > 0 {
			op.Pid = kv.genPid(req.Num, req.Shard)
			kv.sync(op)
		}
	*/
	kv.mu.Unlock()
	return nil
}

func (kv *ShardKV) genPid(cfgnum int, shard int) string {
	return fmt.Sprintf("ChangeJoinState_%d_%d", cfgnum, shard)
}

//
// Ask the shardmaster if there's a new configuration;
// if so, re-configure.
//

func (kv *ShardKV) Reconfig(cfg *shardmaster.Config) {
	for i := kv.cfg.Num + 1; i <= cfg.Num; i++ {
		ncfg := kv.sm.Query(i)
		if kv.cfg.Num == 0 {
			kv.cfg = &ncfg
			continue
		}
		DPrintf("Query new config %#v %#v  gid:%d\n", cfg.Num, kv.cfg.Num, kv.gid)
		for j := 0; j < shardmaster.NShards; j++ {
			if kv.cfg.Shards[j] != kv.gid && ncfg.Shards[j] == kv.gid {
				req := GetShardReq{ncfg.Num, j, kv.gid}
				rsp := GetShardRsp{}
				svrs := kv.cfg.Groups[kv.cfg.Shards[j]]
				pos := 0
				for len(svrs) > 0 {
					if call(svrs[pos], "ShardKV.GetShard", &req, &rsp) {
						break
					}
					pos = (pos + 1) % len(svrs)
				}

				if len(rsp.Data) > 0 {
					kv.mu.Lock()
					kv.sync(Op{Opr: OpJoin, Pid: kv.genPid(cfg.Num, j),
						Data: rsp.Data, Req: rsp.ReqData, Num: cfg.Num})
					kv.mu.Unlock()
				}
				DPrintf("Get shard succeed. gid:%d seq:%d res:%#v reqdata:%#v\n",
					kv.gid, cfg.Num, rsp.Data, rsp.ReqData)
				kv.cfg.Shards[j] = kv.gid
			}
		}

		kv.cfg = &ncfg

	}
}

func (kv *ShardKV) tick() {
	kv.cfglock.Lock()
	defer kv.cfglock.Unlock()
	cfg := kv.sm.Query(-1)
	if cfg.Num > kv.cfg.Num {
		kv.Reconfig(&cfg)
	}
}

// tell the server to shut itself down.
func (kv *ShardKV) kill() {
	kv.dead = true
	kv.l.Close()
	kv.px.Kill()
}

//
// Start a shardkv server.
// gid is the ID of the server's replica group.
// shardmasters[] contains the ports of the
//   servers that implement the shardmaster.
// servers[] contains the ports of the servers
//   in this replica group.
// Me is the index of this server in servers[].
//
func StartServer(gid int64, shardmasters []string,
	servers []string, me int) *ShardKV {
	gob.Register(Op{})

	kv := new(ShardKV)
	kv.cfg = &shardmaster.Config{Num: 0}
	kv.me = me
	kv.gid = gid
	kv.sm = shardmaster.MakeClerk(shardmasters)
	kv.data = make(map[string]Value)
	kv.v.Init(1024) //cache at most 100 of the latest request
	file, _ := os.Create("log.txt")
	log.SetOutput(file)
	// Your initialization code here.
	// Don't call Join().

	rpcs := rpc.NewServer()
	rpcs.Register(kv)

	kv.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	kv.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for kv.dead == false {
			conn, err := kv.l.Accept()
			if err == nil && kv.dead == false {
				if kv.unreliable && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if kv.unreliable && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					go rpcs.ServeConn(conn)
				} else {
					go rpcs.ServeConn(conn)
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && kv.dead == false {
				fmt.Printf("ShardKV(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	go func() {
		for kv.dead == false {
			kv.tick()
			time.Sleep(250 * time.Millisecond)
		}
	}()
	return kv
}
