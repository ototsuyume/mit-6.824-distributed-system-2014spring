package shardmaster

import "net"
import "fmt"
import "net/rpc"
import "log"
import "paxos"
import "sync"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "time"

type ShardMaster struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       bool // for testing
	unreliable bool // for testing
	px         *paxos.Paxos

	configs []Config // indexed by config num

	seq int
}

type Optype int

const (
	OpJoin Optype = iota
	OpLeave
	OpMove
	OpQuery
)

type Op struct {
	Opr   Optype
	GID   int64
	Svr   []string
	Shard int
	Pid   string
}

func (sm *ShardMaster) newcfg() *Config {
	cfg := Config{Num: len(sm.configs), Groups: make(map[int64][]string)}
	if len(sm.configs) > 0 {
		for i, j := range sm.configs[len(sm.configs)-1].Shards {
			cfg.Shards[i] = j
		}

		for gid, svr := range sm.configs[len(sm.configs)-1].Groups {
			cfg.Groups[gid] = svr
		}
	}
	return &cfg
}

func (sm *ShardMaster) rebanlance() {
	count := make(map[int64]int)
	unallocated := 0
	for gid, _ := range sm.configs[len(sm.configs)-1].Groups {
		count[gid] = 0
	}

	for _, gid := range sm.configs[len(sm.configs)-1].Shards {
		if gid <= 0 {
			unallocated++
			continue
		}

		c, _ := count[gid]
		count[gid] = c + 1
	}
	avg := NShards / len(sm.configs[len(sm.configs)-1].Groups)
	res := NShards % len(sm.configs[len(sm.configs)-1].Groups)

	//If unallocated is equal to 0, that means a new group is been added
	if unallocated == 0 {
		from := make(map[int64]int)
		to := int64(0)
		for gid, c := range count {
			if c > avg {
				if res > 0 {
					c--
					res--
				}
				if c == avg {
					continue
				}
				from[gid] = c - avg
			} else if c == 0 {
				to = gid
			}
		}

		for pos, gid := range sm.configs[len(sm.configs)-1].Shards {
			if c, ok := from[gid]; ok && c > 0 {
				from[gid] = c - 1
				sm.configs[len(sm.configs)-1].Shards[pos] = to
			}
		}
	} else {
		for gid, c := range count {
			if c <= avg {
				if res > 0 {
					c--
					res--
				}
				if avg == c {
					continue
				}
				for i, j := 0, 0; i < NShards && j < avg-c; i++ {
					if sm.configs[len(sm.configs)-1].Shards[i] <= 0 {
						sm.configs[len(sm.configs)-1].Shards[i] = gid
					}
				}
			}
		}
	}
}

func (sm *ShardMaster) apply(o Op) {
	switch o.Opr {
	case OpJoin:
		cfg := sm.newcfg()
		cfg.Groups[o.GID] = o.Svr
		sm.configs = append(sm.configs, *cfg)
		sm.rebanlance()
	case OpLeave:
		cfg := sm.newcfg()
		for pos, gid := range cfg.Shards {
			if gid == o.GID {
				cfg.Shards[pos] = 0
			}
		}
		delete(cfg.Groups, o.GID)
		sm.configs = append(sm.configs, *cfg)

		sm.rebanlance()
	case OpMove:
		cfg := sm.newcfg()
		cfg.Shards[o.Shard] = o.GID
		sm.configs = append(sm.configs, *cfg)
	}
}

func (sm *ShardMaster) wait(seq int) Op {
	to := 10 * time.Millisecond
	for {
		if ok, val := sm.px.Status(seq); ok {
			return val.(Op)
		}
		time.Sleep(to)
		if to < 10*time.Second {
			to *= 2
		}
	}
}

func (sm *ShardMaster) sync(o Op) {
	var ro Op
	for {
		ok, v := sm.px.Status(sm.seq + 1)
		if ok {
			ro = v.(Op)
		} else {
			sm.px.Start(sm.seq+1, o)
			ro = sm.wait(sm.seq + 1)
		}

		sm.apply(ro)
		sm.seq++
		sm.px.Done(sm.seq)
		if ro.Pid == o.Pid {
			break
		}
	}
}

func (sm *ShardMaster) genpid() string {
	return fmt.Sprintf("%d_%d", time.Now().UnixNano(), sm.me)
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.sync(Op{OpJoin, args.GID, args.Servers, 0, sm.genpid()})
	return nil
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) error {
	// Your code here.
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.sync(Op{OpLeave, args.GID, nil, 0, sm.genpid()})
	return nil
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.sync(Op{OpMove, args.GID, nil, args.Shard, sm.genpid()})
	return nil
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) error {
	// Your code here.
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.sync(Op{OpQuery, 0, nil, 0, sm.genpid()})
	if args.Num == -1 {
		reply.Config = sm.configs[len(sm.configs)-1]
	} else {
		reply.Config = sm.configs[args.Num]
	}
	return nil
}

// please don't change this function.
func (sm *ShardMaster) Kill() {
	sm.dead = true
	sm.l.Close()
	sm.px.Kill()
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *ShardMaster {
	gob.Register(Op{})

	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int64][]string{}

	rpcs := rpc.NewServer()
	rpcs.Register(sm)

	sm.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	sm.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for sm.dead == false {
			conn, err := sm.l.Accept()
			if err == nil && sm.dead == false {
				if sm.unreliable && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if sm.unreliable && (rand.Int63()%1000) < 200 {
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
			if err != nil && sm.dead == false {
				fmt.Printf("ShardMaster(%v) accept: %v\n", me, err.Error())
				sm.Kill()
			}
		}
	}()

	return sm
}
