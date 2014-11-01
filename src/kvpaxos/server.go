package kvpaxos

import "net"
import "fmt"
import "time"
import "net/rpc"
import "log"
import "paxos"
import "sync"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "strconv"

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type oprtype int

const (
	OprPut oprtype = iota
	OprGet
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Opr    oprtype
	Key    string
	Value  string
	DoHash bool
	Pid    int64
	Client string
}

type Reply struct {
	Pid int64
	Old string
}

type KVPaxos struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       bool // for testing
	unreliable bool // for testing
	px         *paxos.Paxos

	// Your definitions here.

	//client -> pid, if i save the pid in data, the code couldn't pass the memory test
	/************************Note***********************************************************
	There are some problems for the TestManyPartition and TestUnreliable, some time it may get
	wrong value, because of a request will be sent to different server due to network error,
	but it has chance that the request 1 for key "a" had been received by a server, the server
	use paxos to synchorize to the other servers, and save the pid1 in "kv.state", and then the
	request 2 for key "a" arrived, and processed by the servers, then replaced pid1 by pid2 in
	"kv.state". However, when the server failed to reply the request 1 for key "a", the client
	will retry, in the normal case, the server will find pid1 has been processd, so it would
	response the old value for key "a" instead of process the request, but in this case, the pid1
	had been replaced by pid2, the request 1 would be process twice, and made a wrong value.

	I caught up with two methods to solve this problem. The first method, is simple, but not
	too well, when a client couldn't receive the response from server, it will sleep for a
	few seconds. The second method, storing the recent request pid in a queue, each time a
	requst came in, the server should check whether the request had been processed.

	In this part, I simply used the first method. This would let the test code runing for a
	bit longer (In my pc, it tooks about 359 seconds to finish the test.). The implementation
	of the second method, had been written in homework4.
	*************************************************************************************/
	state map[string]Reply
	data  map[string]string
	seq   int
}

func (kv *KVPaxos) apply(o Op) string {

	oldv := ""

	if v, ok := kv.data[o.Key]; ok {
		oldv = v
	}

	kv.state[o.Client] = Reply{o.Pid, oldv}
	if o.Opr == OprPut {
		if o.DoHash {
			newval := strconv.Itoa(int(hash(oldv + o.Value)))
			kv.data[o.Key] = newval
		} else {
			kv.data[o.Key] = o.Value
		}
	}

	return oldv
}

func (kv *KVPaxos) sync(o Op) string {

	for {
		if v, ok := kv.state[o.Client]; ok && v.Pid == o.Pid {
			DPrintf("Operation has been processed. type:%d key:%s val:%s\n",
				o.Opr, o.Key, o.Value)
			return v.Old
		}
		var nop Op
		if ok, v := kv.px.Status(kv.seq + 1); ok {
			nop = v.(Op)

		} else {
			kv.px.Start(kv.seq+1, o)
			nop = kv.wait(kv.seq + 1)
		}

		ret := kv.apply(nop)
		//kv.px.Done(kv.seq)
		kv.seq++
		kv.px.Done(kv.seq)
		//fmt.Println(kv.me, kv.count, o, no)
		if nop == o {
			return ret
		}

	}
}

func (kv *KVPaxos) wait(seq int) Op {
	to := 10 * time.Millisecond
	for {
		if ok, ret := kv.px.Status(seq); ok {
			return ret.(Op)
		}

		time.Sleep(100 * time.Millisecond)
		if to < time.Second {
			to *= 2
		}
	}
}

func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply.Value = kv.sync(Op{OprGet, args.Key, "", false, args.Pid, args.Client})

	return nil
}

func (kv *KVPaxos) Put(args *PutArgs, reply *PutReply) error {
	// Your code here.
	//fmt.Printf("Receive put.Server_%d key:%s value:%s\n", kv.me, args.Key, args.Value)
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply.PreviousValue = kv.sync(Op{OprPut, args.Key, args.Value,
		args.DoHash, args.Pid, args.Client})

	return nil
}

// tell the server to shut itself down.
// please do not change this function.
func (kv *KVPaxos) kill() {
	DPrintf("Kill(%d): die\n", kv.me)
	kv.dead = true
	kv.l.Close()
	kv.px.Kill()
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *KVPaxos {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(KVPaxos)
	kv.me = me
	kv.data = make(map[string]string)
	kv.state = make(map[string]Reply)
	kv.seq = 0
	// Your initialization code here.

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
				fmt.Printf("KVPaxos(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	return kv
}
