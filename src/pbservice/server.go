package pbservice

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "viewservice"
import "os"
import "syscall"
import "math/rand"
import "sync"
import "strconv"

//import "strconv"

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		n, err = fmt.Printf(format, a...)
	}
	return
}

type SyncReq struct {
	Data map[string]Value
}

type SyncRsp struct {
	Result bool
}

type Value struct {
	Val  string
	Last string
	Seq  int64
}

type PBServer struct {
	l          net.Listener
	dead       bool // for testing
	unreliable bool // for testing
	me         string
	vs         *viewservice.Clerk
	done       sync.WaitGroup
	finish     chan interface{}
	// Your declarations here.
	primary bool
	backup  string
	vn      uint
	data    map[string]Value
	locker  *sync.RWMutex
	resync  bool
}

func (pb *PBServer) Put(args *PutArgs, reply *PutReply) error {
	// Your code here.
	if !pb.primary {
		reply.Err = ErrWrongServer
		return fmt.Errorf(ErrWrongServer)
	}

	return pb.putimpl(args, reply)
}

func (pb *PBServer) putimpl(args *PutArgs, reply *PutReply) error {
	pb.locker.Lock()
	defer pb.locker.Unlock()
	reply.Err = OK
	val := Value{"", "", 0}
	if v, ok := pb.data[args.Key]; ok {
		val = v
	}
	if val.Seq == args.Seq {
		reply.PreviousValue = val.Last
		return nil
	}
	if args.DoHash {
		newval := strconv.Itoa(int(hash(val.Val + args.Value)))
		reply.PreviousValue = val.Val
		val.Last = val.Val
		val.Val = newval
		val.Seq = args.Seq
		pb.data[args.Key] = val

	} else {
		pb.data[args.Key] = Value{args.Value, val.Val, args.Seq}
	}

	if pb.primary && pb.backup != "" {
		res := PutReply{}
		if err := pb.ForwardPut(args, &res); err != nil || res.Err != OK {
			pb.resync = true
		}
	}
	return nil
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {
	if !pb.primary {
		reply.Err = ErrWrongServer
		return fmt.Errorf(ErrWrongServer)
	}
	pb.locker.RLock()
	defer pb.locker.RUnlock()
	if v, ok := pb.data[args.Key]; ok {
		reply.Value = v.Val
	} else {
		return fmt.Errorf(ErrNoKey)
	}
	return nil
}

func (pb *PBServer) HandleForwardPut(args *PutArgs, reply *PutReply) error {
	if pb.primary {
		reply.Err = OK
		return nil
	}
	return pb.putimpl(args, reply)
	//return fmt.Errorf("Rpc error")
}

func (pb *PBServer) HandleSyncData(args *SyncReq, reply *SyncRsp) error {
	if pb.primary {
		return nil
	}
	pb.data = args.Data
	reply.Result = true
	return nil
}

func (pb *PBServer) ForwardPut(args *PutArgs, reply *PutReply) error {
	ok := call(pb.backup, "PBServer.HandleForwardPut", args, reply)
	if !ok {
		return fmt.Errorf("ForwardPut error\n")
	}
	return nil
}

func (pb *PBServer) SyncData(req *SyncReq, rsp *SyncRsp) error {
	ok := call(pb.backup, "PBServer.HandleSyncData", req, rsp)
	if !ok {
		return fmt.Errorf("SyncData error\n")
	}
	return nil
}

// ping the viewserver periodically.
func (pb *PBServer) tick() {
	// Your code here.

	vx, _ := pb.vs.Ping(pb.vn)
	pb.vn = vx.Viewnum
	if vx.Primary == pb.me {
		pb.primary = true
		if vx.Backup != "" && vx.Backup != pb.backup {
			pb.backup = vx.Backup
			pb.resync = true
		}
	} else {
		pb.primary = false
		pb.backup = ""
	}

	if pb.backup != "" && pb.resync {
		if pb.SyncData(&SyncReq{pb.data}, &SyncRsp{}) != nil {
			pb.resync = false
		}
	}
}

// tell the server to shut itself down.
// please do not change this function.
func (pb *PBServer) kill() {
	pb.dead = true
	pb.l.Close()
}

func StartServer(vshost string, me string) *PBServer {
	pb := new(PBServer)
	pb.me = me
	pb.vs = viewservice.MakeClerk(me, vshost)
	pb.finish = make(chan interface{})

	// Your pb.* initializations here.
	pb.locker = new(sync.RWMutex)
	pb.data = make(map[string]Value)
	pb.resync = true

	rpcs := rpc.NewServer()
	rpcs.Register(pb)

	os.Remove(pb.me)
	l, e := net.Listen("unix", pb.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	pb.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for pb.dead == false {
			conn, err := pb.l.Accept()
			if err == nil && pb.dead == false {
				if pb.unreliable && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if pb.unreliable && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					pb.done.Add(1)
					go func() {
						rpcs.ServeConn(conn)
						pb.done.Done()
					}()
				} else {
					pb.done.Add(1)
					go func() {
						rpcs.ServeConn(conn)
						pb.done.Done()
					}()
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && pb.dead == false {
				fmt.Printf("PBServer(%v) accept: %v\n", me, err.Error())
				pb.kill()
			}
		}
		DPrintf("%s: wait until all request are done\n", pb.me)
		pb.done.Wait()
		// If you have an additional thread in your solution, you could
		// have it read to the finish channel to hear when to terminate.
		close(pb.finish)
	}()

	pb.done.Add(1)
	go func() {
		for pb.dead == false {
			pb.tick()
			time.Sleep(viewservice.PingInterval)
		}
		pb.done.Done()
	}()

	return pb
}
