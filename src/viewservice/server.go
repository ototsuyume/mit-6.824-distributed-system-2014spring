package viewservice

import "net"
import "net/rpc"
import "log"
import "time"
import "sync"
import "fmt"
import "os"

type ViewServer struct {
	mu       sync.Mutex
	l        net.Listener
	dead     bool
	me       string
	primary  string
	backup   string
	others   []string
	lastreq  map[string]time.Time
	viewnum  uint
	lastview uint
	// Your declarations here.
}

//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {

	// Your code here.

	if vs.primary == "" {
		vs.viewnum++
		vs.primary = args.Me
	} else if vs.backup == "" && vs.primary != args.Me {
		vs.viewnum++
		vs.backup = args.Me
	} else if vs.primary == args.Me && args.Viewnum == 0 {
		vs.primary, vs.backup = vs.backup, vs.primary
		vs.viewnum++
	}

	if args.Me == vs.primary {
		vs.lastview = args.Viewnum
	}
	vs.lastreq[args.Me] = time.Now()
	reply.View.Backup = vs.backup
	reply.View.Primary = vs.primary
	reply.View.Viewnum = vs.viewnum
	return nil
}

//
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {

	// Your code here.
	reply.View.Primary = vs.primary
	reply.View.Backup = vs.backup
	reply.View.Viewnum = vs.viewnum

	return nil
}

//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) tick() {
	idle := make([]string, 0)
	cur := time.Now().UnixNano()
	for n, d := range vs.lastreq {
		if (cur-d.UnixNano())/int64(PingInterval) >= DeadPings &&
			vs.lastview == vs.viewnum {
			if n == vs.primary {
				vs.primary, vs.backup = vs.backup, ""
				vs.viewnum++
			} else if n == vs.backup {
				vs.backup = ""
				vs.viewnum++
			}
		} else {
			idle = append(idle, n)
		}
	}

	if vs.backup == "" {
		for _, d := range idle {
			if d != vs.primary {
				vs.backup = d
				break
			}
		}
	}
	// Your code here.
}

//
// tell the server to shut itself down.
// for testing.
// please don't change this function.
//
func (vs *ViewServer) Kill() {
	vs.dead = true
	vs.l.Close()
}

func StartServer(me string) *ViewServer {
	vs := new(ViewServer)
	vs.backup = ""
	vs.primary = ""
	vs.lastreq = make(map[string]time.Time)
	vs.me = me
	// Your vs.* initializations here.

	// tell net/rpc about our RPC server and handlers.
	rpcs := rpc.NewServer()
	rpcs.Register(vs)

	// prepare to receive connections from clients.
	// change "unix" to "tcp" to use over a network.
	os.Remove(vs.me) // only needed for "unix"
	l, e := net.Listen("unix", vs.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	vs.l = l

	// please don't change any of the following code,
	// or do anything to subvert it.

	// create a thread to accept RPC connections from clients.
	go func() {
		for vs.dead == false {
			conn, err := vs.l.Accept()
			if err == nil && vs.dead == false {
				go rpcs.ServeConn(conn)
			} else if err == nil {
				conn.Close()
			}
			if err != nil && vs.dead == false {
				fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
				vs.Kill()
			}
		}
	}()

	// create a thread to call tick() periodically.
	go func() {
		for vs.dead == false {
			vs.tick()
			time.Sleep(PingInterval)
		}
	}()

	return vs
}
