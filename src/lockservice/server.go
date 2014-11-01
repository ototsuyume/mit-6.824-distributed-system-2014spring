package lockservice

import "net"
import "net/rpc"
import "log"
import "sync"
import "fmt"
import "os"
import "io"
import "time"

type OpType int

const (
	Oplock OpType = iota
	Opunlock
)

type Request struct {
	Op       OpType
	Lockname string
	Pid      string
}

type Response struct {
}

type LRUCache struct {
	cache            map[string]string
	lastest          []string
	head, tail, size int
}

func (cache *LRUCache) Init(size int) {
	cache.size, cache.head, cache.tail = size, 0, 0
	cache.lastest = make([]string, size)
	cache.cache = make(map[string]string)
}

func (cache *LRUCache) Check(pid string, locks string) int {
	if ln, ok := cache.cache[pid]; !ok {
		return -1
	} else if ln != locks {
		return 0
	}
	return 1
}

func (cache *LRUCache) Add(pid string, locks string) {
	if len(cache.cache) == cache.size {
		delete(cache.cache, cache.lastest[cache.head])
		cache.head = (cache.head + 1) % cache.size
	}
	cache.cache[pid] = locks
	cache.lastest[cache.tail] = pid
	cache.tail = (cache.tail + 1) % cache.size
}

type LockServer struct {
	mu    sync.Mutex
	l     net.Listener
	dead  bool // for test_test.go
	dying bool // for test_test.go

	am_primary bool   // am I the primary?
	backup     string // backup's port
	primary    string

	// for each lock name, is it locked?
	locks   map[string]string
	unlocks LRUCache
	reqlist []Request
}

//
// server Lock RPC handler.
//
// you will have to modify this function
//

func (ls *LockServer) Sync(req *Request, rsp *Response) error {
	ls.mu.Lock()
	defer ls.mu.Unlock()
	if req.Op == Oplock {
		ls.locks[req.Lockname] = req.Pid
	} else {
		if _, ok := ls.locks[req.Lockname]; ok {
			delete(ls.locks, req.Lockname)
		}
		ls.unlocks.Add(req.Pid, req.Lockname)
	}
	return nil
}

func (ls *LockServer) Send() {
	addr := ls.primary
	if ls.am_primary {
		addr = ls.backup
	}
	pos := 0
	for i, r := range ls.reqlist {
		if !call(addr, "LockServer.Sync", r, &Response{}) {
			pos = i
			break
		}
	}
	if pos > 0 {
		length := len(ls.reqlist) - pos
		nreq := make([]Request, length)
		copy(nreq, ls.reqlist[pos:])
		ls.reqlist = nreq
	}
}

func (ls *LockServer) Lock(args *LockArgs, reply *LockReply) error {
	ls.mu.Lock()
	defer ls.mu.Unlock()
	pid, ok := ls.locks[args.Lockname]

	if !ok || pid == args.Pid {
		reply.OK = true
		ls.locks[args.Lockname] = args.Pid
		if !ok {
			ls.reqlist = append(ls.reqlist, Request{Oplock, args.Lockname, args.Pid})
		}
		ls.Send()
	} else {
		reply.OK = false
	}

	return nil
}

//
// server Unlock RPC handler.
//
func (ls *LockServer) Unlock(args *UnlockArgs, reply *UnlockReply) error {
	ls.mu.Lock()
	defer ls.mu.Unlock()
	// Your code here.
	if v := ls.unlocks.Check(args.Pid, args.Lockname); v == 0 {
		reply.OK = false
		return nil
	} else if v == 1 {
		reply.OK = true
		return nil
	}
	if _, ok := ls.locks[args.Lockname]; ok {
		delete(ls.locks, args.Lockname)
		reply.OK = true
		ls.unlocks.Add(args.Pid, args.Lockname)
		ls.reqlist = append(ls.reqlist, Request{Opunlock, args.Lockname, args.Pid})
		ls.Send()
	} else {
		ls.unlocks.Add(args.Pid, "")
		ls.reqlist = append(ls.reqlist, Request{Opunlock, "", args.Pid})
		ls.Send()
		reply.OK = false
	}

	return nil
}

//
// tell the server to shut itself down.
// for testing.
// please don't change this.
//
func (ls *LockServer) kill() {
	ls.dead = true
	ls.l.Close()
}

//
// hack to allow test_test.go to have primary process
// an RPC but not send a reply. can't use the shutdown()
// trick b/c that causes client to immediately get an
// error and send to backup before primary does.
// please don't change anything to do with DeafConn.
//
type DeafConn struct {
	c io.ReadWriteCloser
}

func (dc DeafConn) Write(p []byte) (n int, err error) {
	return len(p), nil
}
func (dc DeafConn) Close() error {
	return dc.c.Close()
}
func (dc DeafConn) Read(p []byte) (n int, err error) {
	return dc.c.Read(p)
}

func StartServer(primary string, backup string, am_primary bool) *LockServer {
	ls := new(LockServer)
	ls.backup = backup
	ls.primary = primary
	ls.am_primary = am_primary
	ls.locks = make(map[string]string)
	ls.unlocks.Init(100)
	// Your initialization code here.

	me := ""
	if am_primary {
		me = primary
	} else {
		me = backup
	}

	// tell net/rpc about our RPC server and handlers.
	rpcs := rpc.NewServer()
	rpcs.Register(ls)

	// prepare to receive connections from clients.
	// change "unix" to "tcp" to use over a network.
	os.Remove(me) // only needed for "unix"
	l, e := net.Listen("unix", me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	ls.l = l

	// please don't change any of the following code,
	// or do anything to subvert it.

	// create a thread to accept RPC connections from clients.
	go func() {
		for ls.dead == false {
			conn, err := ls.l.Accept()
			if err == nil && ls.dead == false {
				if ls.dying {
					// process the request but force discard of reply.

					// without this the connection is never closed,
					// b/c ServeConn() is waiting for more requests.
					// test_test.go depends on this two seconds.
					go func() {
						time.Sleep(2 * time.Second)
						conn.Close()
					}()
					ls.l.Close()

					// this object has the type ServeConn expects,
					// but discards writes (i.e. discards the RPC reply).
					deaf_conn := DeafConn{c: conn}

					rpcs.ServeConn(deaf_conn)

					ls.dead = true
				} else {
					go rpcs.ServeConn(conn)
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && ls.dead == false {
				fmt.Printf("LockServer(%v) accept: %v\n", me, err.Error())
				ls.kill()
			}
		}
	}()

	return ls
}
