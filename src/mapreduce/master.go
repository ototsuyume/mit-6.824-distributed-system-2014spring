package mapreduce

import "container/list"
import "fmt"

type WorkerInfo struct {
	address string
	// You can add definitions here.
}

type Status struct {
	Jobid int
	Res   bool
}

// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *MapReduce) KillWorkers() *list.List {
	l := list.New()
	for _, w := range mr.Workers {
		DPrintf("DoWork: shutdown %s\n", w.address)
		args := &ShutdownArgs{}
		var reply ShutdownReply
		ok := call(w.address, "Worker.Shutdown", args, &reply)
		if ok == false {
			fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
		} else {
			l.PushBack(reply.Njobs)
		}
	}
	return l
}

func (mr *MapReduce) RunMaster() *list.List {
	// Your code here
	tmpchan := make(chan Status)
	callworker := func(addr string, method JobType, num int, i int) bool {
		var args DoJobArgs
		var reply DoJobReply
		args.File = mr.file
		args.NumOtherPhase = num
		args.JobNumber = i
		args.Operation = method
		return call(addr, "Worker.DoJob", args, &reply)
	}

	domap := func(jobnum int) {
		addr := <-mr.registerChannel
		ret := callworker(addr, Map, mr.nReduce, jobnum)
		tmpchan <- Status{jobnum, ret}
		mr.registerChannel <- addr
	}

	for i := 0; i < mr.nMap; i++ {
		go domap(i)
	}
	succ_count := 0
	for succ_count < mr.nMap {
		s := <-tmpchan
		if s.Res {
			succ_count++
		} else {
			go domap(s.Jobid)
		}
	}

	doreduce := func(jobnum int) {
		addr := <-mr.registerChannel
		ret := callworker(addr, Reduce, mr.nMap, jobnum)
		tmpchan <- Status{jobnum, ret}
		mr.registerChannel <- addr
	}
	for i := 0; i < mr.nReduce; i++ {
		go doreduce(i)
	}

	succ_count = 0
	for succ_count < mr.nReduce {
		s := <-tmpchan
		if s.Res {
			succ_count++
		} else {
			go doreduce(s.Jobid)
		}
	}

	return mr.KillWorkers()
}
