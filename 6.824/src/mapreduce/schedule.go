package mapreduce

import "fmt"
import "sync"
// import "time"

//
// schedule() starts and waits for all tasks in the given phase (Map
// or Reduce). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	// TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
	//

    var wg sync.WaitGroup
    for i := 0; i < ntasks; i++ {
        debug("wait %d:%d %v task find the available channel\n", i, ntasks, phase)

        wg.Add(1)

        go func(file string, taskNum int) {
            defer wg.Done()
            for {
                //make DoTaskArgs
                args := new(DoTaskArgs)
                args.JobName = jobName
                args.File = file
                args.Phase = phase
                args.NumOtherPhase = n_other
                args.TaskNumber = taskNum

                //found available channel
                channel := <-registerChan

                //sync rpc call?
                ok := call(channel, "Worker.DoTask", args, new(struct{}))
                if ok {
                    debug("schedule %d task to %s\n", i, channel)
                    go func() {
                        registerChan <- channel
                    }() // send without holding the lock.
                    break
                } else {
                    debug("schedule worker:%s DoTask error\n", channel)
                    // time.Sleep(1 * time.Second)
                }
            }
        }(mapFiles[i], i)
    }

    debug("%v %v tasks scheduled, now wait\n", ntasks, phase)
    wg.Wait()

    fmt.Printf("Schedule: %v phase done\n", phase)
}
