package mapreduce

import "sync"

func (mr *Master) schedule(phase jobPhase) {
	var ntasks int
	var numOtherPhase int
	switch phase {
	case mapPhase:
		ntasks = len(mr.files)     // number of map tasks
		numOtherPhase = mr.nReduce // number of reducers
	case reducePhase:
		ntasks = mr.nReduce           // number of reduce tasks
		numOtherPhase = len(mr.files) // number of map tasks
	}
	debug("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, numOtherPhase)

	// ToDo: Complete this function. See the description in the assignment.
	var wg sync.WaitGroup
	wg.Add(ntasks)
	for task := range ntasks {
		go func(taskNum int) {
			defer wg.Done()
			for {
				worker := <-mr.registerChannel
				args := RunTaskArgs{
					JobName: mr.jobName,
					File: mr.files[taskNum], //only in the case of mapPhase
					Phase: phase,
					TaskNumber: taskNum,
					NumOtherPhase: numOtherPhase,
				}
				if phase == reducePhase {
					args.File = ""
				}
				ok := call(worker, "Worker.RunTask", &args, new(struct{}))
				if ok {
					go func() {
						mr.registerChannel <- worker
					}()
					return
				}
			}
		}(task)
	}
	wg.Wait()
	debug("Schedule: %v phase finished\n", phase)
}
