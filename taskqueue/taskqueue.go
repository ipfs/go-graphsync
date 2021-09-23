package taskqueue

import (
	"context"
	"time"

	"github.com/ipfs/go-peertaskqueue"
	"github.com/ipfs/go-peertaskqueue/peertask"
	peer "github.com/libp2p/go-libp2p-core/peer"
)

const thawSpeed = time.Millisecond * 100

// Executor runs a single task on the queue
type Executor interface {
	ExecuteTask(ctx context.Context, pid peer.ID, task *peertask.Task) bool
}

// TaskQueue is a wrapper around peertaskqueue.PeerTaskQueue that manages running workers
// that pop tasks and execute them
type TaskQueue struct {
	ctx           context.Context
	cancelFn      func()
	peerTaskQueue *peertaskqueue.PeerTaskQueue
	workSignal    chan struct{}
	ticker        *time.Ticker
}

// NewTaskQueue initializes a new queue
func NewTaskQueue(ctx context.Context) *TaskQueue {
	ctx, cancelFn := context.WithCancel(ctx)
	return &TaskQueue{
		ctx:           ctx,
		cancelFn:      cancelFn,
		peerTaskQueue: peertaskqueue.New(),
		workSignal:    make(chan struct{}, 1),
		ticker:        time.NewTicker(thawSpeed),
	}
}

// PushTask pushes a new task on to the queue
func (tq *TaskQueue) PushTask(p peer.ID, task peertask.Task) {
	tq.peerTaskQueue.PushTasks(p, task)
	select {
	case tq.workSignal <- struct{}{}:
	default:
	}
}

// TaskDone marks a task as completed so further tasks can be executed
func (tq *TaskQueue) TaskDone(p peer.ID, task *peertask.Task) {
	tq.peerTaskQueue.TasksDone(p, task)
}

// Startup runs the given number of task workers with the given executor
func (tq *TaskQueue) Startup(workerCount uint64, executor Executor) {
	for i := uint64(0); i < workerCount; i++ {
		go tq.worker(executor)
	}
}

// Shutdown shuts down all running workers
func (tq *TaskQueue) Shutdown() {
	tq.cancelFn()
}

func (tq *TaskQueue) worker(executor Executor) {
	targetWork := 1
	for {
		pid, tasks, _ := tq.peerTaskQueue.PopTasks(targetWork)
		for len(tasks) == 0 {
			select {
			case <-tq.ctx.Done():
				return
			case <-tq.workSignal:
				pid, tasks, _ = tq.peerTaskQueue.PopTasks(targetWork)
			case <-tq.ticker.C:
				tq.peerTaskQueue.ThawRound()
				pid, tasks, _ = tq.peerTaskQueue.PopTasks(targetWork)
			}
		}
		for _, task := range tasks {
			terminate := executor.ExecuteTask(tq.ctx, pid, task)
			if terminate {
				return
			}
		}
	}
}
