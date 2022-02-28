package sidejob

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/betas-in/logger"
	"github.com/betas-in/pool"
)

// type WorkerGroup interface {
// 	SetQueue(*Queue)
// 	Process(ctx context.Context, workerCtx *pool.WorkerContext, id string) error
// }

type workerGroup struct {
	log       *logger.Logger
	heartbeat time.Duration
	poll      time.Duration
	queue     *Queue
}

// NewWorkerGroup ...
func NewWorkerGroup(log *logger.Logger, heartbeat, poll time.Duration) WorkerGroup {
	return &workerGroup{
		log:       log,
		heartbeat: heartbeat,
		poll:      poll,
	}
}

func (w *workerGroup) SetQueue(q *Queue) {
	w.queue = q
}

func (w *workerGroup) Process(ctx context.Context, workerCtx *pool.WorkerContext, id string) {
	workerCtx.Heartbeat <- pool.Heartbeat{ID: id, Ping: true}

	heartbeatTicker := time.NewTicker(w.heartbeat)
	pollTicker := time.NewTicker(w.poll)
	defer heartbeatTicker.Stop()
	defer pollTicker.Stop()

	worker, err := w.queue.GetWorker(id)
	if err != nil {
		w.LogErr(id, err)
		workerCtx.Heartbeat <- pool.Heartbeat{ID: id, Closed: true}
		return
	}

	for {
		select {
		case <-pollTicker.C:

			// w.queue.Publish("100")

			job, err := worker.Consume()
			if err != nil {
				w.LogErr(id, err)
				continue
			}
			if job == "" {
				continue
			}

			// Your code
			time.Sleep(100 * time.Millisecond)
			successfullyProcessed := true
			processedCount := 1

			jobID, _ := strconv.ParseInt(job, 10, 64)
			if jobID%23 == 0 {
				successfullyProcessed = false
				processedCount = 0
			}

			if successfullyProcessed {
				err = worker.Ack()
				if err != nil {
					w.LogErr(id, err)
					continue
				}
				workerCtx.Processed <- job
			} else {
				err = worker.Reject(job)
				if err != nil {
					w.LogErr(id, err)
					continue
				}
			}

			workerCtx.Heartbeat <- pool.Heartbeat{ID: id, Processed: int64(processedCount)}
		case j := <-workerCtx.Jobs:
			workerCtx.Heartbeat <- pool.Heartbeat{ID: id, Processed: 1}
			workerCtx.Processed <- j
		case <-workerCtx.Close:
			workerCtx.Heartbeat <- pool.Heartbeat{ID: id, Closed: true}
			return
		case <-ctx.Done():
			workerCtx.Heartbeat <- pool.Heartbeat{ID: id, Closed: true}
			return
		case <-heartbeatTicker.C:
			workerCtx.Heartbeat <- pool.Heartbeat{ID: id, Ping: true}
		}
	}
}

func (w *workerGroup) LogErr(id string, err error) {
	w.log.Error(w.FullName(id)).Msgf("%+v", err)
}

func (w *workerGroup) FullName(id string) string {
	return fmt.Sprintf("%s%s%s", w.queue.QName, separator, id)
}
