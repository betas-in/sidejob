package sidejob

import (
	"context"
	"fmt"
	"time"

	"github.com/betas-in/pool"
	"github.com/go-redis/redis/v8"
)

// Queue ...
type Queue struct {
	QName     string
	mq        *sidejob
	pool      pool.Pool
	heartbeat chan pool.Heartbeat
	closed    chan struct{}
}

// NewQueue ...
func NewQueue(name string, mq *sidejob) *Queue {
	q := Queue{QName: name, mq: mq, pool: pool.NewPool(name, mq.log), closed: make(chan struct{})}
	q.heartbeat = q.pool.GetHeartbeat()
	go q.listenHeartbeat()
	return &q
}

// Publish ...
func (q *Queue) Publish(payload string) (int64, error) {
	ctx := context.TODO()
	cnt, err := q.mq.cache.RPush(ctx, q.QName, payload)
	if err != nil {
		return 0, fmt.Errorf("queue publish error: %s: %w", q.QName, err)
	}
	if len(payload) < 20 {
		q.mq.log.Info(q.QName).Msgf("published data: %+v", payload)
	}
	return cnt, nil
}

// GetQueueLength ...
func (q *Queue) GetQueueLength() (int64, error) {
	ctx := context.TODO()
	return q.mq.cache.LLen(ctx, q.QName)
}

// GetDLQLength ...
func (q *Queue) GetDLQLength() (int64, error) {
	ctx := context.TODO()
	return q.mq.cache.LLen(ctx, q.getDLQName())
}

// SetWorkerGroup ...
func (q *Queue) SetWorkerGroup(wg WorkerGroup) {
	wg.SetQueue(q)
	q.pool.AddWorkerGroup(wg)
}

// GetWorker ...
func (q *Queue) GetWorker(id string) (*Worker, error) {
	return q.mq.getWorker(fmt.Sprintf("%s%s%s", q.QName, separator, id))
}

// Start ...
func (q *Queue) Start(count int64) chan interface{} {
	return q.pool.Start(count)
}

// Stop ...
func (q *Queue) Stop(ctx context.Context) {
	q.pool.Stop(ctx)
	<-q.closed
}

// Update ...
func (q *Queue) Update(count int64) {
	q.pool.Update(count)
}

//
// Internal
//

func (q *Queue) getDLQName() string {
	return fmt.Sprintf("%s%s%s", q.QName, separator, "dlq")
}

func (q *Queue) listenHeartbeat() {
	ctx := context.Background()
	for hb := range q.heartbeat {
		switch {
		case hb.Closed:
			worker, err := q.GetWorker(hb.ID)
			if err != nil {
				q.mq.log.Error(q.QName).Msgf("Failed to get worker %s: %+v", hb.ID, err)
				continue
			}
			workerName := worker.WorkerName()

			count, err := worker.GetWorkerLength()
			if err != nil {
				q.mq.log.Error(q.QName).Msgf("Failed to get worker length for %s: %+v", workerName, err)
				continue
			}

			if count > 0 {
				err = worker.ReturnAll()
				if err != nil {
					q.mq.log.Error(q.QName).Msgf("unable to remove tasks from %s: %+v", workerName, err)
					continue
				}
			}

			_, err = q.mq.cache.ZRem(ctx, workersList, workerName)
			if err != nil {
				q.mq.log.Error(q.QName).Msgf("queue close error: %+v", err)
			}
			q.mq.log.Info(q.QName).Msgf("closed and removed from <%s>", workersList)
		default:
			workerName := fmt.Sprintf("%s%s%s", q.QName, separator, hb.ID)
			item := redis.Z{Score: float64(time.Now().Unix()), Member: workerName}
			_, err := q.mq.cache.ZAdd(ctx, workersList, &item)
			if err != nil {
				q.mq.log.Error(q.QName).Msgf("queue close error: %+v", err)
			}
		}
	}
	q.closed <- struct{}{}
}
