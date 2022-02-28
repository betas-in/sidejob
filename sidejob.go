package sidejob

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/betas-in/logger"
	"github.com/betas-in/rediscache"
	"github.com/go-redis/redis/v8"
)

var (
	queuesList  = "sidejob.queues"
	workersList = "sidejob.workers"
	separator   = "."
)

// Sidejob ...
type Sidejob interface {
	GetPublisher(string) (*Queue, error)
	GetPublisherWithWorker(string, WorkerGroup) (*Queue, error)
	GetConsumer(string) (*Queue, error)
	GetConsumerWithWorker(string, WorkerGroup) (*Queue, error)
	GetCache() rediscache.Cache
}

type sidejob struct {
	log       *logger.Logger
	cache     rediscache.Cache
	queues    map[string]*Queue
	workers   map[string]*Worker
	lock      *sync.RWMutex
	heartbeat time.Duration
}

// #TODO support local sidejob without redis

// NewSidejob ...
func NewSidejob(log *logger.Logger, c rediscache.Cache, heartbeat time.Duration) (Sidejob, error) {
	log.Info("sidejob").Msgf("initializing")
	mq := sidejob{
		log:       log,
		cache:     c,
		queues:    map[string]*Queue{},
		workers:   map[string]*Worker{},
		lock:      &sync.RWMutex{},
		heartbeat: heartbeat,
	}

	ctx := context.TODO()
	_, err := c.Ping(ctx)
	if err != nil {
		log.Error("sidejob").Msgf("Could not ping redis %+v", err)
		return nil, err
	}
	err = mq.sync()
	if err != nil {
		log.Fatal("sidejob").Msgf("Sync failed with %+v", err)
	}
	err = mq.handleUnunsedWorker()
	if err != nil {
		log.Fatal("sidejob").Msgf("Handle unused workers failed with %+v", err)
	}
	return &mq, nil
}

func (mq *sidejob) GetPublisher(name string) (*Queue, error) {
	return mq.getQueue(name)
}

func (mq *sidejob) GetPublisherWithWorker(name string, wg WorkerGroup) (*Queue, error) {
	q, err := mq.getQueue(name)
	if err != nil {
		return nil, err
	}
	q.SetWorkerGroup(wg)
	return q, nil
}

func (mq *sidejob) GetConsumer(name string) (*Queue, error) {
	return mq.getQueue(name)
}

func (mq *sidejob) GetConsumerWithWorker(name string, wg WorkerGroup) (*Queue, error) {
	q, err := mq.getQueue(name)
	if err != nil {
		return nil, err
	}
	q.SetWorkerGroup(wg)
	return q, nil
}

func (mq *sidejob) GetCache() rediscache.Cache {
	return mq.cache
}

//
// Internal functions
//

func (mq *sidejob) sync() error {
	// Get list of queues
	mq.log.Info("sidejob").Msgf("syncing data with store")
	ctx := context.TODO()
	queues, err := mq.cache.SMembers(ctx, queuesList)
	if err != nil {
		return fmt.Errorf("queue list error: %w", err)
	}

	for _, qName := range queues {
		mq.addQueueLocally(qName)
	}

	// Get list of workers
	workers, err := mq.cache.ZRange(ctx, workersList, 0, -1)
	if err != nil {
		return fmt.Errorf("worker list error: %w", err)
	}
	for _, wName := range workers {
		mq.addWorkerLocally(wName)
	}

	return nil
}

func (mq *sidejob) handleUnunsedWorker() error {
	ctx := context.TODO()
	// Get list of workers, that have not had heartbeat in an hour
	// #TODO remove hard coding
	delta := 5 * mq.heartbeat
	deltaAgo := fmt.Sprintf("%d", time.Now().Add(delta*-1).Unix())
	workers, err := mq.cache.ZRangeByScore(ctx, workersList, "0", deltaAgo, 0, -1)
	if err != nil {
		return fmt.Errorf("unused worker list error: %w", err)
	}

	for _, worker := range workers {
		success, err := mq.removeWorker(worker)
		if err != nil {
			return err
		}
		if success {
			_, err := mq.cache.ZRem(ctx, workersList, worker)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (mq *sidejob) getQueue(name string) (*Queue, error) {
	q := mq.addQueueLocally(name)
	err := mq.addQueueOnRedis(name)
	return q, err
}

func (mq *sidejob) getWorker(name string) (*Worker, error) {
	w := mq.addWorkerLocally(name)
	err := mq.addWorkerOnRedis(name)
	return w, err
}

func (mq *sidejob) removeWorker(name string) (bool, error) {
	worker, err := mq.getWorker(name)
	if err != nil {
		return false, err
	}
	err = worker.ReturnAll()
	if err != nil {
		return false, err
	}
	length, err := worker.GetWorkerLength()
	if err != nil {
		return false, err
	}
	if length > 0 {
		return false, fmt.Errorf("queue length is still greater than 0 for %s", name)
	}
	return true, nil
}

func (mq *sidejob) addQueueLocally(name string) *Queue {
	queue, ok := mq.queues[name]
	if !ok {
		queue = NewQueue(name, mq)
		mq.queues[name] = queue
	}
	return queue
}

func (mq *sidejob) addWorkerLocally(name string) *Worker {
	mq.lock.Lock()
	defer mq.lock.Unlock()
	worker, ok := mq.workers[name]
	if !ok {
		nameSplits := strings.Split(name, separator)
		id := nameSplits[len(nameSplits)-1]
		queueName := strings.Join(nameSplits[0:len(nameSplits)-1], separator)

		queue := mq.addQueueLocally(queueName)

		worker = NewWorker(id, queue, mq)
		mq.workers[name] = worker
	}
	return worker
}

func (mq *sidejob) addQueueOnRedis(name string) error {
	ctx := context.TODO()
	_, err := mq.cache.SAdd(ctx, queuesList, name)
	if err != nil {
		return fmt.Errorf("add queue error: %w", err)
	}
	return nil
}

func (mq *sidejob) addWorkerOnRedis(name string) error {
	ctx := context.TODO()
	z := redis.Z{Score: float64(time.Now().Unix()), Member: name}
	_, err := mq.cache.ZAdd(ctx, workersList, &z)
	if err != nil {
		return fmt.Errorf("add worker error: %w", err)
	}
	return nil
}
