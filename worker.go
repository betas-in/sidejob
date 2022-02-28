package sidejob

import (
	"context"
	"fmt"
)

// Worker ...
type Worker struct {
	ID    string
	Queue *Queue
	mq    *sidejob
}

// NewWorker ...
func NewWorker(id string, q *Queue, mq *sidejob) *Worker {
	return &Worker{ID: id, Queue: q, mq: mq}
}

// Consume ...
func (w *Worker) Consume() (string, error) {
	ctx := context.TODO()
	// check if there is anything in the worker queue
	results, err := w.mq.cache.LRange(ctx, w.WorkerName(), 0, 0)
	if err != nil {
		return "", fmt.Errorf("%s: consume worker queue: %w", w.WorkerName(), err)
	}
	if len(results) > 0 {
		return results[0], nil
	}

	// if not, pull from the main queue
	item, err := w.Move(w.Queue.QName, w.WorkerName())
	if err != nil {
		return "", fmt.Errorf("%s: worker: consume main queue: %w", w.WorkerName(), err)
	}
	if item != "" && len(item) < 20 {
		w.mq.log.Info(w.WorkerName()).Msgf("consumed data from <%s>: %+v", w.Queue.QName, item)
	}
	return item, nil
}

// ReturnAll ...
func (w *Worker) ReturnAll() error {
	ctx := context.TODO()
	// check if there is anything in the worker queue
	results, err := w.mq.cache.LRange(ctx, w.WorkerName(), 0, -1)
	if err != nil {
		return fmt.Errorf("%s: return worker queue: %w", w.WorkerName(), err)
	}
	if len(results) == 0 {
		return nil
	}

	// if yes, move it to the main queue
	moveCount := 0
	for range results {
		_, err := w.Move(w.WorkerName(), w.Queue.QName)
		if err != nil {
			return fmt.Errorf("%s: return main queue: %w", w.WorkerName(), err)
		}
		moveCount++
	}
	if moveCount > 0 {
		w.mq.log.Info(w.WorkerName()).Msgf("moved <%d> job to <%s>", moveCount, w.Queue.QName)
	}

	return nil
}

// Ack ...
func (w *Worker) Ack() error {
	ctx := context.TODO()
	item, err := w.mq.cache.LPop(ctx, w.WorkerName())
	if err != nil {
		return fmt.Errorf("%s: worker: ack error: %w", w.WorkerName(), err)
	}
	if len(item) < 20 {
		w.mq.log.Info(w.WorkerName()).Msgf("acked: %s", item)
	}
	return nil
}

// Reject ...
func (w *Worker) Reject(payload string) error {
	if payload == "" {
		return nil
	}

	item, err := w.Move(w.WorkerName(), w.Queue.getDLQName())
	if err != nil {
		return fmt.Errorf("%s: error in moving to dlq: %w", w.WorkerName(), err)
	}
	if len(item) < 20 {
		w.mq.log.Error(w.WorkerName()).Msgf("reject: moved to <%s>: %s", w.Queue.getDLQName(), item)
	}
	return nil
}

// GetWorkerLength ...
func (w *Worker) GetWorkerLength() (int64, error) {
	ctx := context.TODO()
	return w.mq.cache.LLen(ctx, w.WorkerName())
}

// WorkerName ...
func (w *Worker) WorkerName() string {
	return fmt.Sprintf("%s%s%s", w.Queue.QName, separator, w.ID)
}

// Move ...
func (w *Worker) Move(from, to string) (string, error) {
	ctx := context.TODO()
	return w.mq.cache.LMove(ctx, from, to, "LEFT", "RIGHT")
}

// UpdateTop ...
func (w *Worker) UpdateTop(queueName, payload string) (string, error) {
	ctx := context.TODO()
	return w.mq.cache.LSet(ctx, queueName, 0, payload)
}
