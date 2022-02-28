package sidejob

import (
	"context"

	"github.com/betas-in/pool"
)

// WorkerGroup ...
type WorkerGroup interface {
	SetQueue(*Queue)
	Process(ctx context.Context, workerCtx *pool.WorkerContext, id string)
}
