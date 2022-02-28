package sidejob

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/betas-in/logger"
	"github.com/betas-in/rediscache"
	"github.com/betas-in/utils"
)

var (
	QUEUE = "test-sidejob"
)

func TestSidejob(t *testing.T) {
	log := logger.NewLogger(1, true)

	redisConf := rediscache.Config{
		Host:     "127.0.0.1",
		Port:     9876,
		Password: "596a96cc7bf9108cd896f33c44aedc8a",
	}

	cache, err := rediscache.NewCache(&redisConf, log)
	utils.Test().Nil(t, err)

	heartbeat, _ := time.ParseDuration("15s")
	poll, _ := time.ParseDuration("100ms")

	mq, err := NewSidejob(log, cache, heartbeat)
	utils.Test().Nil(t, err)

	published := 5
	workers := 2

	// Publish
	publisher, err := mq.GetPublisher(QUEUE)
	utils.Test().Nil(t, err)

	for i := 0; i < published; i++ {
		_, err = publisher.Publish(fmt.Sprintf("%d", i))
		utils.Test().Nil(t, err)
	}

	expectedFailures := ((published - (published % 23)) / 23) + 1

	// Consumer
	wg := NewWorkerGroup(log, heartbeat, poll)
	consumer, err := mq.GetConsumerWithWorker(QUEUE, wg)
	utils.Test().Nil(t, err)
	processed := consumer.Start(int64(workers))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	count := 0
	for range processed {
		count++
		if count == published-expectedFailures {
			utils.Test().Equals(t, published-expectedFailures, count)
			consumer.Stop(ctx)
			return
		}
	}
}
