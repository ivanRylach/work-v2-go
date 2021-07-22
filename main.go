package main

import (
	"context"
	"fmt"
	"github.com/go-redis/redis/v8"
	"github.com/taylorchu/work"
	"github.com/taylorchu/work/middleware/unique"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"
)

const NAMESPACE = "scheduler"
const QUEUE = "sample-queue"

func main() {

	redisClient, queue, worker := initialize()

	registerHandler(worker)

	go worker.Start()

	createJobs(redisClient, queue)

	quit := make(chan os.Signal)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	_, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	worker.Stop()

	fmt.Printf("Shutdown gracefully")
}

func initialize() (redis.UniversalClient, work.Queue, *work.Worker) {
	redisOpt := redis.UniversalOptions{
		Addrs: []string{"localhost:6379"},
	}
	redisClient := redis.NewUniversalClient(&redisOpt)
	queue := work.NewRedisQueue(redisClient)
	opt := work.WorkerOptions{
		Namespace: NAMESPACE,
		Queue:     queue,
		ErrorFunc: nil,
	}
	worker := work.NewWorker(&opt)
	return redisClient, queue, worker
}

func registerHandler(worker *work.Worker) {
	jobOpt := work.JobOptions{
		MaxExecutionTime: 10,
		IdleWait:         10,
		NumGoroutines:    2,
	}
	err := worker.Register(QUEUE, handler, &jobOpt)
	if err != nil {
		panic(err.Error())
	}
}

func createJobs(redisClient redis.UniversalClient, queue work.Queue) {
	// Enqueue unique jobs only
	enqrOpt := unique.EnqueuerOptions{
		Client:     redisClient,
		UniqueFunc: uniqueJob,
	}
	enqueuer := unique.Enqueuer(&enqrOpt)

	//enqFunc := enqueuer(queue.Enqueue)
	enqFunc := enqueuer(func(job *work.Job, opt *work.EnqueueOptions) error {
		err := queue.Enqueue(job, opt)
		fmt.Printf("Enqueued: %v\n", string(job.Payload))
		return err
	})

	// Create jobs
	for i := 0; i < 100_000; i++ {
		payload := fmt.Sprintf("payload:%v", i)

		job := work.Job{
			ID:      strconv.Itoa(i),
			Payload: []byte(payload),
		}
		enqOpt := work.EnqueueOptions{
			Namespace: NAMESPACE,
			QueueID:   QUEUE,
		}

		err := enqFunc(&job, &enqOpt)
		//fmt.Printf("Submitted: %v\n", i+1)

		if err != nil {
			panic(err.Error())
		}
	}
}

func handler(job *work.Job, opt *work.DequeueOptions) error {
	fmt.Printf("Processed: %v\n", string(job.Payload))
	return nil
}

func uniqueJob(job *work.Job, opt *work.EnqueueOptions) ([]byte, time.Duration, error) {
	return []byte(job.ID), time.Minute, nil
}
