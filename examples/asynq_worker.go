package main

import (
	"asynq"
	"asynq/examples/tasks"
	"context"
	"fmt"
	"log"
)

const redisAddrWorker = "127.0.0.1:6379"

func main() {
	srv := asynq.NewServer(
		asynq.RedisClientOpt{Addr: redisAddrWorker},
		asynq.Config{
			Concurrency: 10,
			Queues: map[string]int{
				"critical":      6,
				"default":       3,
				"low":           1,
				"email:deliver": 7,
			},
		},
	)

	// mux maps a type to a handler
	mux := asynq.NewServeMux()
	mux.Use(FlowMiddleware)
	mux.HandleFunc(tasks.TypeEmailDelivery, tasks.HandleEmailDeliveryTask)
	srv.AddHandler(mux)

	if err := srv.Run(); err != nil {
		log.Fatalf("could not run server: %v", err)
	}
}

func FlowMiddleware(h asynq.Handler) asynq.Handler {
	return asynq.HandlerFunc(func(ctx context.Context, t *asynq.Task) error {
		err := h.ProcessTask(ctx, t)
		if err != nil {
			return err
		}
		info, err := t.ResultWriter().Broker().GetTaskInfo(t.Type(), t.ResultWriter().TaskID())
		if err != nil {
			return err
		}
		fmt.Println(info.State)
		// task := asynq.NewTask(t.ResultWriter().NextQueue(), info.Result)

		// t.ResultWriter().Broker().Enqueue(context.Background(), )
		return nil
	})
}
