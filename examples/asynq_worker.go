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
			// Specify how many concurrent workers to use
			Concurrency: 10,
			// Optionally specify multiple queues with different priority.
			Queues: map[string]int{
				"critical":      6,
				"default":       3,
				"low":           1,
				"email:deliver": 7,
			},
			// See the godoc for other configuration options
		},
	)

	// mux maps a type to a handler
	mux := asynq.NewServeMux()
	mux.Use(FlowMiddleware)
	mux.HandleFunc(tasks.TypeEmailDelivery, tasks.HandleEmailDeliveryTask)
	mux.Handle(tasks.TypeImageResize, tasks.NewImageProcessor())
	srv.AddHandler(mux)
	// ...register other handlers...

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
		fmt.Println(info)
		// task := asynq.NewTask(t.ResultWriter().NextQueue(), info.Result)

		// t.ResultWriter().Broker().Enqueue(context.Background(), )
		return nil
	})
}
