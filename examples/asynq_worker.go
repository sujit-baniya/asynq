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
	flow := asynq.NewFlow(redisAddrWorker, 10)
	flow.AddHandler(tasks.TypeEmailDelivery, tasks.HandleEmailDeliveryTask)
	err := flow.SetupServer()
	if err != nil {
		panic(err)
	}
	if err := flow.Start(); err != nil {
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
