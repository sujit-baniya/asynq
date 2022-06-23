package main

import (
	"asynq"
	"context"
	"encoding/json"
	"log"
	"time"
)

const redisAddrWorker = "127.0.0.1:6379"

func main() {
	client()
	worker()
	// fl()
}

func fl() {
	flow := asynq.NewFlow(redisAddrWorker, asynq.Config{Concurrency: 1}, false)
	flow.AddHandler("email:deliver", &EmailDelivery{Operation{Type: "process"}})
	flow.AddHandler("prepare:email", &PrepareEmail{Operation{Type: "process"}})
	flow.AddHandler("get:input", &GetData{Operation{Type: "input"}})
	flow.AddHandler("loop", &Loop{Operation{Type: "loop"}})
	flow.AddHandler("condition", &Condition{Operation{Type: "condition"}})
	flow.AddHandler("store:data", &StoreData{Operation{Type: "process"}})
	flow.AddBranch("condition", map[string]string{
		"pass": "email:deliver",
		"fail": "store:data",
	})
	flow.AddEdge("get:input", "loop")
	flow.AddLoop("loop", "prepare:email")
	flow.AddEdge("prepare:email", "condition")
	err := flow.SetupServer()
	if err != nil {
		panic(err)
	}
	go func() {
		data := []map[string]any{
			{
				"phone": "+123456789",
				"email": "abc.xyz@gmail.com",
			},
			{
				"phone": "+98765412",
				"email": "xyz.abc@gmail.com",
			},
		}
		bt, _ := json.Marshal(data)
		asynq.SendToFlow(redisAddrWorker, flow, bt)
	}()
	if err := flow.Start(); err != nil {
		log.Fatalf("could not run server: %v", err)
	}
}

func client() {
	c := asynq.NewClient(asynq.RedisClientOpt{Addr: redisAddrWorker})
	defer c.Close()

	task := asynq.NewTask("aggregation-tutorial", []byte(`Hello`))
	info, err := c.Enqueue(task, asynq.Queue("tutorial"), asynq.Group("example-group"))
	if err != nil {
		log.Fatalf("Failed to enqueue task: %v", err)
	}
	log.Printf("Successfully enqueued task: %s", info.ID)
}

func handleAggregatedTask(ctx context.Context, task *asynq.Task) asynq.Result {
	log.Print("Handler received aggregated task")
	log.Printf("aggregated messags: %s", task.Payload())
	return asynq.Result{}
}

func worker() {
	srv := asynq.NewServer(
		asynq.RedisClientOpt{Addr: redisAddrWorker},
		asynq.Config{
			Queues: map[string]int{"tutorial": 1},
			GroupAggregator: asynq.GroupAggregatorFunc(func(group string, tasks []*asynq.Task) *asynq.Task {
				for _, t := range tasks {
					return asynq.NewTask("aggregation-tutorial", t.Payload(), t.Options()...)
				}
				return nil
			}),
			GroupGracePeriod: 1 * time.Second,
			GroupMaxDelay:    10 * time.Second,
			GroupMaxSize:     1,
		},
	)

	mux := asynq.NewServeMux()
	mux.HandleFunc("aggregation-tutorial", handleAggregatedTask)
	srv.AddHandler(mux)
	if err := srv.Run(); err != nil {
		log.Fatalf("Failed to start the server: %v", err)
	}
}
