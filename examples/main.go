package main

import (
	"asynq"
	"context"
	"encoding/json"
	"fmt"
	"log"
)

const redisAddrWorker = "127.0.0.1:6379"

// The CompleteHandlerFunc type is an adapter to allow the use of  ordinary functions as a CompleteHandler.
// If f is a function with the appropriate signature, CompleteHandlerFunc(f) is a CompleteHandler that calls f.
type CompleteHandlerFunc struct{}

// HandleComplete calls fn(ctx, task, err)
func (fn CompleteHandlerFunc) HandleComplete(ctx context.Context, task *asynq.Task) {
	fmt.Println("Complete...", string(task.Payload()), "task", task.FlowID)
}

func main() {
	cfg := asynq.Config{
		CompleteHandler: &CompleteHandlerFunc{},
	}
	flow := asynq.NewFlow(redisAddrWorker, cfg, false)
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
