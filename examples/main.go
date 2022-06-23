package main

import (
	"asynq"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"
)

const redisAddrWorker = "127.0.0.1:6379"

type HandleFinalStatus struct{}

func (fn HandleFinalStatus) HandleComplete(ctx context.Context, task *asynq.Task) {}

func (fn HandleFinalStatus) HandleDone(ctx context.Context, task *asynq.Task) {}

func (fn HandleFinalStatus) HandleError(ctx context.Context, task *asynq.Task, err error) {}

func main() {
	cfg := asynq.Config{
		CompleteHandler: HandleFinalStatus{},
		DoneHandler:     HandleFinalStatus{},
		ErrorHandler:    HandleFinalStatus{},
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
	go func() {
		time.Sleep(5 * time.Second)
		flow.Shutdown()
		fmt.Println("Server is shutdown")
	}()
	if err := flow.Start(); err != nil {
		log.Fatalf("could not run server: %v", err)
	}
}
