package main

import (
	"asynq"
	"context"
	"encoding/json"
	"fmt"
	"log"
)

const redisAddrWorker = "127.0.0.1:6379"

type Operation struct {
	Type string `json:"type"`
	Key  string `json:"key"`
}

func (e *Operation) ProcessTask(ctx context.Context, task *asynq.Task) asynq.Result {
	return asynq.Result{Data: task.Payload()}
}

func (e *Operation) GetType() string {
	return e.Type
}

func (e *Operation) GetKey() string {
	return e.Key
}

type GetData struct {
	Operation
}

func (e *GetData) ProcessTask(ctx context.Context, task *asynq.Task) asynq.Result {
	var data []map[string]any
	err := json.Unmarshal(task.Payload(), &data)
	if err != nil {
		panic(err)
	}
	fmt.Println("Getting Data...", data)
	fmt.Println(task.Options())
	return asynq.Result{Data: task.Payload()}
}

type Loop struct {
	Operation
}

type Condition struct {
	Operation
}

func (e *Condition) ProcessTask(ctx context.Context, task *asynq.Task) asynq.Result {
	var data map[string]any
	json.Unmarshal(task.Payload(), &data)
	fmt.Println("Checking...", data)
	return asynq.Result{Data: task.Payload(), Status: "pass"}
}

type PrepareEmail struct {
	Operation
}

func (e *PrepareEmail) ProcessTask(ctx context.Context, task *asynq.Task) asynq.Result {
	var data map[string]any
	json.Unmarshal(task.Payload(), &data)
	fmt.Println("Preparing...", data)
	return asynq.Result{Data: task.Payload()}
}

type EmailDelivery struct {
	Operation
}

func (e *EmailDelivery) ProcessTask(ctx context.Context, task *asynq.Task) asynq.Result {
	var data map[string]any
	json.Unmarshal(task.Payload(), &data)
	fmt.Println("Sending Email...", data)
	return asynq.Result{Data: task.Payload()}
}

func main() {
	flow := asynq.NewFlow(redisAddrWorker, 10)
	flow.AddHandler("email:deliver", &EmailDelivery{Operation{Type: "process"}})
	flow.AddHandler("prepare:email", &PrepareEmail{Operation{Type: "process"}})
	flow.AddHandler("get:input", &GetData{Operation{Type: "input"}})
	flow.AddHandler("loop", &Loop{Operation{Type: "loop"}})
	flow.AddHandler("condition", &Condition{Operation{Type: "condition"}})
	flow.AddBranch("condition", map[string]string{
		"pass": "email:deliver",
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
