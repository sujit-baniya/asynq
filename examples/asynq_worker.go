package main

import (
	"asynq"
	"asynq/examples/tasks"
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
	p := tasks.EmailDeliveryPayload{
		UserID:     1,
		TemplateID: "This is template",
	}
	d, _ := json.Marshal(p)
	fmt.Println("Preparing...")
	return asynq.Result{Data: d}
}

type Loop struct {
	Operation
}

func (e *Loop) ProcessTask(ctx context.Context, task *asynq.Task) asynq.Result {
	return asynq.Result{Data: task.Payload()}
}

type PrepareEmail struct {
	Operation
}

func (e *PrepareEmail) ProcessTask(ctx context.Context, task *asynq.Task) asynq.Result {
	p := tasks.EmailDeliveryPayload{
		UserID:     1,
		TemplateID: "This is template",
	}
	d, _ := json.Marshal(p)
	fmt.Println("Preparing...")
	return asynq.Result{Data: d}
}

type EmailDelivery struct {
	Operation
}

func (e *EmailDelivery) ProcessTask(ctx context.Context, task *asynq.Task) asynq.Result {
	var p tasks.EmailDeliveryPayload
	if err := json.Unmarshal(task.Payload(), &p); err != nil {
		return asynq.Result{
			Data:  nil,
			Error: fmt.Errorf("json.Unmarshal failed: %v: %w", err, asynq.SkipRetry),
		}
	}
	p.UserID = 123
	d, _ := json.Marshal(p)
	log.Printf("Sending Email to User: user_id=%d, template_id=%s", p.UserID, p.TemplateID)
	return asynq.Result{Data: d}
}

func main() {
	flow := asynq.NewFlow(redisAddrWorker, 10)
	flow.AddHandler("email:deliver", &EmailDelivery{Operation{Type: "process"}})
	flow.AddHandler("prepare:email", &PrepareEmail{Operation{Type: "process"}})
	flow.AddHandler("get:input", &GetData{Operation{Type: "input"}})
	flow.AddHandler("loop", &Loop{Operation{Type: "loop"}})
	flow.AddEdge("get:input", "loop")
	flow.AddEdge("prepare:email", "email:deliver")
	flow.AddLoop("loop", "prepare:email")
	err := flow.SetupServer()
	if err != nil {
		panic(err)
	}
	if err := flow.Start(); err != nil {
		log.Fatalf("could not run server: %v", err)
	}
}
