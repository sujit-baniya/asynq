package main

import (
	"asynq"
	"context"
	"encoding/json"
	"fmt"
)

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

type Loop struct {
	Operation
}

type Condition struct {
	Operation
}

func (e *Condition) ProcessTask(ctx context.Context, task *asynq.Task) asynq.Result {
	var data map[string]any
	json.Unmarshal(task.Payload(), &data)
	if data["email"].(string) == "abc.xyz@gmail.com" {
		fmt.Println("Checking...", data, "Pass...")
		return asynq.Result{Data: task.Payload(), Status: "pass"}
	}
	fmt.Println("Checking...", data, "Fail...")
	return asynq.Result{Data: task.Payload(), Status: "fail"}
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

type StoreData struct {
	Operation
}

func (e *StoreData) ProcessTask(ctx context.Context, task *asynq.Task) asynq.Result {
	var data map[string]any
	json.Unmarshal(task.Payload(), &data)
	fmt.Println("Storing Data...", data)
	return asynq.Result{Data: task.Payload()}
}
