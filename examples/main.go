package main

import (
	"asynq"
	"encoding/json"
	"log"
)

const redisAddrWorker = "127.0.0.1:6379"

func main() {
	flow := asynq.NewFlow(redisAddrWorker, 10)
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
