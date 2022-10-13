package main

import (
	"encoding/json"
	"github.com/sujit-baniya/asynq"
	"log"
	"time"
)

const redisAddrWorker = "127.0.0.1:6379"

func main() {
	// send(asynq.Sync)
	send(asynq.Async)
}
func send(mode asynq.Mode) {
	cfg := asynq.Config{Mode: mode}
	flow := asynq.NewFlow(redisAddrWorker, cfg)
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
	flow.Send(bt)
	go func() {
		if err := flow.Run(); err != nil {
			log.Fatalf("could not run server: %v", err)
		}
	}()
	time.Sleep(10 * time.Second)
	flow.Shutdown()
}
