package asynq

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"sync"
)

type Branch struct {
	Key              string            `json:"key"`
	ConditionalNodes map[string]string `json:"conditional_nodes"`
}

type Flow struct {
	ID          string `json:"id"`
	NodeHandler map[string]Handler
	Nodes       []string   `json:"nodes,omitempty"`
	Edges       [][]string `json:"edges,omitempty"`
	Loops       [][]string `json:"loops,omitempty"`
	Branches    []Branch   `json:"branches,omitempty"`
	FirstNode   string     `json:"first_node"`
	LastNode    string     `json:"last_node"`
	Server      *Server
	edges       map[string][]string
	loops       map[string][]string
	branches    map[string]string
	mu          sync.Mutex
	handler     *ServeMux
}

func NewFlow(redisServer string, concurrency int) *Flow {
	srv := NewServer(
		RedisClientOpt{Addr: redisServer},
		Config{
			Concurrency: concurrency,
		},
	)
	return &Flow{
		ID:          uuid.New().String(),
		NodeHandler: make(map[string]Handler),
		Server:      srv,
		mu:          sync.Mutex{},
		edges:       make(map[string][]string),
		loops:       make(map[string][]string),
	}
}

func (flow *Flow) edgeMiddleware(h Handler) Handler {
	return HandlerFunc(func(ctx context.Context, task *Task) Result {
		result := h.ProcessTask(ctx, task)
		if result.Error != nil {
			return result
		}
		if h.GetType() == "loop" {
			var rs []interface{}
			err := json.Unmarshal(result.Data, &rs)
			if err != nil {
				result.Error = err
				return result
			}
			for _, single := range rs {
				single := single
				payload := result.Data
				currentData := make(map[string]interface{})
				switch s := single.(type) {
				case map[string]interface{}:
					currentData = s
				}
				if currentData != nil {
					payload, err = json.Marshal(currentData)
					if err != nil {
						result.Error = err
						return result
					}
				} else {
					payload, err = json.Marshal(single)
					if err != nil {
						result.Error = err
						return result
					}
				}
				if f, ok := flow.loops[task.Type()]; ok {
					for _, v := range f {
						t := NewTask(v, payload)
						_, err := EnqueueContext(task.ResultWriter().Broker(), ctx, t)
						if err != nil {
							result.Error = err
							return result
						}
					}
				}
			}
		} else if h.GetType() == "condition" {
			// @TODO - Add condition
			fmt.Println("Condition")
			fmt.Println(task)
		} else {
			if f, ok := flow.edges[task.Type()]; ok {
				for _, v := range f {
					t := NewTask(v, result.Data)
					_, err := EnqueueContext(task.ResultWriter().Broker(), ctx, t)
					if err != nil {
						result.Error = err
						return result
					}
				}
			}
		}
		return result
	})
}

func (flow *Flow) AddHandler(node string, handler Handler) {
	flow.mu.Lock()
	defer flow.mu.Unlock()
	flow.NodeHandler[node] = handler
	flow.Nodes = append(flow.Nodes, node)
}

func (flow *Flow) AddEdge(in, out string) {
	edge := []string{in, out}
	flow.Edges = append(flow.Edges, edge)
}

func (flow *Flow) AddBranch(vertex string, conditions map[string]string) {
	branch := Branch{
		Key:              vertex,
		ConditionalNodes: conditions,
	}
	flow.Branches = append(flow.Branches, branch)
}

func (flow *Flow) AddLoop(in, out string) {
	loop := []string{in, out}
	flow.Loops = append(flow.Loops, loop)
}

func (flow *Flow) SetupServer() error {
	flow.PrepareEdge()
	flow.PrepareLoop()
	mux := NewServeMux()
	for node, handler := range flow.NodeHandler {
		if handler.GetType() == "input" {
			flow.FirstNode = node
		}
		flow.Server.AddQueue(node, 1)
		result := mux.Handle(node, handler)
		if result.Error != nil {
			return result.Error
		}
	}
	mux.Use(flow.edgeMiddleware)
	flow.handler = mux
	flow.Server.AddHandler(mux)
	return nil
}

func (flow *Flow) PrepareEdge() {
	for _, edge := range flow.Edges {
		flow.edges[edge[0]] = append(flow.edges[edge[0]], edge[1])
	}
}

func (flow *Flow) PrepareLoop() {
	for _, loop := range flow.Loops {
		flow.loops[loop[0]] = append(flow.loops[loop[0]], loop[1])
	}
}

func (flow *Flow) Use(handler func(h Handler) Handler) {
	flow.handler.Use(handler)
}

func (flow *Flow) Start() error {
	return flow.Server.Run()
}

func (flow *Flow) Shutdown() {
	flow.Server.Shutdown()
}

func SendToFlow(redisAddress string, flow *Flow, data []byte) (*TaskInfo, error) {
	task := NewTask(flow.FirstNode, data)
	client := NewClient(RedisClientOpt{Addr: redisAddress})
	defer client.Close()
	return client.Enqueue(task, Queue(flow.FirstNode))
}
