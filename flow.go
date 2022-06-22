package asynq

import "sync"

type Flow struct {
	NodeHandler map[string]HandlerFunc
	Nodes       []string   `json:"nodes,omitempty"`
	Edges       [][]string `json:"edges,omitempty"`
	FirstNode   string     `json:"first_node"`
	LastNode    string     `json:"last_node"`
	Server      *Server
	mu          sync.Mutex
}

func NewFlow(redisServer string, concurrency int) *Flow {
	srv := NewServer(
		RedisClientOpt{Addr: redisServer},
		Config{
			Concurrency: concurrency,
		},
	)
	return &Flow{
		NodeHandler: make(map[string]HandlerFunc),
		Server:      srv,
		mu:          sync.Mutex{},
	}
}

func (flow *Flow) AddHandler(node string, handler HandlerFunc) {
	flow.mu.Lock()
	defer flow.mu.Unlock()
	flow.NodeHandler[node] = handler
	flow.Nodes = append(flow.Nodes, node)
}

func (flow *Flow) AddEdge(in, out string) {
	edge := []string{in, out}
	flow.Edges = append(flow.Edges, edge)
}

func (flow *Flow) SetupServer() error {
	mux := NewServeMux()
	for node, handler := range flow.NodeHandler {
		flow.Server.AddQueue(node, 1)
		err := mux.Handle(node, handler)
		if err != nil {
			return err
		}
	}
	flow.Server.AddHandler(mux)
	return nil
}

func (flow *Flow) Start() error {
	return flow.Server.Run()
}

func (flow *Flow) Shutdown() {
	flow.Server.Shutdown()
}
