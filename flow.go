package asynq

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/rs/xid"
	"github.com/sujit-baniya/asynq/internal/base"
	"github.com/sujit-baniya/asynq/internal/rdb"
	"golang.org/x/sync/errgroup"
	"strings"
	"sync"
)

type Branch struct {
	Key              string            `json:"key"`
	ConditionalNodes map[string]string `json:"conditional_nodes"`
}

type Mode string

const (
	Sync  Mode = "sync"
	Async Mode = "async"
	Form  Mode = "form"
)

type Flow struct {
	ID                 string            `json:"id"`
	Name               string            `json:"name"`
	Slug               string            `json:"slug"`
	Mode               Mode              `json:"mode"`
	Error              error             `json:"error"`
	Nodes              []string          `json:"nodes,omitempty"`
	Edges              [][]string        `json:"edges,omitempty"`
	Loops              [][]string        `json:"loops,omitempty"`
	UserID             any               `json:"user_id"`
	Branches           []Branch          `json:"branches,omitempty"`
	LastNode           string            `json:"last_node"`
	FirstNode          string            `json:"first_node"`
	RedisAddress       string            `json:"redis_address"`
	EnableTaskGrouping bool              `json:"enable_task_grouping"`
	Status             string            `json:"status"`
	CronEntries        map[string]string `json:"cron_entries"`

	NodeHandler map[string]Handler
	RDB         *rdb.RDB
	Config      Config
	server      *Server
	handler     *ServeMux
	inspector   *Inspector
	scheduler   *Scheduler
	edges       map[string][]string
	loops       map[string][]string
	branches    map[string]map[string]string
	mu          sync.Mutex
}

type CronReportHandler struct {
	Key  string `json:"key"`
	Type string `json:"type"`
	flow *Flow
}

func (v *CronReportHandler) ProcessTask(ctx context.Context, task *Task) Result {
	return Result{}
}

func (v *CronReportHandler) GetType() string {
	return v.Type
}

func (v *CronReportHandler) GetKey() string {
	return v.Key
}

type HandleFinalStatus struct {
	rdb      *rdb.RDB
	flow     *Flow
	RedisUri string
	config   Config
}

func (fn *HandleFinalStatus) handle(payload []byte, flowID, operation, status string) {
	if fn.rdb == nil {
		fn.rdb = NewRDB(Config{RedisClientOpt: RedisClientOpt{Addr: fn.RedisUri}})
	}
	data := make(map[string]any)
	src := make(map[string]any)
	json.Unmarshal(payload, &data)
	if id, ok := data[fn.config.idKey]; ok {
		d, _ := fn.rdb.Client().Get(context.Background(), "o:f:"+flowID+":t:"+id.(string)).Bytes()
		if d != nil {
			json.Unmarshal(d, &src)
			data = MergeMap(src, data)
		}
		data[fn.config.operationKey] = operation
		data[fn.config.statusKey] = status
		dataToWrite, _ := json.Marshal(data)
		fn.rdb.Client().Set(context.Background(), "o:f:"+flowID+":t:"+id.(string), dataToWrite, 0)
		fn.rdb.Client().RPush(context.Background(), "o:f:"+flowID+":o:"+operation, dataToWrite)
	}
}

func (fn *HandleFinalStatus) HandleComplete(ctx context.Context, task *Task) {
	fn.handle(task.Payload(), task.FlowID, task.Type(), "completed")
}

func (fn *HandleFinalStatus) HandleDone(ctx context.Context, task *Task) {
	fn.handle(task.Payload(), task.FlowID, task.Type(), "completed")
}

func (fn *HandleFinalStatus) HandleError(ctx context.Context, task *Task, err error) {
	fn.handle(task.Payload(), task.FlowID, task.Type(), "failed")
}

func NewFlow(redisServer string, cfg Config) *Flow {
	cfg.RedisClientOpt = RedisClientOpt{Addr: redisServer}
	if cfg.Mode == "" {
		cfg.Mode = Async
	}
	if cfg.FlowPrefix == "" {
		cfg.FlowPrefix = "asynq"
	}
	cfg.idKey = cfg.FlowPrefix + "_id"
	cfg.operationKey = cfg.FlowPrefix + "_operation"
	cfg.statusKey = cfg.FlowPrefix + "_status"
	cfg.flowIDKey = cfg.FlowPrefix + "_flow_id"
	if cfg.Concurrency == 0 {
		cfg.Concurrency = 1
	}
	if cfg.FlowID == "" {
		cfg.FlowID = xid.New().String()
	}
	if cfg.ServerID == "" {
		cfg.ServerID = cfg.FlowID
	}
	cfg.RDB = NewRDB(cfg)
	flow := &Flow{
		ID:           cfg.FlowID,
		Mode:         cfg.Mode,
		UserID:       cfg.UserID,
		NodeHandler:  make(map[string]Handler),
		RedisAddress: redisServer,
		CronEntries:  make(map[string]string),
		RDB:          cfg.RDB,
		mu:           sync.Mutex{},
		edges:        make(map[string][]string),
		loops:        make(map[string][]string),
		branches:     make(map[string]map[string]string),
		inspector:    NewInspectorFromRDB(cfg.RDB),
		scheduler:    NewSchedulerFromRDB(cfg.RDB, nil),
	}
	if cfg.CompleteHandler == nil {
		cfg.CompleteHandler = &HandleFinalStatus{RedisUri: redisServer, flow: flow, config: cfg}
	}
	if cfg.DoneHandler == nil {
		cfg.DoneHandler = &HandleFinalStatus{RedisUri: redisServer, flow: flow, config: cfg}
	}
	if cfg.ErrorHandler == nil {
		cfg.ErrorHandler = &HandleFinalStatus{RedisUri: redisServer, flow: flow, config: cfg}
	}
	flow.Config = cfg
	if flow.Mode == Async {
		srv := NewServer(cfg)
		flow.server = srv
	}
	return flow
}

func (flow *Flow) QueueList() ([]*QueueInfo, error) {
	queueList, err := flow.inspector.Queues()
	if err != nil {
		return nil, err
	}
	var queues []*QueueInfo
	for _, queue := range queueList {
		if strings.Contains(queue, flow.ID) {
			info, err := flow.QueueInfo(queue)
			if err != nil {
				return nil, err
			}
			queues = append(queues, info)
		}
	}
	return queues, nil
}

func (flow *Flow) QueueHistory(queue string, noOfDays int) ([]*DailyStats, error) {
	if noOfDays == 0 {
		noOfDays = 7
	}
	return flow.inspector.History(queue, noOfDays)
}

func (flow *Flow) QueueInfo(queue string) (*QueueInfo, error) {
	return flow.inspector.GetQueueInfo(queue)
}

func (flow *Flow) Pause(queue string) error {
	return flow.inspector.PauseQueue(queue)
}

func (flow *Flow) Unpause(queue string) error {
	return flow.inspector.UnpauseQueue(queue)
}

func (flow *Flow) TaskListByStatus(queue string, status string) ([]*TaskInfo, error) {
	switch status {
	case "Active":
		return flow.inspector.ListActiveTasks(queue)
	case "Pending":
		return flow.inspector.ListPendingTasks(queue)
	case "Scheduled":
		return flow.inspector.ListScheduledTasks(queue)
	case "Archived":
		return flow.inspector.ListArchivedTasks(queue)
	case "Retry":
		return flow.inspector.ListRetryTasks(queue)
	default:
		return nil, nil
	}
}

func (flow *Flow) ActiveTaskList(queue string) ([]*TaskInfo, error) {
	return flow.inspector.ListActiveTasks(queue)
}

func (flow *Flow) PendingTaskList(queue string) ([]*TaskInfo, error) {
	return flow.inspector.ListPendingTasks(queue)
}

func (flow *Flow) ScheduledTaskList(queue string) ([]*TaskInfo, error) {
	return flow.inspector.ListScheduledTasks(queue)
}

func (flow *Flow) ArchivedTaskList(queue string) ([]*TaskInfo, error) {
	return flow.inspector.ListArchivedTasks(queue)
}

func (flow *Flow) RetryTaskList(queue string) ([]*TaskInfo, error) {
	return flow.inspector.ListRetryTasks(queue)
}

func (flow *Flow) Enqueue(ctx context.Context, queueName string, broker base.Broker, flowID string, payload []byte, result *Result) {
	task := NewTask(queueName, payload, FlowID(flowID), Queue(queueName))
	_, err := EnqueueContext(broker, ctx, task, FlowID(flowID), Queue(queueName))
	if err != nil {
		result.Error = err
	}
}

func (flow *Flow) loop(ctx context.Context, task *Task) ([]any, error) {
	g, ctx := errgroup.WithContext(ctx)
	result := make(chan interface{})
	var rs, results []interface{}
	err := json.Unmarshal(task.Payload(), &rs)
	if err != nil {
		return nil, err
	}
	for _, single := range rs {
		single := single
		g.Go(func() error {
			var payload []byte
			currentData := make(map[string]any)
			switch s := single.(type) {
			case map[string]any:
				id := xid.New().String()
				if _, ok := s[flow.Config.idKey]; !ok {
					s[flow.Config.idKey] = id
				}
				if _, ok := s[flow.Config.flowIDKey]; !ok {
					s[flow.Config.flowIDKey] = flow.ID
				}
				if _, ok := s[flow.Config.statusKey]; !ok {
					s[flow.Config.statusKey] = "pending"
				}
				currentData = s
				payload, err = json.Marshal(currentData)
				if err != nil {
					return err
				}
				break
			default:
				payload, err = json.Marshal(single)
				if err != nil {
					return err
				}
			}
			var responseData map[string]interface{}
			if f, ok := flow.loops[task.Type()]; ok {
				for _, v := range f {
					t := NewTask(v, payload, FlowID(task.FlowID), Queue(v))
					res := flow.ProcessTask(ctx, t, flow.NodeHandler[v])
					if res.Error != nil {
						flow.Config.ErrorHandler.HandleError(ctx, task, res.Error)
						return res.Error
					} else {
						flow.Config.CompleteHandler.HandleComplete(ctx, task)
					}
					err = json.Unmarshal(res.Data, &responseData)
					if err != nil {
						return err
					}
					currentData = MergeMap(currentData, responseData)
				}
				payload, err = json.Marshal(currentData)
				if err != nil {
					return err
				}
				err = json.Unmarshal(payload, &single)
				if err != nil {
					return err
				}
				select {
				case result <- single:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			return nil
		})
	}
	go func() {
		g.Wait()
		close(result)
	}()
	for ch := range result {
		results = append(results, ch)
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}
	return results, nil
}

func (flow *Flow) ProcessTask(ctx context.Context, task *Task, handler ...Handler) Result {
	var h Handler
	if len(handler) > 0 {
		h = handler[0]
	} else if flow.FirstNode != "" {
		h = flow.NodeHandler[flow.FirstNode]
	}
	if h == nil {
		return Result{Error: errors.New("Handler not found")}
	}
	result := h.ProcessTask(ctx, task)
	if result.Error != nil {
		flow.Config.ErrorHandler.HandleError(ctx, task, result.Error)
		return result
	} else {
		flow.Config.CompleteHandler.HandleComplete(ctx, task)
	}
	if h.GetType() == "loop" {
		newTask := NewTask(task.Type(), result.Data, FlowID(flow.ID))
		results, err := flow.loop(ctx, newTask)
		if err != nil {
			result.Error = err
			return result
		}
		tmp, err := json.Marshal(results)
		if err != nil {
			result.Error = err
			return result
		}
		result.Data = tmp
	}
	if h.GetType() == "condition" {
		if f, ok := flow.branches[task.Type()]; ok && result.Status != "" {
			if c, o := f[result.Status]; o {
				t := NewTask(c, result.Data, FlowID(task.FlowID))
				res := flow.ProcessTask(ctx, t, flow.NodeHandler[c])
				if res.Error != nil {
					flow.Config.ErrorHandler.HandleError(ctx, task, res.Error)
					return res
				} else {
					flow.Config.CompleteHandler.HandleComplete(ctx, task)
				}
				result = res
			}
		}
	}
	if f, ok := flow.edges[task.Type()]; ok {
		for _, v := range f {
			t := NewTask(v, result.Data, FlowID(task.FlowID))
			res := flow.ProcessTask(ctx, t, flow.NodeHandler[v])
			if res.Error != nil {
				flow.Config.ErrorHandler.HandleError(ctx, task, res.Error)
				return res
			} else {
				flow.Config.CompleteHandler.HandleComplete(ctx, task)
			}
			result = res
		}
	}
	return result
}

func (flow *Flow) edgeMiddleware(h Handler) Handler {
	return HandlerFunc(func(ctx context.Context, task *Task) Result {
		result := h.ProcessTask(ctx, task)
		if result.Error != nil {
			return result
		}
		if h.GetType() == "loop" {
			var rs []any
			err := json.Unmarshal(result.Data, &rs)
			if err != nil {
				result.Error = err
				return result
			}
			for _, single := range rs {
				single := single
				payload := result.Data
				currentData := make(map[string]any)
				switch s := single.(type) {
				case map[string]any:
					id := xid.New().String()
					if _, ok := s[flow.Config.idKey]; !ok {
						s[flow.Config.idKey] = id
					}
					if _, ok := s[flow.Config.flowIDKey]; !ok {
						s[flow.Config.flowIDKey] = task.FlowID
					}
					if _, ok := s[flow.Config.statusKey]; !ok {
						s[flow.Config.statusKey] = "pending"
					}
					currentData = s
					payload, err = json.Marshal(currentData)
					if err != nil {
						result.Error = err
						return result
					}
					err = task.ResultWriter().Broker().AddTask("o:f:"+task.FlowID+":t:"+id, payload)
					if err != nil {
						result.Error = err
						return result
					}
					break
				default:
					payload, err = json.Marshal(single)
					if err != nil {
						result.Error = err
						return result
					}
				}
				if f, ok := flow.loops[task.Type()]; ok {
					for _, v := range f {
						flow.Enqueue(ctx, v, task.ResultWriter().Broker(), task.FlowID, payload, &result)
						if result.Error != nil {
							return result
						}
					}
				}
			}
		}
		if h.GetType() == "condition" {
			if f, ok := flow.branches[task.Type()]; ok && result.Status != "" {
				if c, o := f[result.Status]; o {
					flow.Enqueue(ctx, c, task.ResultWriter().Broker(), task.FlowID, result.Data, &result)
					if result.Error != nil {
						return result
					}
				}
			}
		}
		if f, ok := flow.edges[task.Type()]; ok {
			for _, v := range f {
				flow.Enqueue(ctx, v, task.ResultWriter().Broker(), task.FlowID, result.Data, &result)
				if result.Error != nil {
					return result
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

func (flow *Flow) AddLoop(in string, out ...string) {
	loop := []string{in}
	loop = append(loop, out...)
	flow.Loops = append(flow.Loops, loop)
}

func (flow *Flow) Prepare() {
	for _, edge := range flow.Edges {
		flow.edges[edge[0]] = append(flow.edges[edge[0]], edge[1])
	}
	for _, loop := range flow.Loops {
		flow.loops[loop[0]] = append(flow.loops[loop[0]], loop[1:]...)
	}
	for _, branch := range flow.Branches {
		flow.branches[branch.Key] = branch.ConditionalNodes
	}
}

func (flow *Flow) SetupServer() error {
	if flow.Config.NoService {
		return nil
	}
	flow.Prepare()
	mux := NewServeMux()
	for node, handler := range flow.NodeHandler {
		if handler.GetType() == "input" {
			flow.FirstNode = node
		}
		if handler.GetType() == "output" {
			flow.LastNode = node
		}

		if flow.Mode == Async {
			flow.server.AddQueue(node, 1)
			flow.RDB.Client().SAdd(context.Background(), base.AllQueues, node)
			result := mux.Handle(node, handler)
			if result.Error != nil {
				return result.Error
			}
		}
	}

	key := "cron:1:" + flow.ID
	if flow.Config.CronReportHandler == nil {
		mux.Handle(key, &CronReportHandler{flow: flow})
	} else {
		mux.Handle(key, flow.Config.CronReportHandler)
	}
	if flow.Mode == Async {
		flow.server.AddQueue(key, 2)

		mux.Use(flow.edgeMiddleware)
		flow.handler = mux
		flow.server.AddHandler(mux)
		register, err := flow.scheduler.Register("@every 10s", NewTask(key, nil, FlowID(flow.ID), Queue(key)), Queue(key))
		if err != nil {
			return err
		}
		flow.mu.Lock()
		flow.CronEntries[register] = key
		flow.mu.Unlock()
	}
	return nil
}

func (flow *Flow) Use(handler func(h Handler) Handler) {
	if flow.handler != nil {
		flow.handler.Use(handler)
	}
}

func (flow *Flow) Run() error {
	if flow.server == nil {
		return nil
	}
	return flow.server.Run()
}

func (flow *Flow) Start() error {
	if flow.server == nil {
		return nil
	}
	if flow.scheduler.state.value != srvStateActive {
		flow.scheduler.Start()
	}

	if flow.server.state.value != srvStateActive {
		return flow.server.Start()
	}
	return nil
}

func (flow *Flow) GetStatus() string {
	if flow.server == nil {
		flow.Status = "new"
		return flow.Status
	}
	switch flow.server.state.value {
	case srvStateActive:
		flow.Status = "active"
		break
	case srvStateStopped:
		flow.Status = "stopped"
		break
	case srvStateClosed:
		flow.Status = "closed"
		break
	default:
		flow.Status = "new"
		break
	}
	return flow.Status
}

func (flow *Flow) Shutdown() {
	if flow.server == nil {
		return
	}
	flow.scheduler.Shutdown()
	flow.server.Shutdown()
}

func (flow *Flow) Send(data []byte) Result {
	if flow.Mode == Async {
		res, err := flow.SendAsync(data)
		if err != nil {
			return Result{Error: err}
		}
		result := Result{
			Status: res.State.String(),
			Data:   res.Payload,
		}
		return result
	}
	task := NewTask(flow.FirstNode, data, FlowID(flow.ID))
	return flow.ProcessTask(context.Background(), task, flow.NodeHandler[flow.FirstNode])
}

func (flow *Flow) SendAsync(data []byte) (*TaskInfo, error) {
	return SendAsync(flow.RedisAddress, flow.ID, flow.FirstNode, data)
}

func SendAsync(redisAddress, flowID string, queue string, data []byte) (*TaskInfo, error) {
	task := NewTask(queue, data, FlowID(flowID))
	client := NewClient(RedisClientOpt{Addr: redisAddress})
	defer client.Close()
	var ops []Option
	ops = append(ops, Queue(queue), FlowID(flowID))
	return client.Enqueue(task, ops...)
}

func MergeMap(map1 map[string]any, map2 map[string]any) map[string]any {
	for k, m := range map2 {
		if _, ok := map1[k]; !ok {
			map1[k] = m
		}
	}
	return map1
}
