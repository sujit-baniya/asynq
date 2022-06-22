package tasks

import (
	"asynq"
	"context"
	"encoding/json"
	"fmt"
	"log"
)

// A list of task types.
const (
	TypeEmailDelivery = "email:deliver"
	TypeImageResize   = "image:resize"
)

type EmailDeliveryPayload struct {
	UserID     int
	TemplateID string
}

type ImageResizePayload struct {
	SourceURL string
}

func NewEmailDeliveryTask(userID int, tmplID string) (*asynq.Task, error) {
	payload, err := json.Marshal(EmailDeliveryPayload{UserID: userID, TemplateID: tmplID})
	if err != nil {
		return nil, err
	}
	return asynq.NewTask(TypeEmailDelivery, payload, asynq.Queue(TypeEmailDelivery)), nil
}

func HandleEmailDeliveryTask(ctx context.Context, t *asynq.Task) asynq.Result {
	var p EmailDeliveryPayload
	if err := json.Unmarshal(t.Payload(), &p); err != nil {
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
