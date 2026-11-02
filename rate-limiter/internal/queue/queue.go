package queue

import (
	"context"
	"time"
)

type Message struct {
	ClientID   string    `redis:"clientId"`
	Method     string    `redis:"method"`
	Path       string    `redis:"path"`
	Body       string    `redis:"body"`
	ReceivedAt time.Time `redis:"receivedAt"`
}

type Queue interface {
	Enqueue(ctx context.Context, msg Message) (id string, err error)
}
