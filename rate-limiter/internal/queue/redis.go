package queue

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

type RedisQueue struct {
	StreamName string
	MaxLength  int
	client     *redis.Client
}

func NewRedisQueue(addr, streamName string, maxLength int) (*RedisQueue, error) {
	client := redis.NewClient(&redis.Options{
		Addr: addr,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := client.Ping(ctx).Result()
	if err != nil {
		client.Close()
		return nil, fmt.Errorf("connecting to redis at %s: %w", addr, err)
	}

	return &RedisQueue{
		StreamName: streamName,
		MaxLength:  maxLength,
		client:     client,
	}, nil
}

var ErrQueueFull = errors.New("queue is full")

func (rq *RedisQueue) Enqueue(ctx context.Context, msg Message) (string, error) {
	count := rq.client.XLen(ctx, rq.StreamName)
	countVal, err := count.Result()
	if err != nil {
		return "", fmt.Errorf("failed running xlen: %w", err)
	}

	if countVal >= int64(rq.MaxLength) {
		return "", ErrQueueFull
	}

	res := rq.client.XAdd(ctx, &redis.XAddArgs{Stream: rq.StreamName, Values: msg})
	id, err := res.Result()
	if err != nil {
		return "", err
	}

	return id, nil
}

func (rq *RedisQueue) Close() error {
	return rq.client.Close()
}

var _ Queue = (*RedisQueue)(nil)
