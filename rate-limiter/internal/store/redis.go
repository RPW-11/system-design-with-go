package store

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

type RedisStore struct {
	client *redis.Client
}

func NewRedisStore(addr string) (*RedisStore, error) {
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

	return &RedisStore{
		client: client,
	}, nil
}

func (rs *RedisStore) Incr(ctx context.Context, key string, window time.Duration) (int64, error) {
	if window <= 0 {
		return 0, fmt.Errorf("window must be more than 0, got: %v", window)
	}

	// redis atomic transaction pipeline
	pipe := rs.client.TxPipeline()

	res := pipe.Incr(ctx, key)
	// NX set an expiry if the key does not have an expiry yet
	pipe.Do(ctx, "PEXPIRE", key, window.Milliseconds(), "NX")
	_, err := pipe.Exec(ctx)
	if err != nil {
		return 0, fmt.Errorf("redis incr %q: %w", key, err)
	}

	return res.Val(), nil
}

func (rs *RedisStore) Close() error {
	return rs.client.Close()
}

// Compile time check
var _ Store = (*RedisStore)(nil)
