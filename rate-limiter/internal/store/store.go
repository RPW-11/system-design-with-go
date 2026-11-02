package store

import (
	"context"
	"time"
)

type Store interface {
	Incr(ctx context.Context, key string, window time.Duration) (int64, error)
}
