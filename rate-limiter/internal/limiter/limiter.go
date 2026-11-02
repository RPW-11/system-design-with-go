package limiter

import (
	"context"
	"fmt"
	"time"

	"github.com/rpw-11/rate-limiter-in-go/internal/rules"
	"github.com/rpw-11/rate-limiter-in-go/internal/store"
)

type Limiter struct {
	store store.Store
	now   func() time.Time
}

type Result struct {
	Allowed    bool
	Limit      int
	Remaining  int
	RetryAfter time.Duration
}

func New(s store.Store) *Limiter {
	return &Limiter{
		store: s,
		now:   time.Now,
	}
}

func (l *Limiter) Allow(ctx context.Context, clientID string, rule rules.Rule) (Result, error) {
	window, err := rule.Window()
	if err != nil {
		return Result{}, fmt.Errorf("error reading rule window: %w", err)
	}

	now := l.now()
	windowStart := now.Truncate(window)

	key := createKey(clientID, rule.Method, rule.Path, windowStart)

	count, err := l.store.Incr(ctx, key, window)
	if err != nil {
		return Result{}, fmt.Errorf("failed performing incr: %w", err)
	}

	allowed := count <= int64(rule.Limit)

	retryAfter := time.Duration(0)
	if !allowed {
		retryAfter = windowStart.Add(window).Sub(now)
	}

	return Result{
		Allowed:    allowed,
		Limit:      rule.Limit,
		Remaining:  max(rule.Limit-int(count), 0),
		RetryAfter: retryAfter,
	}, nil

}

func createKey(clientID, method, path string, windowStart time.Time) string {
	return fmt.Sprintf("ratelimit:%s:%s %s:%d", clientID, method, path, windowStart.Unix())
}
