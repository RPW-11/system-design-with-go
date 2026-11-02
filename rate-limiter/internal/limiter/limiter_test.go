package limiter

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/rpw-11/rate-limiter-in-go/internal/rules"
	"github.com/rpw-11/rate-limiter-in-go/internal/store"
)

type fakeStore struct {
	counts map[string]int64
	err    error
}

func (f *fakeStore) Incr(ctx context.Context, key string, window time.Duration) (int64, error) {
	if f.err != nil {
		return 0, f.err
	}
	f.counts[key]++
	return f.counts[key], nil
}

var _ store.Store = (*fakeStore)(nil)

var fixed = time.Date(2026, 10, 27, 12, 0, 40, 0, time.UTC)

var helloRule = rules.Rule{Method: "GET", Path: "/api/hello", Limit: 5, Unit: "minute"}

func newTestLimiter(now *time.Time) (*Limiter, *fakeStore) {
	fake := &fakeStore{counts: make(map[string]int64)}
	l := New(fake)
	l.now = func() time.Time { return *now }
	return l, fake
}

func mustAllow(t *testing.T, l *Limiter, clientID string, rule rules.Rule) Result {
	t.Helper()

	res, err := l.Allow(t.Context(), clientID, rule)
	if err != nil {
		t.Fatalf("Allow: %v", err)
	}
	return res
}

func TestAllow_UnderLimit(t *testing.T) {
	current := fixed
	l, _ := newTestLimiter(&current)

	for i := 1; i <= helloRule.Limit; i++ {
		res := mustAllow(t, l, "alice", helloRule)
		if !res.Allowed {
			t.Errorf("call %d: Allowed = false, want true", i)
		}
		if res.Limit != helloRule.Limit {
			t.Errorf("call %d: Limit = %d, want %d", i, res.Limit, helloRule.Limit)
		}
	}
}

func TestAllow_OverLimit(t *testing.T) {
	current := fixed
	l, _ := newTestLimiter(&current)

	for range helloRule.Limit {
		mustAllow(t, l, "alice", helloRule)
	}

	res := mustAllow(t, l, "alice", helloRule)
	if res.Allowed {
		t.Error("call 6: Allowed = true, want false")
	}
}

func TestAllow_RemainingCountsDown(t *testing.T) {
	current := fixed
	l, _ := newTestLimiter(&current)

	want := []int{4, 3, 2, 1, 0, 0, 0}
	for i, w := range want {
		res := mustAllow(t, l, "alice", helloRule)
		if res.Remaining != w {
			t.Errorf("call %d: Remaining = %d, want %d", i+1, res.Remaining, w)
		}
	}
}

func TestAllow_RetryAfterWhenBlocked(t *testing.T) {
	current := fixed
	l, _ := newTestLimiter(&current)

	for i := 1; i <= helloRule.Limit; i++ {
		res := mustAllow(t, l, "alice", helloRule)
		if res.RetryAfter != 0 {
			t.Errorf("call %d: RetryAfter = %v, want 0", i, res.RetryAfter)
		}
	}

	// 12:00:40 → the window ends at 12:01:00.
	res := mustAllow(t, l, "alice", helloRule)
	if want := 20 * time.Second; res.RetryAfter != want {
		t.Errorf("call 6: RetryAfter = %v, want %v", res.RetryAfter, want)
	}
}

func TestAllow_RetryAfterSubSecond(t *testing.T) {
	current := time.Date(2026, 10, 27, 12, 0, 40, 300*int(time.Millisecond), time.UTC)
	l, _ := newTestLimiter(&current)
	rule := rules.Rule{Method: "GET", Path: "/api/hello", Limit: 1, Unit: "second"}

	mustAllow(t, l, "alice", rule)

	// 12:00:40.300 → the window ends at 12:00:41.000.
	res := mustAllow(t, l, "alice", rule)
	if res.Allowed {
		t.Error("Allowed = true, want false")
	}
	if want := 700 * time.Millisecond; res.RetryAfter != want {
		t.Errorf("RetryAfter = %v, want %v", res.RetryAfter, want)
	}
}

func TestAllow_NewWindowResets(t *testing.T) {
	current := fixed
	l, _ := newTestLimiter(&current)

	for range helloRule.Limit {
		mustAllow(t, l, "alice", helloRule)
	}

	current = time.Date(2026, 10, 27, 12, 1, 5, 0, time.UTC)

	res := mustAllow(t, l, "alice", helloRule)
	if !res.Allowed {
		t.Error("Allowed = false, want true")
	}
	if res.Remaining != 4 {
		t.Errorf("Remaining = %d, want 4", res.Remaining)
	}
}

func TestAllow_WindowBoundary(t *testing.T) {
	current := time.Date(2026, 10, 27, 12, 0, 59, 999*int(time.Millisecond), time.UTC)
	l, fake := newTestLimiter(&current)

	mustAllow(t, l, "alice", helloRule)

	current = time.Date(2026, 10, 27, 12, 1, 0, 0, time.UTC)

	res := mustAllow(t, l, "alice", helloRule)
	if res.Remaining != 4 {
		t.Errorf("Remaining after boundary = %d, want 4", res.Remaining)
	}
	if len(fake.counts) != 2 {
		t.Errorf("got %d keys, want 2 (one per window): %v", len(fake.counts), fake.counts)
	}
}

func TestAllow_ClientsAreIndependent(t *testing.T) {
	current := fixed
	l, _ := newTestLimiter(&current)

	for range helloRule.Limit {
		mustAllow(t, l, "alice", helloRule)
	}

	res := mustAllow(t, l, "bob", helloRule)
	if !res.Allowed {
		t.Error("bob: Allowed = false, want true")
	}
	if res.Remaining != 4 {
		t.Errorf("bob: Remaining = %d, want 4", res.Remaining)
	}
}

func TestAllow_RulesAreIndependent(t *testing.T) {
	current := fixed
	l, _ := newTestLimiter(&current)
	messagesRule := rules.Rule{Method: "POST", Path: "/api/messages", Limit: 5, Unit: "minute"}

	for range helloRule.Limit {
		mustAllow(t, l, "alice", helloRule)
	}

	res := mustAllow(t, l, "alice", messagesRule)
	if !res.Allowed {
		t.Error("POST /api/messages: Allowed = false, want true")
	}
	if res.Remaining != 4 {
		t.Error("POST /api/messages: Remaining != 4")
	}
}

func TestAllow_StoreError(t *testing.T) {
	current := fixed
	l, fake := newTestLimiter(&current)
	fake.err = errors.New("redis down")

	_, err := l.Allow(t.Context(), "alice", helloRule)
	if err == nil {
		t.Fatal("err = nil, want an error")
	}
	if !errors.Is(err, fake.err) {
		t.Errorf("errors.Is(err, fake.err) = false; err = %v", err)
	}
}

func TestCreateKey(t *testing.T) {
	windowStart := time.Date(2026, 10, 27, 12, 0, 0, 0, time.UTC)

	got := createKey("alice", "GET", "/api/hello", windowStart)
	want := "ratelimit:alice:GET /api/hello:" + strconv.FormatInt(windowStart.Unix(), 10)
	if got != want {
		t.Errorf("createKey = %q, want %q", got, want)
	}
}
