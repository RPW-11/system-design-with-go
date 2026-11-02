package middleware

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/rpw-11/rate-limiter-in-go/internal/limiter"
	"github.com/rpw-11/rate-limiter-in-go/internal/queue"
	"github.com/rpw-11/rate-limiter-in-go/internal/rules"
)

type fakeRules struct {
	rules map[string]rules.Rule
}

func (f *fakeRules) Get(method, path string) (rules.Rule, bool) {
	rule, ok := f.rules[method+" "+path]
	return rule, ok
}

type fakeAllower struct {
	res    limiter.Result
	err    error
	called bool

	gotClientID string
	gotRule     rules.Rule
}

func (f *fakeAllower) Allow(ctx context.Context, clientID string, rule rules.Rule) (limiter.Result, error) {
	f.called = true
	f.gotClientID = clientID
	f.gotRule = rule
	return f.res, f.err
}

// slowFakeAllower simulates a store that hangs until the caller's deadline.
type slowFakeAllower struct {
	gotErr error
}

func (f *slowFakeAllower) Allow(ctx context.Context, clientID string, rule rules.Rule) (limiter.Result, error) {
	select {
	case <-ctx.Done():
		f.gotErr = ctx.Err()
		return limiter.Result{}, f.gotErr
	case <-time.After(2 * time.Second):
		// Only reached if the middleware set no timeout. Blocked, so the
		// middleware returns 429 and the test fails instead of hanging.
		return limiter.Result{Allowed: false}, nil
	}
}

type fakeEnqueuer struct {
	id     string
	err    error
	called bool

	gotMsg queue.Message
}

func (f *fakeEnqueuer) Enqueue(ctx context.Context, msg queue.Message) (string, error) {
	f.called = true
	f.gotMsg = msg
	return f.id, f.err
}

var (
	_ RuleGetter = (*fakeRules)(nil)
	_ Allower    = (*fakeAllower)(nil)
	_ Allower    = (*slowFakeAllower)(nil)
	_ Enqueuer   = (*fakeEnqueuer)(nil)
)

var (
	helloRule    = rules.Rule{Method: "GET", Path: "/api/hello", Limit: 5, Unit: "minute"}
	messagesRule = rules.Rule{Method: "POST", Path: "/api/messages", Limit: 10, Unit: "second", OnLimit: "queue"}
)

func newFakeRules() *fakeRules {
	return &fakeRules{rules: map[string]rules.Rule{
		"GET /api/hello":     helloRule,
		"POST /api/messages": messagesRule,
	}}
}

// blockedAllower always reports the client as over the limit.
func blockedAllower() *fakeAllower {
	return &fakeAllower{res: limiter.Result{Allowed: false, Limit: 10, Remaining: 0, RetryAfter: 500 * time.Millisecond}}
}

// serve runs one request through the middleware and reports whether next was called.
func serve(t *testing.T, rg RuleGetter, a Allower, eq Enqueuer, req *http.Request) (*httptest.ResponseRecorder, bool) {
	t.Helper()

	nextCalled := false
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		nextCalled = true
		w.WriteHeader(http.StatusOK)
	})

	rec := httptest.NewRecorder()
	RateLimiting(rg, a, eq)(next).ServeHTTP(rec, req)
	return rec, nextCalled
}

func TestRateLimiting_NoRule(t *testing.T) {
	allower := &fakeAllower{}
	req := httptest.NewRequest(http.MethodGet, "/api/unlimited", nil)

	rec, nextCalled := serve(t, newFakeRules(), allower, nil, req)

	if !nextCalled {
		t.Error("next was not called, want called")
	}
	if allower.called {
		t.Error("Allow was called, want not called when no rule matches")
	}
	if rec.Code != http.StatusOK {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	if got := rec.Header().Get("X-Ratelimit-Limit"); got != "" {
		t.Errorf("X-Ratelimit-Limit = %q, want no header", got)
	}
}

func TestRateLimiting_Allowed(t *testing.T) {
	allower := &fakeAllower{res: limiter.Result{Allowed: true, Limit: 5, Remaining: 3}}
	req := httptest.NewRequest(http.MethodGet, "/api/hello", nil)

	rec, nextCalled := serve(t, newFakeRules(), allower, nil, req)

	if !nextCalled {
		t.Error("next was not called, want called")
	}
	if rec.Code != http.StatusOK {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	if got := rec.Header().Get("X-Ratelimit-Limit"); got != "5" {
		t.Errorf("X-Ratelimit-Limit = %q, want %q", got, "5")
	}
	if got := rec.Header().Get("X-Ratelimit-Remaining"); got != "3" {
		t.Errorf("X-Ratelimit-Remaining = %q, want %q", got, "3")
	}
	if got := rec.Header().Get("Retry-After"); got != "" {
		t.Errorf("Retry-After = %q, want no header on allowed request", got)
	}
}

func TestRateLimiting_Blocked(t *testing.T) {
	allower := &fakeAllower{res: limiter.Result{Allowed: false, Limit: 5, Remaining: 0, RetryAfter: 20 * time.Second}}
	req := httptest.NewRequest(http.MethodGet, "/api/hello", nil)

	rec, nextCalled := serve(t, newFakeRules(), allower, nil, req)

	if nextCalled {
		t.Error("next was called, want not called when blocked")
	}
	if rec.Code != http.StatusTooManyRequests {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusTooManyRequests)
	}

	wantHeaders := map[string]string{
		"X-Ratelimit-Limit":       "5",
		"X-Ratelimit-Remaining":   "0",
		"X-Ratelimit-Retry-After": "20",
		"Retry-After":             "20",
	}
	for name, want := range wantHeaders {
		if got := rec.Header().Get(name); got != want {
			t.Errorf("%s = %q, want %q", name, got, want)
		}
	}
}

func TestRateLimiting_RetryAfterRoundsUp(t *testing.T) {
	tests := []struct {
		retryAfter time.Duration
		want       string
	}{
		{700 * time.Millisecond, "1"},
		{1 * time.Second, "1"},
		{1001 * time.Millisecond, "2"},
		{20 * time.Second, "20"},
	}

	for _, tt := range tests {
		t.Run(tt.retryAfter.String(), func(t *testing.T) {
			allower := &fakeAllower{res: limiter.Result{Allowed: false, Limit: 5, RetryAfter: tt.retryAfter}}
			req := httptest.NewRequest(http.MethodGet, "/api/hello", nil)

			rec, _ := serve(t, newFakeRules(), allower, nil, req)

			if got := rec.Header().Get("Retry-After"); got != tt.want {
				t.Errorf("Retry-After = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestRateLimiting_StoreErrorFailsOpen(t *testing.T) {
	allower := &fakeAllower{err: errors.New("redis down")}
	req := httptest.NewRequest(http.MethodGet, "/api/hello", nil)

	rec, nextCalled := serve(t, newFakeRules(), allower, nil, req)

	if !nextCalled {
		t.Error("next was not called, want called (fail open)")
	}
	if rec.Code != http.StatusOK {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	if got := rec.Header().Get("X-Ratelimit-Limit"); got != "" {
		t.Errorf("X-Ratelimit-Limit = %q, want no header when the check failed", got)
	}
}

func TestRateLimiting_ContextCanceled(t *testing.T) {
	allower := &fakeAllower{err: context.Canceled}
	req := httptest.NewRequest(http.MethodGet, "/api/hello", nil)

	rec, nextCalled := serve(t, newFakeRules(), allower, nil, req)

	if nextCalled {
		t.Error("next was called, want not called when the client is gone")
	}
	if rec.Body.Len() != 0 {
		t.Errorf("body = %q, want empty", rec.Body.String())
	}
}

func TestRateLimiting_PassesClientIDAndRule(t *testing.T) {
	allower := &fakeAllower{res: limiter.Result{Allowed: true, Limit: 5, Remaining: 4}}
	req := httptest.NewRequest(http.MethodGet, "/api/hello", nil)
	req.Header.Set("X-Client-ID", "alice")

	serve(t, newFakeRules(), allower, nil, req)

	if allower.gotClientID != "alice" {
		t.Errorf("clientID = %q, want %q", allower.gotClientID, "alice")
	}
	if allower.gotRule != helloRule {
		t.Errorf("rule = %+v, want %+v", allower.gotRule, helloRule)
	}
}

func TestGetClientID(t *testing.T) {
	tests := []struct {
		name       string
		remoteAddr string
		header     string
		want       string
	}{
		{"ipv4 with port", "1.2.3.4:5678", "", "1.2.3.4"},
		{"ipv6 with port", "[::1]:8080", "", "::1"},
		{"no port", "1.2.3.4", "", "1.2.3.4"},
		{"header wins", "1.2.3.4:5678", "alice", "alice"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/api/hello", nil)
			req.RemoteAddr = tt.remoteAddr
			if tt.header != "" {
				req.Header.Set("X-Client-ID", tt.header)
			}

			if got := getClientID(req); got != tt.want {
				t.Errorf("getClientID = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestRateLimiting_SlowStoreFailsOpen(t *testing.T) {
	slowAllower := slowFakeAllower{}

	req := httptest.NewRequest(http.MethodGet, "/api/hello", nil)
	req.Header.Set("X-Client-ID", "alice")

	startTime := time.Now()
	rec, nextCalled := serve(t, newFakeRules(), &slowAllower, nil, req)
	elapsed := time.Since(startTime)

	if !nextCalled {
		t.Errorf("next function is not called")
	}

	if !errors.Is(slowAllower.gotErr, context.DeadlineExceeded) {
		t.Errorf("invalid error type = %v want %v", slowAllower.gotErr, context.DeadlineExceeded)
	}

	limitInHeader := rec.Header().Get("X-Ratelimit-Limit")
	if limitInHeader != "" {
		t.Errorf("expected empty value for X-Ratelimit-Limit, got %s", limitInHeader)
	}

	expectedUpperBoundTimeOut := time.Second
	if elapsed > expectedUpperBoundTimeOut {
		t.Errorf("the rate limit should take less than: %d ms, got %d ms", expectedUpperBoundTimeOut.Milliseconds(), elapsed.Milliseconds())
	}
}

func TestRateLimiting_QueuedMessageContents(t *testing.T) {
	enqueuer := &fakeEnqueuer{id: "1-0"}
	req := httptest.NewRequest(http.MethodPost, "/api/messages", strings.NewReader("hello"))
	req.Header.Set("X-Client-ID", "alice")

	before := time.Now()
	serve(t, newFakeRules(), blockedAllower(), enqueuer, req)

	if !enqueuer.called {
		t.Fatal("Enqueue was not called, want called")
	}

	msg := enqueuer.gotMsg
	if msg.ClientID != "alice" {
		t.Errorf("ClientID = %q, want %q", msg.ClientID, "alice")
	}
	if msg.Method != "POST" || msg.Path != "/api/messages" {
		t.Errorf("Method, Path = %q, %q; want %q, %q", msg.Method, msg.Path, "POST", "/api/messages")
	}
	if msg.Body != "hello" {
		t.Errorf("Body = %q, want %q", msg.Body, "hello")
	}

	if msg.ReceivedAt.Before(before.Truncate(time.Microsecond)) || msg.ReceivedAt.After(time.Now()) {
		t.Errorf("ReceivedAt = %v, want between %v and now", msg.ReceivedAt, before)
	}
}

func TestRateLimiting_QueuedResponseBody(t *testing.T) {
	enqueuer := &fakeEnqueuer{id: "1730281234567-0"}
	req := httptest.NewRequest(http.MethodPost, "/api/messages", strings.NewReader("hello"))

	rec, _ := serve(t, newFakeRules(), blockedAllower(), enqueuer, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusAccepted)
	}
	if got := rec.Header().Get("Content-Type"); got != "application/json" {
		t.Errorf("Content-Type = %q, want %q", got, "application/json")
	}
	if got := rec.Header().Get("X-Ratelimit-Limit"); got != "10" {
		t.Errorf("X-Ratelimit-Limit = %q, want %q (headers kept on 202)", got, "10")
	}

	var body struct {
		Queued bool   `json:"queued"`
		ID     string `json:"id"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
		t.Fatalf("decoding body %q: %v", rec.Body.String(), err)
	}
	if !body.Queued {
		t.Error("queued = false, want true")
	}
	if body.ID != enqueuer.id {
		t.Errorf("id = %q, want %q", body.ID, enqueuer.id)
	}
}

func TestRateLimiting_QueuedBodyTooLarge(t *testing.T) {
	enqueuer := &fakeEnqueuer{id: "1-0"}
	tooLarge := strings.Repeat("a", 1<<20+1)
	req := httptest.NewRequest(http.MethodPost, "/api/messages", strings.NewReader(tooLarge))

	rec, nextCalled := serve(t, newFakeRules(), blockedAllower(), enqueuer, req)

	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusRequestEntityTooLarge)
	}
	if enqueuer.called {
		t.Error("Enqueue was called, want not called for an oversized body")
	}
	if nextCalled {
		t.Error("next was called, want not called")
	}
}

func TestRateLimiting_BlockedOutcomes(t *testing.T) {
	tests := []struct {
		name   string
		method string
		path   string

		enqueuer *fakeEnqueuer
		disabled bool // pass a nil Enqueuer to the middleware (queuing turned off)

		wantStatus     int
		wantEnqueueHit bool
	}{
		{name: "drop rule", method: "GET", path: "/api/hello", enqueuer: &fakeEnqueuer{id: "1-0"}, wantStatus: http.StatusTooManyRequests, wantEnqueueHit: false},
		{name: "queued", method: "POST", path: "/api/messages", enqueuer: &fakeEnqueuer{id: "1-0"}, wantStatus: http.StatusAccepted, wantEnqueueHit: true},
		{name: "queue full", method: "POST", path: "/api/messages", enqueuer: &fakeEnqueuer{err: queue.ErrQueueFull}, wantStatus: http.StatusTooManyRequests, wantEnqueueHit: true},
		{name: "enqueue error", method: "POST", path: "/api/messages", enqueuer: &fakeEnqueuer{err: errors.New("redis down")}, wantStatus: http.StatusTooManyRequests, wantEnqueueHit: true},
		{name: "queuing disabled", method: "POST", path: "/api/messages", disabled: true, wantStatus: http.StatusTooManyRequests},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(tt.method, tt.path, strings.NewReader("hello"))

			// A nil *fakeEnqueuer stored in an Enqueuer is not a nil interface,
			// so queuing disabled must pass an Enqueuer that was never assigned.
			var eq Enqueuer
			if !tt.disabled {
				eq = tt.enqueuer
			}

			rec, nextCalled := serve(t, newFakeRules(), blockedAllower(), eq, req)

			if rec.Code != tt.wantStatus {
				t.Errorf("status = %d, want %d", rec.Code, tt.wantStatus)
			}
			if nextCalled {
				t.Error("next was called, want not called for a blocked request")
			}
			if !tt.disabled && tt.enqueuer.called != tt.wantEnqueueHit {
				t.Errorf("Enqueue called = %v, want %v", tt.enqueuer.called, tt.wantEnqueueHit)
			}
		})
	}
}
