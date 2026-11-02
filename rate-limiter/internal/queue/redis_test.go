package queue

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"
)

// newTestQueue connects to Redis (skipping the test if it is unavailable) and
// uses a stream unique to the test, deleted when the test ends.
func newTestQueue(t *testing.T, maxLength int) *RedisQueue {
	t.Helper()

	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "localhost:6379"
	}

	stream := "test:" + t.Name()
	rq, err := NewRedisQueue(addr, stream, maxLength)
	if err != nil {
		t.Skipf("redis not available at %s: %v", addr, err)
	}

	rq.client.Del(context.Background(), stream)
	t.Cleanup(func() {
		rq.client.Del(context.Background(), stream)
		rq.Close()
	})
	return rq
}

func getTestMsg(t *testing.T) Message {
	receivedAt, err := time.Parse(time.RFC3339Nano, "2026-10-30T14:31:06.355578+08:00")
	if err != nil {
		t.Fatalf("invalid time format: %v", err)
	}
	return Message{
		ClientID:   "alice",
		Method:     "POST",
		Path:       "/api/messages",
		Body:       "hello",
		ReceivedAt: receivedAt,
	}
}

func TestEnqueue_AddsToStream(t *testing.T) {
	rq := newTestQueue(t, 10)
	ctx := t.Context()
	testMsg := getTestMsg(t)

	id, err := rq.Enqueue(ctx, testMsg)
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if id == "" {
		t.Error("id is empty, want a stream ID")
	}

	n, err := rq.client.XLen(ctx, rq.StreamName).Result()
	if err != nil {
		t.Fatalf("XLen: %v", err)
	}
	if n != 1 {
		t.Errorf("XLEN = %d, want 1", n)
	}
}

func TestEnqueue_StoresAllFields(t *testing.T) {
	rq := newTestQueue(t, 10)
	ctx := t.Context()

	testMsg := getTestMsg(t)

	id, err := rq.Enqueue(ctx, testMsg)
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	entries, err := rq.client.XRange(ctx, rq.StreamName, "-", "+").Result()
	if err != nil {
		t.Fatalf("XRange: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("got %d entries, want 1", len(entries))
	}
	if entries[0].ID != id {
		t.Errorf("entry ID = %q, want the ID returned by Enqueue %q", entries[0].ID, id)
	}

	want := map[string]string{
		"clientId":   testMsg.ClientID,
		"method":     testMsg.Method,
		"path":       testMsg.Path,
		"body":       testMsg.Body,
		"receivedAt": testMsg.ReceivedAt.Format(time.RFC3339Nano),
	}
	for field, wantVal := range want {
		if got := entries[0].Values[field]; got != wantVal {
			t.Errorf("field %q = %v, want %q", field, got, wantVal)
		}
	}
}

func TestEnqueue_UniqueIDs(t *testing.T) {
	rq := newTestQueue(t, 10)
	ctx := t.Context()
	testMsg := getTestMsg(t)

	first, err := rq.Enqueue(ctx, testMsg)
	if err != nil {
		t.Fatalf("Enqueue 1: %v", err)
	}
	second, err := rq.Enqueue(ctx, testMsg)
	if err != nil {
		t.Fatalf("Enqueue 2: %v", err)
	}
	if first == second {
		t.Errorf("both enqueues returned ID %q, want different IDs", first)
	}
}

func TestEnqueue_Full(t *testing.T) {
	rq := newTestQueue(t, 2)
	ctx := t.Context()
	testMsg := getTestMsg(t)

	for i := range 2 {
		if _, err := rq.Enqueue(ctx, testMsg); err != nil {
			t.Fatalf("Enqueue %d: %v", i+1, err)
		}
	}

	_, err := rq.Enqueue(ctx, testMsg)
	if !errors.Is(err, ErrQueueFull) {
		t.Errorf("Enqueue 3: err = %v, want ErrQueueFull", err)
	}

	n, err := rq.client.XLen(ctx, rq.StreamName).Result()
	if err != nil {
		t.Fatalf("XLen: %v", err)
	}
	if n != 2 {
		t.Errorf("XLEN = %d, want 2 (rejected message must not be added)", n)
	}
}

func TestEnqueue_CanceledContext(t *testing.T) {
	rq := newTestQueue(t, 10)
	testMsg := getTestMsg(t)

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	if _, err := rq.Enqueue(ctx, testMsg); err == nil {
		t.Error("err = nil, want an error with a canceled context")
	}
}
