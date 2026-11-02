package store

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"
)

func newTestStore(t *testing.T) *RedisStore {
	t.Helper()
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "localhost:6379"
	}
	rs, err := NewRedisStore(addr)
	if err != nil {
		t.Skipf("redis not available at %s: %v", addr, err)
	}
	t.Cleanup(func() { rs.Close() })
	return rs
}

func testKey(t *testing.T, rs *RedisStore) string {
	t.Helper()
	key := "test:" + t.Name()
	rs.client.Del(context.Background(), key)
	t.Cleanup(func() { rs.client.Del(context.Background(), key) })
	return key
}

func TestNewRedisStore_InvalidAddr(t *testing.T) {
	_, err := NewRedisStore("127.0.0.1:1")
	if err == nil {
		t.Fatal("expected error for unreachable address, got nil")
	}
}

func TestIncr_CountsUp(t *testing.T) {
	rs := newTestStore(t)
	key := testKey(t, rs)
	ctx := t.Context()

	for want := int64(1); want <= 3; want++ {
		got, err := rs.Incr(ctx, key, time.Minute)
		if err != nil {
			t.Fatalf("Incr: %v", err)
		}
		if got != want {
			t.Errorf("Incr = %d, want %d", got, want)
		}
	}
}

func TestIncr_SetsTTL(t *testing.T) {
	rs := newTestStore(t)
	key := testKey(t, rs)
	ctx := t.Context()
	window := time.Minute

	if _, err := rs.Incr(ctx, key, window); err != nil {
		t.Fatalf("Incr: %v", err)
	}

	ttl, err := rs.client.PTTL(ctx, key).Result()
	if err != nil {
		t.Fatalf("PTTL: %v", err)
	}
	if ttl <= 0 || ttl > window {
		t.Errorf("TTL = %v, want in (0, %v]", ttl, window)
	}
}

func TestIncr_DoesNotResetTTL(t *testing.T) {
	rs := newTestStore(t)
	key := testKey(t, rs)
	ctx := t.Context()
	window := time.Minute

	if _, err := rs.Incr(ctx, key, window); err != nil {
		t.Fatalf("Incr: %v", err)
	}
	first, err := rs.client.PTTL(ctx, key).Result()
	if err != nil {
		t.Errorf("failed calling PTTL: %v", err)
	}

	time.Sleep(200 * time.Millisecond)

	if _, err := rs.Incr(ctx, key, window); err != nil {
		t.Fatalf("Incr: %v", err)
	}
	second, _ := rs.client.PTTL(ctx, key).Result()

	if second >= first {
		t.Errorf("TTL was reset: first=%v second=%v", first, second)
	}
}

func TestIncr_ResetsAfterWindow(t *testing.T) {
	rs := newTestStore(t)
	key := testKey(t, rs)
	ctx := t.Context()
	window := 100 * time.Millisecond

	for range 3 {
		if _, err := rs.Incr(ctx, key, window); err != nil {
			t.Fatalf("Incr: %v", err)
		}
	}

	time.Sleep(window + 50*time.Millisecond)

	got, err := rs.Incr(ctx, key, window)
	if err != nil {
		t.Fatalf("Incr: %v", err)
	}
	if got != 1 {
		t.Errorf("Incr after window = %d, want 1", got)
	}
}

func TestIncr_KeysAreIndependent(t *testing.T) {
	rs := newTestStore(t)
	key := testKey(t, rs)
	keyA, keyB := key+":a", key+":b"
	t.Cleanup(func() { rs.client.Del(context.Background(), keyA, keyB) })
	ctx := t.Context()

	_, err := rs.Incr(ctx, keyA, time.Minute)
	if err != nil {
		t.Errorf("error incrementing key: %s", keyA)
	}

	_, err = rs.Incr(ctx, keyA, time.Minute)
	if err != nil {
		t.Errorf("error incrementing key: %s", keyA)
	}

	got, err := rs.Incr(ctx, keyB, time.Minute)
	if err != nil {
		t.Fatalf("Incr: %v", err)
	}
	if got != 1 {
		t.Errorf("Incr on keyB = %d, want 1 (keyA's count leaked)", got)
	}
}

func TestIncr_Concurrent(t *testing.T) {
	rs := newTestStore(t)
	key := testKey(t, rs)
	ctx := t.Context()
	const n = 100

	var wg sync.WaitGroup
	results := make(chan int64, n)

	for range n {
		wg.Go(func() {
			v, err := rs.Incr(ctx, key, time.Minute)
			if err != nil {
				t.Errorf("Incr: %v", err)
				return
			}
			results <- v
		})
	}

	wg.Wait()
	close(results)

	seen := make(map[int64]bool)
	for v := range results {
		if seen[v] {
			t.Errorf("value %d returned twice", v)
		}
		seen[v] = true
	}
	if len(seen) != n {
		t.Errorf("got %d distinct values, want %d", len(seen), n)
	}
}

func TestIncr_CanceledContext(t *testing.T) {
	rs := newTestStore(t)
	key := testKey(t, rs)

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	if _, err := rs.Incr(ctx, key, time.Minute); err == nil {
		t.Error("expected error with canceled context, got nil")
	}
}
