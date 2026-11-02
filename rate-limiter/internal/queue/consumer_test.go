package queue

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func newTestConsumer(t *testing.T, rq *RedisQueue) *Consumer {
	t.Helper()

	c := NewConsumer(rq.StreamName, "test-group", "test-consumer", rq.client)
	if err := c.EnsureGroup(t.Context()); err != nil {
		t.Fatalf("EnsureGroup: %v", err)
	}
	t.Cleanup(func() { rq.client.Del(context.Background(), c.DeadStreamName) })
	return c
}

func mustEnqueue(t *testing.T, rq *RedisQueue, msg Message) string {
	t.Helper()

	id, err := rq.Enqueue(t.Context(), msg)
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	return id
}

func mustRead(t *testing.T, c *Consumer) []Delivery {
	t.Helper()

	deliveries, err := c.Read(t.Context())
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	return deliveries
}

func counts(t *testing.T, c *Consumer) (length, pending int64) {
	t.Helper()

	length, err := c.client.XLen(t.Context(), c.StreamName).Result()
	if err != nil {
		t.Fatalf("XLen: %v", err)
	}
	p, err := c.client.XPending(t.Context(), c.StreamName, c.GroupName).Result()
	if err != nil {
		t.Fatalf("XPending: %v", err)
	}
	return length, p.Count
}

func TestEnsureGroup_Twice(t *testing.T) {
	rq := newTestQueue(t, 10)
	c := newTestConsumer(t, rq)

	if err := c.EnsureGroup(t.Context()); err != nil {
		t.Errorf("second EnsureGroup: %v, want nil", err)
	}
}

func TestRead_RoundTrip(t *testing.T) {
	rq := newTestQueue(t, 10)
	c := newTestConsumer(t, rq)
	sent := getTestMsg(t)
	id := mustEnqueue(t, rq, sent)

	deliveries := mustRead(t, c)

	if len(deliveries) != 1 {
		t.Fatalf("got %d deliveries, want 1", len(deliveries))
	}
	d := deliveries[0]
	if d.ID != id {
		t.Errorf("ID = %q, want %q", d.ID, id)
	}
	got := d.Message
	if got.ClientID != sent.ClientID || got.Method != sent.Method || got.Path != sent.Path || got.Body != sent.Body {
		t.Errorf("message = %+v, want %+v", got, sent)
	}
	if !got.ReceivedAt.Equal(sent.ReceivedAt) {
		t.Errorf("ReceivedAt = %v, want %v", got.ReceivedAt, sent.ReceivedAt)
	}
}

func TestRead_DoesNotAck(t *testing.T) {
	rq := newTestQueue(t, 10)
	c := newTestConsumer(t, rq)
	mustEnqueue(t, rq, getTestMsg(t))

	mustRead(t, c)

	length, pending := counts(t, c)
	if length != 1 || pending != 1 {
		t.Errorf("XLEN, pending = %d, %d; want 1, 1 until Ack", length, pending)
	}
}

func TestAck_RemovesMessage(t *testing.T) {
	rq := newTestQueue(t, 10)
	c := newTestConsumer(t, rq)
	mustEnqueue(t, rq, getTestMsg(t))
	deliveries := mustRead(t, c)

	if err := c.Ack(t.Context(), deliveries[0].ID); err != nil {
		t.Fatalf("Ack: %v", err)
	}

	length, pending := counts(t, c)
	if length != 0 || pending != 0 {
		t.Errorf("XLEN, pending = %d, %d; want 0, 0 after Ack", length, pending)
	}
}

func TestAck_Twice(t *testing.T) {
	rq := newTestQueue(t, 10)
	c := newTestConsumer(t, rq)
	mustEnqueue(t, rq, getTestMsg(t))
	deliveries := mustRead(t, c)

	if err := c.Ack(t.Context(), deliveries[0].ID); err != nil {
		t.Fatalf("first Ack: %v", err)
	}
	if err := c.Ack(t.Context(), deliveries[0].ID); err == nil {
		t.Error("second Ack: err = nil, want an error for a message that is already gone")
	}
}

func TestRead_PoisonMessageIsDeadLettered(t *testing.T) {
	rq := newTestQueue(t, 10)
	c := newTestConsumer(t, rq)
	badID, err := rq.client.XAdd(t.Context(), &redis.XAddArgs{
		Stream: rq.StreamName,
		Values: map[string]any{"clientId": "bad"},
	}).Result()
	if err != nil {
		t.Fatalf("XAdd: %v", err)
	}
	goodID := mustEnqueue(t, rq, getTestMsg(t))

	deliveries := mustRead(t, c)

	if len(deliveries) != 1 || deliveries[0].ID != goodID {
		t.Fatalf("deliveries = %+v, want only the good message %s", deliveries, goodID)
	}

	length, pending := counts(t, c)
	if length != 1 || pending != 1 {
		t.Errorf("XLEN, pending = %d, %d; want 1, 1 (only the good message left)", length, pending)
	}

	dead, err := c.client.XRange(t.Context(), c.DeadStreamName, "-", "+").Result()
	if err != nil {
		t.Fatalf("XRange dead: %v", err)
	}
	if len(dead) != 1 {
		t.Fatalf("got %d dead-letter entries, want 1", len(dead))
	}
	values := dead[0].Values
	if values["originalId"] != badID {
		t.Errorf("originalId = %v, want %s", values["originalId"], badID)
	}
	if values["clientId"] != "bad" {
		t.Errorf("clientId = %v, want the original field kept", values["clientId"])
	}
	if errText, _ := values["error"].(string); !strings.Contains(errText, `"receivedAt"`) {
		t.Errorf("error = %q, want it to name the missing field", errText)
	}
}

func TestReadPending(t *testing.T) {
	rq := newTestQueue(t, 10)
	c := newTestConsumer(t, rq)
	for range 3 {
		mustEnqueue(t, rq, getTestMsg(t))
	}
	read := mustRead(t, c)
	if len(read) != 3 {
		t.Fatalf("Read: got %d deliveries, want 3", len(read))
	}
	if err := c.Ack(t.Context(), read[0].ID); err != nil {
		t.Fatalf("Ack: %v", err)
	}

	pending, lastID, err := c.ReadPending(t.Context(), "0")
	if err != nil {
		t.Fatalf("ReadPending: %v", err)
	}
	if len(pending) != 2 || pending[0].ID != read[1].ID || pending[1].ID != read[2].ID {
		t.Errorf("pending = %+v, want the 2 unacknowledged messages", pending)
	}
	if lastID != read[2].ID {
		t.Errorf("lastID = %q, want %q", lastID, read[2].ID)
	}

	rest, lastID, err := c.ReadPending(t.Context(), lastID)
	if err != nil {
		t.Fatalf("ReadPending after lastID: %v", err)
	}
	if len(rest) != 0 || lastID != "" {
		t.Errorf("after the last pending ID: got %d deliveries, lastID %q; want 0, empty", len(rest), lastID)
	}
}

func TestReadPending_DoesNotBlock(t *testing.T) {
	rq := newTestQueue(t, 10)
	c := newTestConsumer(t, rq)

	start := time.Now()
	deliveries, lastID, err := c.ReadPending(t.Context(), "0")
	if err != nil {
		t.Fatalf("ReadPending: %v", err)
	}
	if len(deliveries) != 0 || lastID != "" {
		t.Errorf("got %d deliveries, lastID %q; want 0, empty", len(deliveries), lastID)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("ReadPending took %v, want it to return immediately", elapsed)
	}
}

func TestReadPending_SkipsPoisonButMovesOn(t *testing.T) {
	rq := newTestQueue(t, 10)
	c := newTestConsumer(t, rq)
	if err := rq.client.XAdd(t.Context(), &redis.XAddArgs{
		Stream: rq.StreamName,
		Values: map[string]any{"clientId": "bad"},
	}).Err(); err != nil {
		t.Fatalf("XAdd: %v", err)
	}
	mustEnqueue(t, rq, getTestMsg(t))

	if _, err := c.client.XReadGroup(t.Context(), &redis.XReadGroupArgs{
		Group:    c.GroupName,
		Consumer: c.Name,
		Streams:  []string{c.StreamName, ">"},
	}).Result(); err != nil {
		t.Fatalf("XReadGroup: %v", err)
	}

	deliveries, lastID, err := c.ReadPending(t.Context(), "0")
	if err != nil {
		t.Fatalf("ReadPending: %v", err)
	}
	if len(deliveries) != 1 {
		t.Errorf("got %d deliveries, want 1 (the good message)", len(deliveries))
	}
	if lastID == "" {
		t.Error("lastID is empty, want the last ID Redis returned so the caller keeps going")
	}
}

func TestToQueueMessage(t *testing.T) {
	valid := func() map[string]any {
		return map[string]any{
			"clientId":   "alice",
			"method":     "POST",
			"path":       "/api/messages",
			"body":       "hello",
			"receivedAt": "2026-10-30T14:31:06.355578+08:00",
		}
	}

	t.Run("valid", func(t *testing.T) {
		got, err := toQueueMessage(valid())
		if err != nil {
			t.Fatalf("toQueueMessage: %v", err)
		}
		want := getTestMsg(t)
		if got.ClientID != want.ClientID || got.Method != want.Method || got.Path != want.Path || got.Body != want.Body || !got.ReceivedAt.Equal(want.ReceivedAt) {
			t.Errorf("got %+v, want %+v", got, want)
		}
	})

	t.Run("empty body is allowed", func(t *testing.T) {
		values := valid()
		values["body"] = ""
		if _, err := toQueueMessage(values); err != nil {
			t.Errorf("toQueueMessage: %v, want nil", err)
		}
	})

	for _, field := range []string{"clientId", "method", "path", "body", "receivedAt"} {
		t.Run("missing "+field, func(t *testing.T) {
			values := valid()
			delete(values, field)

			_, err := toQueueMessage(values)
			if err == nil || !strings.Contains(err.Error(), `"`+field+`"`) {
				t.Errorf("err = %v, want an error naming %q", err, field)
			}
		})
	}

	t.Run("non-string field", func(t *testing.T) {
		values := valid()
		values["method"] = 42

		if _, err := toQueueMessage(values); err == nil {
			t.Error("err = nil, want an error")
		}
	})

	t.Run("bad receivedAt", func(t *testing.T) {
		values := valid()
		values["receivedAt"] = "yesterday"

		_, err := toQueueMessage(values)
		if err == nil || !strings.Contains(err.Error(), `"receivedAt"`) {
			t.Errorf("err = %v, want a parse error naming receivedAt", err)
		}
	})
}
