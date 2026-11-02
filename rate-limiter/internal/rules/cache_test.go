package rules

import (
	"os"
	"sync"
	"testing"
	"time"
)

const oneRule = `
rules:
- method: GET
  path: /api/hello
  limit: 5
  unit: minute
`

const updatedRules = `
rules:
- method: GET
  path: /api/hello
  limit: 50
  unit: minute
- method: POST
  path: /api/messages
  limit: 10
  unit: second
`

func mustNewCache(t *testing.T, path string) *Cache {
	t.Helper()

	c, err := NewCache(path)
	if err != nil {
		t.Fatalf("NewCache: %v", err)
	}
	return c
}

// rewrite replaces the contents of an existing rules file.
func rewrite(t *testing.T, path, content string) {
	t.Helper()

	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("rewriting rules file: %v", err)
	}
}

func TestCache_Get(t *testing.T) {
	c := mustNewCache(t, writeRules(t, oneRule))

	rule, ok := c.Get("GET", "/api/hello")
	if !ok {
		t.Fatal("Get(GET /api/hello): ok = false, want true")
	}
	if rule.Limit != 5 {
		t.Errorf("Limit = %d, want 5", rule.Limit)
	}

	if _, ok := c.Get("POST", "/api/hello"); ok {
		t.Error("Get(POST /api/hello): ok = true, want false (method must match)")
	}
	if _, ok := c.Get("GET", "/api/other"); ok {
		t.Error("Get(GET /api/other): ok = true, want false")
	}
}

func TestNewCache_InvalidFile(t *testing.T) {
	path := writeRules(t, "rules:\n- {method: GET, path: /a, limit: 0, unit: second}\n")

	if _, err := NewCache(path); err == nil {
		t.Error("err = nil, want an error for an invalid rules file")
	}
}

func TestCache_ReloadPicksUpChanges(t *testing.T) {
	path := writeRules(t, oneRule)
	c := mustNewCache(t, path)

	rewrite(t, path, updatedRules)
	if err := c.Reload(); err != nil {
		t.Fatalf("Reload: %v", err)
	}

	rule, ok := c.Get("GET", "/api/hello")
	if !ok || rule.Limit != 50 {
		t.Errorf("Get(GET /api/hello) = %+v, %v; want Limit 50", rule, ok)
	}
	if _, ok := c.Get("POST", "/api/messages"); !ok {
		t.Error("Get(POST /api/messages): ok = false, want true after reload")
	}
}

func TestCache_ReloadRemovesDeletedRules(t *testing.T) {
	path := writeRules(t, updatedRules)
	c := mustNewCache(t, path)

	rewrite(t, path, oneRule)
	if err := c.Reload(); err != nil {
		t.Fatalf("Reload: %v", err)
	}

	if _, ok := c.Get("POST", "/api/messages"); ok {
		t.Error("Get(POST /api/messages): ok = true, want false after the rule was removed")
	}
}

func TestCache_FailedReloadKeepsOldRules(t *testing.T) {
	path := writeRules(t, oneRule)
	c := mustNewCache(t, path)

	rewrite(t, path, "rules:\n- {method: GET, path: /api/hello, limit: 5, unit: minuet}\n")
	if err := c.Reload(); err == nil {
		t.Fatal("Reload: err = nil, want an error for an invalid file")
	}

	rule, ok := c.Get("GET", "/api/hello")
	if !ok || rule.Limit != 5 {
		t.Errorf("Get(GET /api/hello) = %+v, %v; want the old rule with Limit 5", rule, ok)
	}
}

func TestCache_RefresherPicksUpChanges(t *testing.T) {
	path := writeRules(t, oneRule)
	c := mustNewCache(t, path)

	c.StartRefresher(t.Context(), 10*time.Millisecond)
	rewrite(t, path, updatedRules)

	// Poll instead of sleeping a fixed time: the test passes as soon as the
	// refresher has run, and only fails if it never does.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if rule, ok := c.Get("GET", "/api/hello"); ok && rule.Limit == 50 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("refresher did not pick up the new rules within 2s")
}

func TestCache_ConcurrentGetAndReload(t *testing.T) {
	path := writeRules(t, oneRule)
	c := mustNewCache(t, path)

	// Run with -race: this fails if Get and Reload touch the map without the lock.
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for range 200 {
				if _, ok := c.Get("GET", "/api/hello"); !ok {
					t.Error("Get(GET /api/hello): ok = false during reload, want true")
					return
				}
			}
		})
	}
	for range 50 {
		if err := c.Reload(); err != nil {
			t.Errorf("Reload: %v", err)
		}
	}
	wg.Wait()
}
