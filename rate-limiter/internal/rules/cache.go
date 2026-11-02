package rules

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"
)

type Cache struct {
	mu          sync.RWMutex
	cfgFilePath string
	data        map[string]Rule
}

func NewCache(path string) (*Cache, error) {
	rules, err := Load(path)
	if err != nil {
		return nil, fmt.Errorf("failed reading rules: %w", err)
	}

	cache := &Cache{
		data:        make(map[string]Rule),
		cfgFilePath: path,
	}

	for _, rule := range rules {
		key := ruleKey(rule.Method, rule.Path)
		cache.data[key] = rule
	}

	return cache, nil
}

func (c *Cache) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return len(c.data)
}

func (c *Cache) Get(method, path string) (Rule, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	key := ruleKey(method, path)
	rule, ok := c.data[key]
	return rule, ok
}

func (c *Cache) Reload() error {
	if c.cfgFilePath == "" {
		return fmt.Errorf("empty config file path")
	}

	rules, err := Load(c.cfgFilePath)
	if err != nil {
		return fmt.Errorf("failed reading rules: %w", err)
	}

	newData := make(map[string]Rule)

	for _, rule := range rules {
		key := ruleKey(rule.Method, rule.Path)
		newData[key] = rule
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	c.data = newData

	return nil
}

func (c *Cache) StartRefresher(ctx context.Context, interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				err := c.Reload()
				if err != nil {
					log.Printf("reloading rules: %v", err)
				} else {
					log.Printf("successful reloading the rules. Total rules: %d", c.Len())
				}
			}
		}
	}()
}

func ruleKey(method, path string) string {
	return fmt.Sprintf("%s %s", method, path)
}
