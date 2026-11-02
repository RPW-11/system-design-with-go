package rules

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

type Rule struct {
	Method  string `yaml:"method"`
	Path    string `yaml:"path"`
	Limit   int    `yaml:"limit"`
	Unit    string `yaml:"unit"`
	OnLimit string `yaml:"on_limit"`
}

func (r *Rule) Window() (time.Duration, error) {
	switch r.Unit {
	case "second":
		return time.Second, nil
	case "minute":
		return time.Minute, nil
	case "hour":
		return time.Hour, nil
	case "day":
		return time.Hour * 24, nil
	}

	return 0, fmt.Errorf("invalid unit %q", r.Unit)
}

func (r *Rule) IsQueueAble() bool {
	return r.OnLimit == "queue"
}

func Load(path string) ([]Rule, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed reading the file: %w", err)
	}

	var cfg struct {
		Rules []Rule `yaml:"rules"`
	}

	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("failed parsing the yaml file: %w", err)
	}

	for i, rule := range cfg.Rules {
		if !isMethodValid(rule.Method) {
			return nil, fmt.Errorf("rule %d: invalid method %q", i, rule.Method)
		}

		if !isOnLimitModeValid(rule.OnLimit) {
			return nil, fmt.Errorf("rule %d: invalid on limit value %q", i, rule.OnLimit)
		}

		if rule.Path == "" {
			return nil, fmt.Errorf("rule %d: path must not be empty", i)
		}

		if _, err := rule.Window(); err != nil {
			return nil, fmt.Errorf("rule %d: %w", i, err)
		}

		if rule.Limit <= 0 {
			return nil, fmt.Errorf("rule %d: limit must be more than 0, got %d", i, rule.Limit)
		}
	}

	return cfg.Rules, nil
}

func isMethodValid(method string) bool {
	switch method {
	case "GET", "POST", "DELETE":
		return true
	}
	return false
}

func isOnLimitModeValid(mode string) bool {
	switch mode {
	case "drop", "queue", "":
		return true
	}
	return false
}
