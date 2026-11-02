package middleware

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"math"
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/rpw-11/rate-limiter-in-go/internal/limiter"
	"github.com/rpw-11/rate-limiter-in-go/internal/queue"
	"github.com/rpw-11/rate-limiter-in-go/internal/rules"
)

type RuleGetter interface {
	Get(method, path string) (rules.Rule, bool)
}

type Allower interface {
	Allow(ctx context.Context, clientID string, rule rules.Rule) (limiter.Result, error)
}

type Enqueuer interface {
	Enqueue(ctx context.Context, msg queue.Message) (string, error)
}

type ResponseData struct {
	Queued bool   `json:"queued"`
	Id     string `json:"id"`
}

const rateLimitCtxTimeout = 100 * time.Millisecond

func RateLimiting(rg RuleGetter, a Allower, eq Enqueuer) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			rule, ok := rg.Get(r.Method, r.URL.Path)
			if !ok {
				next.ServeHTTP(w, r)
				return
			}

			clientID := getClientID(r)

			ctx, cancel := context.WithTimeout(r.Context(), rateLimitCtxTimeout)
			defer cancel()

			res, err := a.Allow(ctx, clientID, rule)
			if err != nil {
				if errors.Is(err, context.Canceled) {
					return
				}

				log.Printf("rate limit check failed: %v", err)
				// Fail open: if store is unavailable, prioritize availability
				next.ServeHTTP(w, r)
				return
			}

			w.Header().Set("X-Ratelimit-Limit", strconv.Itoa(res.Limit))
			w.Header().Set("X-Ratelimit-Remaining", strconv.Itoa(res.Remaining))

			if res.Allowed {
				next.ServeHTTP(w, r)
				return
			}

			if rule.IsQueueAble() && eq != nil {
				r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
				bodyBytes, err := io.ReadAll(r.Body)
				if err != nil {
					if _, ok := errors.AsType[*http.MaxBytesError](err); ok {
						http.Error(w, "request body too large", http.StatusRequestEntityTooLarge)
						return
					}
					http.Error(w, "error reading body", http.StatusBadRequest)
					return
				}

				msg := queue.Message{
					ClientID:   clientID,
					Method:     rule.Method,
					Path:       rule.Path,
					Body:       string(bodyBytes),
					ReceivedAt: time.Now(),
				}

				id, err := eq.Enqueue(ctx, msg)
				if err == nil {
					writeAcceptedJson(w, id)
					return
				}

				if !errors.Is(err, queue.ErrQueueFull) {
					log.Printf("enqueue failed, rejecting: %v", err)
				}
			}

			retryAfter := int(math.Ceil(res.RetryAfter.Seconds()))
			w.Header().Set("X-Ratelimit-Retry-After", strconv.Itoa(retryAfter))
			w.Header().Set("Retry-After", strconv.Itoa(retryAfter))

			http.Error(w, "too many requests", http.StatusTooManyRequests)
		})
	}
}

func getClientID(r *http.Request) string {
	if clientID := r.Header.Get("X-Client-ID"); clientID != "" {
		return clientID
	}

	ip := r.RemoteAddr
	host, _, err := net.SplitHostPort(ip)

	if err != nil {
		return ip
	}

	return host
}

func writeAcceptedJson(w http.ResponseWriter, id string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)

	res := ResponseData{
		Queued: true,
		Id:     id,
	}
	err := json.NewEncoder(w).Encode(res)
	if err != nil {
		log.Printf("error encoding the json file: %v", err)
	}
}
