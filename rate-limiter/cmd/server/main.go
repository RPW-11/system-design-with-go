package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/rpw-11/rate-limiter-in-go/internal/limiter"
	"github.com/rpw-11/rate-limiter-in-go/internal/middleware"
	"github.com/rpw-11/rate-limiter-in-go/internal/queue"
	"github.com/rpw-11/rate-limiter-in-go/internal/rules"
	"github.com/rpw-11/rate-limiter-in-go/internal/store"
)

func main() {
	path := "rules.yaml"
	rulesCache, err := rules.NewCache(path)

	if err != nil {
		log.Fatalf("failed creating the rules cache: %v", err)
	}

	rulesCache.StartRefresher(context.Background(), 5*time.Second)

	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "localhost:6379"
	}

	// Store setup
	redisStore, err := store.NewRedisStore(addr)
	if err != nil {
		log.Fatalf("failed creating redis store: %v", err)
	}
	defer redisStore.Close()

	// Queue setup
	redisQueue, err := queue.NewRedisQueue(addr, "req-queue", 20)
	if err != nil {
		log.Fatalf("failed creating redis queue: %v", err)
	}
	defer redisQueue.Close()

	// Limiter Setup
	lim := limiter.New(redisStore)

	// Middleware
	rateLimiterMdl := middleware.RateLimiting(rulesCache, lim, redisQueue)

	mux := http.NewServeMux()

	mux.HandleFunc("GET /api/hello", getHelloHandler)
	mux.HandleFunc("POST /api/messages", postHelloHandler)

	handler := rateLimiterMdl(mux)

	port := getPort()
	fmt.Println("Running the server on port", port)
	err = http.ListenAndServe(port, handler)
	if err != nil {
		redisStore.Close()
		log.Fatal(err)
	}
}

func getHelloHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintln(w, "Hello, welcome to Rai's page")
}

func postHelloHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintln(w, "POST: Hello, welcome to Rai's page")
}

func getPort() string {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	return fmt.Sprintf(":%s", port)
}
