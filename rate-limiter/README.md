# Rate Limiter in Go

A small simulation of the rate limiter design from *System Design Interview*, Figure 4-13: rules loaded from a file, counters in Redis, and requests over the limit either dropped (429) or queued for later (202).

```
client ──► middleware ──► rules cache ◄── refresher (rules.yaml)
              │
              ├── Redis counter (fixed window)
              │
              ├── under limit ──────────────► API handler (200)
              └── over limit ─┬─ drop ──────► 429 + Retry-After
                              └─ queue ─────► Redis Stream (202) ──► worker
```

## Components

| Path | What it does |
|---|---|
| `internal/rules` | Loads and validates `rules.yaml`; a cache reloads it every 5s |
| `internal/store` | Redis counter: `INCR` + `PEXPIRE NX` in one transaction |
| `internal/limiter` | Fixed-window algorithm: key per client, rule and window |
| `internal/middleware` | Applies the rule, sets `X-Ratelimit-*` headers, returns 429 or queues |
| `internal/queue` | Redis Streams producer and consumer (ack, recovery, dead-letter) |
| `cmd/server` | API server with the middleware |
| `cmd/worker` | Processes queued requests at a fixed rate |
| `cmd/loadtest` | Sends concurrent or paced traffic and prints results per second |

## Run

```bash
docker compose up -d
go run ./cmd/server
go run ./cmd/worker -rate 5
go run ./cmd/loadtest -method POST -url http://localhost:8080/api/messages -n 90 -rate 30
go test -race ./...
```

`PORT` and `REDIS_ADDR` configure the server; run two servers on different ports to see one shared limit.

## Key lessons

- **Atomic counting.** `INCR` hands each request a unique number, so 50 concurrent requests let exactly 5 through. A `GET`, check, `SET` sequence would not.
- **Shared state.** Counters live in Redis, not in each server, so the limit holds across instances.
- **Fixed window trade-offs.** Simple, but allows up to 2x the limit around a window boundary, and depends on server clocks agreeing.
- **Fail open, quickly.** If Redis is down, requests are allowed; a 100ms timeout keeps that from slowing every request.
- **Small interfaces at the consumer.** `Store`, `RuleGetter`, `Allower` and `Enqueuer` let every layer be tested with fakes.
- **Hot-reloaded config.** A bad rules file is rejected and the old rules stay active.
- **Queue, not just delay.** A bounded queue plus a rate-limited worker turns bursts into steady load; 202 tells the client the request was accepted.
- **At-least-once delivery.** Acknowledge after processing; pending messages are recovered on restart, so processing must be idempotent.
- **Poison messages.** Messages that can never be decoded go to a dead-letter stream instead of blocking the queue.
- **Tests that can fail.** Fakes are scripted, each case has one problem, and mutation checks confirm tests catch real bugs.
