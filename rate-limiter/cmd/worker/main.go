package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/rpw-11/rate-limiter-in-go/internal/queue"
)

func main() {
	streamName := flag.String("stream-name", "req-queue", "stream name of the requests queue")
	groupName := flag.String("group-name", "workers", "the group's name that will be attached to the stream")
	name := flag.String("name", "worker-1", "the worker's name that belongs to the group")
	rate := flag.Int("rate", 10, "messages per second")
	flag.Parse()

	if *rate <= 0 {
		log.Fatalf("rate must be positive, got: %d", *rate)
	}

	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "localhost:6379"
	}

	// Check the redis
	client := redis.NewClient(&redis.Options{
		Addr: addr,
	})

	setupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := client.Ping(setupCtx).Result()
	defer client.Close()
	if err != nil {
		log.Fatalf("connecting to redis at %s: %v", addr, err)
	}

	consumer := queue.NewConsumer(*streamName, *groupName, *name, client)
	if err := consumer.EnsureGroup(setupCtx); err != nil {
		log.Fatalf("creating consumer group %q on stream %q: %v", *groupName, *streamName, err)
	}

	log.Printf("group %q ready on stream %q as consumer %q", *groupName, *streamName, *name)

	readCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	recoverPending(readCtx, consumer, *rate)

	for {
		deliveries, err := consumer.Read(readCtx)
		if err != nil {
			if err == readCtx.Err() {
				break
			}
			log.Printf("error reading the message for consumer %s: %v", consumer.Name, err)
			time.Sleep(time.Second)
			continue
		}
		processAll(readCtx, consumer, deliveries, *rate)
	}
}

func recoverPending(ctx context.Context, consumer *queue.Consumer, rate int) {
	after := "0"
	recovered := 0
	for {
		deliveries, lastID, err := consumer.ReadPending(ctx, after)
		if err != nil {
			log.Printf("reading pending messages: %v (continuing with new messages)", err)
			return
		}
		if lastID == "" {
			break
		}
		processAll(ctx, consumer, deliveries, rate)
		recovered += len(deliveries)
		after = lastID
	}
	log.Printf("recovered %d pending messages", recovered)
}

func processAll(ctx context.Context, consumer *queue.Consumer, deliveries []queue.Delivery, rate int) {
	ticker := time.NewTicker(time.Second / time.Duration(rate))
	defer ticker.Stop()

	for _, d := range deliveries {
		if ctx.Err() != nil { // signal interrupt
			return
		}

		<-ticker.C
		msg := d.Message
		log.Printf("processing %s from %s: %s %s body=%q waited=%v",
			d.ID, msg.ClientID, msg.Method, msg.Path, msg.Body, time.Since(msg.ReceivedAt).Round(time.Millisecond))

		ackCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		if err := consumer.Ack(ackCtx, d.ID); err != nil {
			log.Printf("acknowledging %s: %v", d.ID, err)
		}
		cancel()
	}
}
