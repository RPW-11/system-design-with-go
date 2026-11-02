package queue

import (
	"context"
	"errors"
	"fmt"
	"log"
	"maps"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

type Consumer struct {
	client         *redis.Client
	StreamName     string
	GroupName      string
	Name           string // consumer name
	DeadStreamName string
}

type Delivery struct {
	ID      string
	Message Message
}

const deadStreamMaxLength = 1000

func NewConsumer(streamName, groupName, name string, redisClient *redis.Client) *Consumer {
	return &Consumer{
		client:         redisClient,
		StreamName:     streamName,
		GroupName:      groupName,
		Name:           name,
		DeadStreamName: streamName + ":dead",
	}
}

const busyGroupPrefix = "BUSYGROUP"

func (c *Consumer) EnsureGroup(ctx context.Context) error {
	err := c.client.XGroupCreateMkStream(ctx, c.StreamName, c.GroupName, "0").Err()
	if err != nil && !strings.HasPrefix(err.Error(), busyGroupPrefix) {
		return err
	}
	return nil
}

func (c *Consumer) Read(ctx context.Context) ([]Delivery, error) {
	deliveries, _, err := c.read(ctx, ">", 5*time.Second)
	return deliveries, err
}

func (c *Consumer) ReadPending(ctx context.Context, after string) (deliveries []Delivery, lastID string, err error) {
	return c.read(ctx, after, -1)
}

func (c *Consumer) read(ctx context.Context, id string, block time.Duration) ([]Delivery, string, error) {
	streams, err := c.client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Consumer: c.Name,
		Group:    c.GroupName,
		Streams:  []string{c.StreamName, id},
		Count:    10,
		Block:    block,
	}).Result()

	deliveries := []Delivery{}
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return deliveries, "", nil
		}
		return deliveries, "", err
	}

	lastID := ""
	for _, s := range streams {
		for _, m := range s.Messages {
			lastID = m.ID

			queueMsg, err := toQueueMessage(m.Values)
			if err != nil {
				c.deadLetter(ctx, m, err)
				continue
			}
			deliveries = append(deliveries, Delivery{
				ID:      m.ID,
				Message: queueMsg,
			})
		}
	}

	return deliveries, lastID, nil
}

func (c *Consumer) Ack(ctx context.Context, id string) error {
	statuses, err := c.client.XAckDel(ctx, c.StreamName, c.GroupName, "KEEPREF", id).Result()
	if err != nil {
		return err
	}

	if len(statuses) != 1 || statuses[0] != int64(1) {
		return fmt.Errorf("message %s in group %q on stream %q: XACKDEL statuses %v", id, c.GroupName, c.StreamName, statuses)
	}
	return nil
}

func (c *Consumer) deadLetter(ctx context.Context, m redis.XMessage, cause error) {
	log.Printf("dead-lettering message %s: %v", m.ID, cause)

	values := make(map[string]any, len(m.Values)+2)
	maps.Copy(values, m.Values)
	values["originalId"] = m.ID
	values["error"] = cause.Error()

	err := c.client.XAdd(ctx, &redis.XAddArgs{
		Stream: c.DeadStreamName,
		MaxLen: deadStreamMaxLength,
		Approx: true,
		Values: values,
	}).Err()
	if err != nil {
		log.Printf("adding message %s to %s: %v", m.ID, c.DeadStreamName, err)
		return
	}

	if err := c.Ack(ctx, m.ID); err != nil {
		log.Printf("acknowledging dead-lettered message %s: %v", m.ID, err)
	}
}

func toQueueMessage(values map[string]any) (Message, error) {
	receivedAtStr, err := stringField(values, "receivedAt")
	if err != nil {
		return Message{}, err
	}
	receivedAt, err := time.Parse(time.RFC3339Nano, receivedAtStr)
	if err != nil {
		return Message{}, fmt.Errorf("parsing field %q: %w", "receivedAt", err)
	}

	clientID, err := stringField(values, "clientId")
	if err != nil {
		return Message{}, err
	}
	method, err := stringField(values, "method")
	if err != nil {
		return Message{}, err
	}
	path, err := stringField(values, "path")
	if err != nil {
		return Message{}, err
	}
	body, err := stringField(values, "body")
	if err != nil {
		return Message{}, err
	}

	return Message{
		ClientID:   clientID,
		Method:     method,
		Path:       path,
		Body:       body,
		ReceivedAt: receivedAt,
	}, nil
}

func stringField(values map[string]any, field string) (string, error) {
	s, ok := values[field].(string)
	if !ok {
		return "", fmt.Errorf("missing or invalid field %q", field)
	}
	return s, nil
}
