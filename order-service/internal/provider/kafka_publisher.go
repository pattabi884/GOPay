package provider

import (
	"context"
	"gopay/pkg/outbox"
	"log/slog"
	"strconv"
	"time"

	"github.com/segmentio/kafka-go"
)

type KafkaPublisher struct {
	writer *kafka.Writer
	logger *slog.Logger
}

func NewKafkaPublisher(brokers []string, logger *slog.Logger) *KafkaPublisher {
	return &KafkaPublisher{
		writer: &kafka.Writer{
			Addr:         kafka.TCP(brokers...), //this expands the []string into seperate arguments ofr kafka.TCP
			RequiredAcks: kafka.RequireAll,      //this ensures that kafka confirms all in sync replies the relay should mark an outbox row published after Kafka confirms durability
			Async:        false,
		},
		logger: logger,
	}
}

func (p *KafkaPublisher) Publish(ctx context.Context, event outbox.Event) error {
	msg := kafka.Message{
		Topic: eventTopic(event.EventType),
		Key:   []byte(event.AggregateID.String()),
		Value: event.Payload,
		Time:  time.Now().UTC(),
		Headers: []kafka.Header{
			{Key: "event_type", Value: []byte(event.EventType)},
			{Key: "event_id", Value: []byte(strconv.FormatInt(event.ID, 10))},
		},
	}
	if err := p.writer.WriteMessages(ctx, msg); err != nil {
		return err
	}
	p.logger.Info("kafka event published",
		slog.Int64("event_id", event.ID),
		slog.String("event_type", event.EventType),
		slog.String("topic", msg.Topic),
	)
	return nil
}

func (p *KafkaPublisher) Close() error {
	return p.writer.Close()
}

func eventTopic(eventType string) string {
	switch eventType {
	case "order.created.v1":
		return "order.created"
	default:
		return eventType
	}
}
