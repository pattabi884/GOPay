package consumer

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"

	"gopay/audit-service/internal/usecase"
)

type AuditConsumer struct {
	reader             *kafka.Reader
	topic              string
	logger             *slog.Logger
	recordEventUsecase *usecase.RecordEventUsecase
}

func NewAuditConsumer(
	brokers []string,
	topic string,
	groupID string,
	recordEventUsecase *usecase.RecordEventUsecase,
	logger *slog.Logger,
) *AuditConsumer {
	return &AuditConsumer{
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers:        brokers,
			Topic:          topic,
			GroupID:        groupID,
			StartOffset:    kafka.FirstOffset,
			CommitInterval: 0,
			MinBytes:       1,
			MaxBytes:       10e6,
		}),
		topic:              topic,
		logger:             logger,
		recordEventUsecase: recordEventUsecase,
	}
}

func (c *AuditConsumer) Run(ctx context.Context) {
	c.logger.Info("audit consumer started", slog.String("topic", c.topic))

	for {
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				c.logger.Info("audit consumer stopped", slog.String("topic", c.topic))
				return
			}
			c.logger.Error("fetch audit message",
				slog.String("topic", c.topic),
				slog.String("error", err.Error()),
			)
			continue
		}

		if err := c.handleMessage(ctx, msg); err != nil {
			c.logger.Error("handle audit message",
				slog.String("topic", msg.Topic),
				slog.String("error", err.Error()),
				slog.Int64("offset", msg.Offset),
				slog.Int("partition", msg.Partition),
			)
			continue
		}

		if err := c.reader.CommitMessages(ctx, msg); err != nil {
			c.logger.Error("commit audit offset",
				slog.String("topic", msg.Topic),
				slog.String("error", err.Error()),
				slog.Int64("offset", msg.Offset),
				slog.Int("partition", msg.Partition),
			)
			continue
		}

		c.logger.Info("audit offset committed",
			slog.String("topic", msg.Topic),
			slog.Int64("offset", msg.Offset),
			slog.Int("partition", msg.Partition),
		)
	}
}

func (c *AuditConsumer) handleMessage(ctx context.Context, msg kafka.Message) error {
	eventType := headerValue(msg.Headers, "event_type")

	eventID, err := strconv.ParseInt(headerValue(msg.Headers, "event_id"), 10, 64)
	if err != nil {
		return fmt.Errorf("parse event_id header: %w", err)
	}

	aggregateID, err := uuid.Parse(string(msg.Key))
	if err != nil {
		return fmt.Errorf("parse aggregate_id key: %w", err)
	}

	result, err := c.recordEventUsecase.Execute(ctx, usecase.RecordEventInput{
		EventID:     eventID,
		EventType:   eventType,
		Topic:       msg.Topic,
		AggregateID: aggregateID,
		Payload:     msg.Value,
	})
	if err != nil {
		return err
	}

	c.logger.Info("audit event processed",
		slog.String("event_type", eventType),
		slog.Int64("event_id", eventID),
		slog.String("aggregate_id", aggregateID.String()),
		slog.String("topic", msg.Topic),
		slog.Bool("recorded", result.Recorded),
		slog.Int("partition", msg.Partition),
		slog.Int64("offset", msg.Offset),
	)

	return nil
}

func (c *AuditConsumer) Close() error {
	return c.reader.Close()
}

func headerValue(headers []kafka.Header, key string) string {
	for _, header := range headers {
		if header.Key == key {
			return string(header.Value)
		}
	}
	return ""
}
