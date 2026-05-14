package provider

import (
	"context"
	"log/slog"

	"gopay/pkg/outbox"
)

type LogPublisher struct {
	logger *slog.Logger
}

func NewLogPublisher(logger *slog.Logger) *LogPublisher {
	return &LogPublisher{logger: logger}
}

func (p *LogPublisher) Publish(ctx context.Context, event outbox.Event) error {
	p.logger.Info("outbox event published",
		slog.Int64("event_id", event.ID),
		slog.String("event_type", event.EventType),
		slog.String("aggregate_id", event.AggregateID.String()),
	)

	return nil
}
