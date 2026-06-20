package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"

	"github.com/segmentio/kafka-go"

	"gopay/order-service/internal/entity"
	"gopay/order-service/internal/usecase"
)

type PaymentSettledConsumer struct {
	reader              *kafka.Reader
	logger              *slog.Logger
	confirmOrderUsecase *usecase.ConfirmOrderFromPaymentUsecase
}

func NewPaymentSettledConsumer(
	brokers []string,
	topic string,
	groupID string,
	confirmOrderUsecase *usecase.ConfirmOrderFromPaymentUsecase,
	logger *slog.Logger,
) *PaymentSettledConsumer {
	return &PaymentSettledConsumer{
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers:        brokers,
			Topic:          topic,
			GroupID:        groupID,
			StartOffset:    kafka.FirstOffset,
			CommitInterval: 0,
			MinBytes:       1,
			MaxBytes:       10e6,
		}),
		logger:              logger,
		confirmOrderUsecase: confirmOrderUsecase,
	}
}

func (c *PaymentSettledConsumer) Run(ctx context.Context) {
	c.logger.Info("payment.settled consumer started")

	for {
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				c.logger.Info("payment.settled consumer stopped")
				return
			}
			c.logger.Error("fetch payment.settled message", slog.String("error", err.Error()))
			continue
		}
		if err := c.handleMessage(ctx, msg); err != nil {
			c.logger.Error("handel payment.settled message",
				slog.String("error", err.Error()),
				slog.Int64("offset", msg.Offset),
				slog.Int("partition", msg.Partition),
			)
			continue
		}

		if err := c.reader.CommitMessages(ctx, msg); err != nil {
			c.logger.Error("commit payment.settled offset",
				slog.String("error", err.Error()),
				slog.Int64("offset", msg.Offset),
				slog.Int("partition", msg.Partition),
			)
			continue

		}
		c.logger.Info("payment.settled offset committed",
			slog.Int64("offset", msg.Offset),
			slog.Int("partition", msg.Partition),
		)
	}

}

func (c *PaymentSettledConsumer) handleMessage(ctx context.Context, msg kafka.Message) error {
	var payload entity.PaymentSettledPayload

	if err := json.Unmarshal(msg.Value, &payload); err != nil {
		return err
	}

	result, err := c.confirmOrderUsecase.Execute(ctx, usecase.ConfirmOrderFromPaymentInput{
		OrderID:   payload.OrderID,
		PaymentID: payload.PaymentID,
	})
	if err != nil {
		return err
	}

	c.logger.Info("payment.settled processed",
		slog.String("event_type", headerValue(msg.Headers, "event_type")),
		slog.String("event_id", headerValue(msg.Headers, "event_id")),
		slog.String("order_id", payload.OrderID.String()),
		slog.String("payment_id", payload.PaymentID.String()),
		slog.Bool("order_updated", result.Updated),
		slog.Int("partition", msg.Partition),
		slog.Int64("offset", msg.Offset),
	)

	return nil
}

func (c *PaymentSettledConsumer) Close() error {
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
