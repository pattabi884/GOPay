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

type PaymentFailedConsumer struct {
	reader             *kafka.Reader
	logger             *slog.Logger
	cancelOrderUsecase *usecase.CancelOrderFromPaymentUsecase
}

func NewPaymentFailedConsumer(
	brokers []string,
	topic string,
	groupID string,
	cancelOrderUsecase *usecase.CancelOrderFromPaymentUsecase,
	logger *slog.Logger,
) *PaymentFailedConsumer {
	return &PaymentFailedConsumer{
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers:        brokers,
			Topic:          topic,
			GroupID:        groupID,
			StartOffset:    kafka.FirstOffset,
			CommitInterval: 0,
			MinBytes:       1,
			MaxBytes:       10e6,
		}),
		logger:             logger,
		cancelOrderUsecase: cancelOrderUsecase,
	}
}

func (c *PaymentFailedConsumer) Run(ctx context.Context) {
	c.logger.Info("payment.failed consumer started")

	for {
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				c.logger.Info("payment.failed consumer stopped")
				return
			}
			c.logger.Error("fetch payment.failed message", slog.String("error", err.Error()))
			continue
		}

		if err := c.handleMessage(ctx, msg); err != nil {
			c.logger.Error("handle payment.failed message",
				slog.String("error", err.Error()),
				slog.Int64("offset", msg.Offset),
				slog.Int("partition", msg.Partition),
			)
			continue
		}

		if err := c.reader.CommitMessages(ctx, msg); err != nil {
			c.logger.Error("commit payment.failed offset",
				slog.String("error", err.Error()),
				slog.Int64("offset", msg.Offset),
				slog.Int("partition", msg.Partition),
			)
			continue
		}

		c.logger.Info("payment.failed offset committed",
			slog.Int64("offset", msg.Offset),
			slog.Int("partition", msg.Partition),
		)
	}
}

func (c *PaymentFailedConsumer) handleMessage(ctx context.Context, msg kafka.Message) error {
	var payload entity.PaymentFailedPayload

	if err := json.Unmarshal(msg.Value, &payload); err != nil {
		return err
	}

	result, err := c.cancelOrderUsecase.Execute(ctx, usecase.CancelOrderFromPaymentInput{
		OrderID:   payload.OrderID,
		PaymentID: payload.PaymentID,
		Reason:    payload.Reason,
	})
	if err != nil {
		return err
	}

	c.logger.Info("payment.failed processed",
		slog.String("event_type", headerValue(msg.Headers, "event_type")),
		slog.String("event_id", headerValue(msg.Headers, "event_id")),
		slog.String("order_id", payload.OrderID.String()),
		slog.String("payment_id", payload.PaymentID.String()),
		slog.String("reason", payload.Reason),
		slog.Bool("order_updated", result.Updated),
		slog.Int("partition", msg.Partition),
		slog.Int64("offset", msg.Offset),
	)

	return nil
}

func (c *PaymentFailedConsumer) Close() error {
	return c.reader.Close()
}
