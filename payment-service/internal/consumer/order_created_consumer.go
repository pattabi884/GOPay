package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
	"github.com/shopspring/decimal"

	"gopay/payment-service/internal/usecase"
)

type OrderCreatedPayload struct {
	OrderID    uuid.UUID       `json:"order_id"`
	CustomerID uuid.UUID       `json:"customer_id"`
	ProductID  uuid.UUID       `json:"product_id"`
	Amount     decimal.Decimal `json:"amount"`
	Currency   string          `json:"currency"`
	Status     string          `json:"status"`
	CreatedAt  time.Time       `json:"created_at"`
}

type OrderCreatedConsumer struct {
	reader               *kafka.Reader
	logger               *slog.Logger
	createPaymentUsecase *usecase.CreatePaymentFromOrderUsecase
}

func NewOrderCreatedConsumer(
	brokers []string,
	topic string,
	groupID string,
	createPaymentUsecase *usecase.CreatePaymentFromOrderUsecase,
	logger *slog.Logger,
) *OrderCreatedConsumer {
	return &OrderCreatedConsumer{
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers:        brokers,
			Topic:          topic,
			GroupID:        groupID,
			StartOffset:    kafka.FirstOffset,
			CommitInterval: 0,
			MinBytes:       1,
			MaxBytes:       10e6,
		}),
		logger:               logger,
		createPaymentUsecase: createPaymentUsecase,
	}
}

func (c *OrderCreatedConsumer) Run(ctx context.Context) {
	c.logger.Info("order.created consumer started")

	for {
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				c.logger.Info("order.created consumer stopped")
				return
			}

			c.logger.Error("fetch order.created message", slog.String("error", err.Error()))
			continue
		}

		if err := c.handleMessage(ctx, msg); err != nil {
			c.logger.Error("handle order.created message",
				slog.String("error", err.Error()),
				slog.Int64("offset", msg.Offset),
				slog.Int("partition", msg.Partition),
			)
			continue
		}

		if err := c.reader.CommitMessages(ctx, msg); err != nil {
			c.logger.Error("commit order.created offset",
				slog.String("error", err.Error()),
				slog.Int64("offset", msg.Offset),
				slog.Int("partition", msg.Partition),
			)
			continue
		}

		c.logger.Info("order.created offset committed",
			slog.Int64("offset", msg.Offset),
			slog.Int("partition", msg.Partition),
		)
	}
}

func (c *OrderCreatedConsumer) handleMessage(ctx context.Context, msg kafka.Message) error {
	var payload OrderCreatedPayload

	if err := json.Unmarshal(msg.Value, &payload); err != nil {
		return err
	}

	result, err := c.createPaymentUsecase.Execute(ctx, usecase.CreatePaymentFromOrderInput{
		OrderID:    payload.OrderID,
		CustomerID: payload.CustomerID,
		Amount:     payload.Amount,
		Currency:   payload.Currency,
	})
	if err != nil {
		return err
	}

	c.logger.Info("order.created processed",
		slog.String("event_type", headerValue(msg.Headers, "event_type")),
		slog.String("event_id", headerValue(msg.Headers, "event_id")),
		slog.String("order_id", payload.OrderID.String()),
		slog.String("customer_id", payload.CustomerID.String()),
		slog.String("amount", payload.Amount.String()),
		slog.String("currency", payload.Currency),
		slog.Bool("payment_created", result.Created),
		slog.Int("partition", msg.Partition),
		slog.Int64("offset", msg.Offset),
	)

	return nil
}

func (c *OrderCreatedConsumer) Close() error {
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
