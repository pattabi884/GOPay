package entity

import (
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
)

const OrderCreatedEventType = "order.created.v1"

type OrderCreatedPayload struct {
	OrderID    uuid.UUID       `json:"order_id"`
	CustomerID uuid.UUID       `json:"customer_id"`
	ProductID  uuid.UUID       `json:"product_id"`
	Amount     decimal.Decimal `json:"amount"`
	Currency   string          `json:"currency"`
	Status     OrderStatus     `json:"status"`
	CreatedAt  time.Time       `json:"created_at"`
}

func NewOrderCreatedPayload(order Order) OrderCreatedPayload {
	return OrderCreatedPayload{
		OrderID:    order.ID,
		CustomerID: order.CustomerID,
		ProductID:  order.ProductID,
		Amount:     order.Amount,
		Currency:   order.Currency,
		Status:     order.Status,
		CreatedAt:  order.CreatedAt,
	}
}
