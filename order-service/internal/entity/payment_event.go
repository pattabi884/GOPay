package entity

import (
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
)

const PaymentSettledEventType = "payment.settled.v1"

type PaymentSettledPayload struct {
	PaymentID  uuid.UUID       `json:"payment_id"`
	OrderID    uuid.UUID       `json:"order_id"`
	CustomerID uuid.UUID       `json:"customer_id"`
	Amount     decimal.Decimal `json:"amount"`
	Currency   string          `json:"currency"`
	Status     string          `json:"status"`
	SettledAt  time.Time       `json:"settled_at"`
}
