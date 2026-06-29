package entity

import (
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
)

const (
	PaymentSettledEventType = "payment.settled.v1"
	PaymentFailedEventType  = "payment.failed.v1"
)

type PaymentSettledPayload struct {
	PaymentID  uuid.UUID       `json:"payment_id"`
	OrderID    uuid.UUID       `json:"order_id"`
	CustomerID uuid.UUID       `json:"customer_id"`
	Amount     decimal.Decimal `json:"amount"`
	Currency   string          `json:"currency"`
	Status     PaymentStatus   `json:"status"`
	SettledAt  time.Time       `json:"settled_at"`
}

type PaymentFailedPayload struct {
	PaymentID  uuid.UUID       `json:"payment_id"`
	OrderID    uuid.UUID       `json:"order_id"`
	CustomerID uuid.UUID       `json:"customer_id"`
	Amount     decimal.Decimal `json:"amount"`
	Currency   string          `json:"currency"`
	Status     PaymentStatus   `json:"status"`
	Reason     string          `json:"reason"`
	FailedAt   time.Time       `json:"failed_at"`
}

func NewPaymentSettledPayload(payment Payment) PaymentSettledPayload {
	return PaymentSettledPayload{
		PaymentID:  payment.ID,
		OrderID:    payment.OrderID,
		CustomerID: payment.CustomerID,
		Amount:     payment.Amount,
		Currency:   payment.Currency,
		Status:     payment.Status,
		SettledAt:  payment.UpdatedAt,
	}
}

func NewPaymentFailedPayload(payment Payment, reason string) PaymentFailedPayload {
	return PaymentFailedPayload{
		PaymentID:  payment.ID,
		OrderID:    payment.OrderID,
		CustomerID: payment.CustomerID,
		Amount:     payment.Amount,
		Currency:   payment.Currency,
		Status:     payment.Status,
		Reason:     reason,
		FailedAt:   payment.UpdatedAt,
	}
}
