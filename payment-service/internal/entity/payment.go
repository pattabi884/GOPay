package entity

import (
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
)

type PaymentStatus string

const (
	PaymentStatusProcessing PaymentStatus = "processing"
	PaymentStatusCompleted  PaymentStatus = "completed"
	PaymentStatusFailed     PaymentStatus = "failed"
)

type Payment struct {
	ID         uuid.UUID
	OrderID    uuid.UUID
	CustomerID uuid.UUID
	Amount     decimal.Decimal
	Currency   string
	Status     PaymentStatus
	Version    int
	CreatedAt  time.Time
	UpdatedAt  time.Time
}

func NewPaymentFromOrder(orderID, customerID uuid.UUID, amount decimal.Decimal, currency string) Payment {
	now := time.Now().UTC()

	return Payment{
		ID:         uuid.New(),
		OrderID:    orderID,
		CustomerID: customerID,
		Amount:     amount,
		Currency:   currency,
		Status:     PaymentStatusProcessing,
		Version:    1,
		CreatedAt:  now,
		UpdatedAt:  now,
	}
}
