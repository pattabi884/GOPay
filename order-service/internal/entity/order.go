package entity

import (
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
)

type OrderStatus string

const (
	OrderStatusPending   OrderStatus = "pending"
	OrderStatusConfirmed OrderStatus = "confirmed"
	OrderStatusCancelled OrderStatus = "cancelled"
)

type Order struct {
	ID         uuid.UUID
	CustomerID uuid.UUID
	ProductID  uuid.UUID
	Amount     decimal.Decimal
	Currency   string
	Status     OrderStatus
	Version    int
	CreatedAt  time.Time
	UpdatedAt  time.Time
}

func NewOrder(customerID, productID uuid.UUID, amount decimal.Decimal, currency string) Order {
	now := time.Now().UTC()

	return Order{
		ID:         uuid.New(),
		CustomerID: customerID,
		ProductID:  productID,
		Amount:     amount,
		Currency:   currency,
		Status:     OrderStatusPending,
		Version:    1,
		CreatedAt:  now,
		UpdatedAt:  now,
	}
}
