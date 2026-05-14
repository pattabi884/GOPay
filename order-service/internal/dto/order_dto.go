package dto

import "github.com/shopspring/decimal"

type CreateOrderRequest struct {
	CustomerID string          `json:"customer_id" binding:"required"`
	ProductID  string          `json:"product_id" binding:"required"`
	Amount     decimal.Decimal `json:"amount" binding:"required"`
	Currency   string          `json:"currency" binding:"required,len=3"`
}

type OrderResponse struct {
	ID         string          `json:"id"`
	CustomerID string          `json:"customer_id"`
	ProductID  string          `json:"product_id"`
	Amount     decimal.Decimal `json:"amount"`
	Currency   string          `json:"currency"`
	Status     string          `json:"status"`
	Version    int             `json:"version"`
	CreatedAt  string          `json:"created_at"`
	UpdatedAt  string          `json:"updated_at"`
}
