package gateway

import (
	"context"
	"github.com/shopspring/decimal"
)

type CreateOrderResult struct {
	ProviderOrderID string
	Status string
}

type Gateway interface {
	CreateOrder (ctx context.Context, amount decimal.Decimal, currency, recipt string) (CreateOrderResult, error)

}