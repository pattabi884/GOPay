package usecase

import (
	"context"
	"fmt"
	"strings"

	"github.com/shopspring/decimal"

	"github.com/google/uuid"

	"gopay/order-service/internal/entity"
	"gopay/pkg/apperrors"
)

type CreateOrderInput struct {
	CustomerID string
	ProductID  string
	Amount     decimal.Decimal
	Currency   string
}

type OrderRepository interface {
	Create(ctx context.Context, order entity.Order) error
}

type CreateOrderUsecase struct {
	orderRepo OrderRepository
}

func NewCreateOrderUsecase(orderRepo OrderRepository) *CreateOrderUsecase {
	return &CreateOrderUsecase{
		orderRepo: orderRepo,
	}
}

func (u *CreateOrderUsecase) Execute(ctx context.Context, input CreateOrderInput) (entity.Order, error) {
	customerID, err := uuid.Parse(input.CustomerID)
	if err != nil {
		return entity.Order{}, fmt.Errorf("parse customer_id: %w", apperrors.ErrInvalidInput)
	}

	productID, err := uuid.Parse(input.ProductID)
	if err != nil {
		return entity.Order{}, fmt.Errorf("parse product_id: %w", apperrors.ErrInvalidInput)
	}

	if !isValidAmount(input.Amount) {
		return entity.Order{}, fmt.Errorf("validate amount: %w", apperrors.ErrInvalidInput)
	}

	currency := strings.ToUpper(strings.TrimSpace(input.Currency))
	if len(currency) != 3 {
		return entity.Order{}, fmt.Errorf("validate currency: %w", apperrors.ErrInvalidInput)
	}

	order := entity.NewOrder(customerID, productID, input.Amount, currency)

	if err := u.orderRepo.Create(ctx, order); err != nil {
		return entity.Order{}, fmt.Errorf("create order: %w", err)
	}

	return order, nil

}

func isValidAmount(amount decimal.Decimal) bool {
	if !amount.GreaterThan(decimal.Zero) {
		return false
	}

	return amount.Exponent() >= -2
}
