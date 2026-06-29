package usecase

import (
	"context"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"

	"gopay/payment-service/internal/entity"
	"gopay/pkg/apperrors"
)

type PaymentRepository interface {
	CreateSettledIfNotExists(ctx context.Context, payment entity.Payment) (bool, error)
	CreateFailedIfNotExists(ctx context.Context, payment entity.Payment, reason string) (bool, error)
}

type CreatePaymentFromOrderInput struct {
	OrderID    uuid.UUID
	CustomerID uuid.UUID
	Amount     decimal.Decimal
	Currency   string
}

type CreatePaymentFromOrderResult struct {
	OrderID uuid.UUID
	Created bool
}

type CreatePaymentFromOrderUsecase struct {
	paymentRepo PaymentRepository
	mockOutcome string
}

func NewCreatePaymentFromOrderUsecase(paymentRepo PaymentRepository, mockOutcome string) *CreatePaymentFromOrderUsecase {
	return &CreatePaymentFromOrderUsecase{
		paymentRepo: paymentRepo,
		mockOutcome: strings.ToLower(strings.TrimSpace(mockOutcome)),
	}
}

func (u *CreatePaymentFromOrderUsecase) Execute(
	ctx context.Context,
	input CreatePaymentFromOrderInput,
) (CreatePaymentFromOrderResult, error) {
	if !input.Amount.GreaterThan(decimal.Zero) {
		return CreatePaymentFromOrderResult{}, fmt.Errorf("validate amount: %w", apperrors.ErrInvalidInput)
	}

	currency := strings.ToUpper(strings.TrimSpace(input.Currency))
	if len(currency) != 3 {
		return CreatePaymentFromOrderResult{}, fmt.Errorf("validate currency: %w", apperrors.ErrInvalidInput)
	}

	payment := entity.NewPaymentFromOrder(
		input.OrderID,
		input.CustomerID,
		input.Amount,
		currency,
	)

	var created bool
	var err error

	switch u.mockOutcome {
	case "failed":
		created, err = u.paymentRepo.CreateFailedIfNotExists(ctx, payment, "mock payment failure")
	default:
		created, err = u.paymentRepo.CreateSettledIfNotExists(ctx, payment)
	}

	if err != nil {
		return CreatePaymentFromOrderResult{}, fmt.Errorf("create payment from order: %w", err)
	}

	return CreatePaymentFromOrderResult{
		OrderID: input.OrderID,
		Created: created,
	}, nil
}
