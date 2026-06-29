package usecase

import (
	"context"
	"fmt"

	"github.com/google/uuid"

	"gopay/pkg/apperrors"
)

type CancelOrderRepository interface {
	MarkCancelled(ctx context.Context, orderID uuid.UUID) (bool, error)
}

type CancelOrderFromPaymentInput struct {
	OrderID   uuid.UUID
	PaymentID uuid.UUID
	Reason    string
}

type CancelOrderFromPaymentResult struct {
	OrderID uuid.UUID
	Updated bool
}

type CancelOrderFromPaymentUsecase struct {
	orderRepo CancelOrderRepository
}

func NewCancelOrderFromPaymentUsecase(orderRepo CancelOrderRepository) *CancelOrderFromPaymentUsecase {
	return &CancelOrderFromPaymentUsecase{orderRepo: orderRepo}
}

func (u *CancelOrderFromPaymentUsecase) Execute(
	ctx context.Context,
	input CancelOrderFromPaymentInput,
) (CancelOrderFromPaymentResult, error) {
	if input.OrderID == uuid.Nil || input.PaymentID == uuid.Nil {
		return CancelOrderFromPaymentResult{}, fmt.Errorf("validate payment failed event: %w", apperrors.ErrInvalidInput)
	}

	updated, err := u.orderRepo.MarkCancelled(ctx, input.OrderID)
	if err != nil {
		return CancelOrderFromPaymentResult{}, fmt.Errorf("cancel order from payment: %w", err)
	}

	return CancelOrderFromPaymentResult{
		OrderID: input.OrderID,
		Updated: updated,
	}, nil
}
