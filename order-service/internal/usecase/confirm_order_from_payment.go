package usecase

import (
	"context"
	"fmt"

	"github.com/google/uuid"

	"gopay/pkg/apperrors"
)

type ConfirmOrderRepository interface {
	MarkConfirmed(ctx context.Context, orderID uuid.UUID) (bool, error)
}

type ConfirmOrderFromPaymentInput struct {
	OrderID   uuid.UUID
	PaymentID uuid.UUID
}

type ConfirmOrderFromPaymentResult struct {
	OrderID uuid.UUID
	Updated bool
}

type ConfirmOrderFromPaymentUsecase struct {
	orderRepo ConfirmOrderRepository
}

func NewConfirmOrderFromPaymentUseCase(orderRepo ConfirmOrderRepository) *ConfirmOrderFromPaymentUsecase {
	return &ConfirmOrderFromPaymentUsecase{orderRepo: orderRepo}
}

func (u *ConfirmOrderFromPaymentUsecase) Execute(
	ctx context.Context,
	input ConfirmOrderFromPaymentInput,
) (ConfirmOrderFromPaymentResult, error) {
	if input.OrderID == uuid.Nil || input.PaymentID == uuid.Nil {
		return ConfirmOrderFromPaymentResult{}, fmt.Errorf("vaildate payment settled event: %w", apperrors.ErrInvalidInput)
	}
	updated, err := u.orderRepo.MarkConfirmed(ctx, input.OrderID)
	if err != nil {
		return ConfirmOrderFromPaymentResult{}, fmt.Errorf("confirm order form payment: %w", err)
	}
	return ConfirmOrderFromPaymentResult{
		OrderID: input.OrderID,
		Updated: updated,
	}, nil

}
