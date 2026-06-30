package usecase

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"

	"gopay/audit-service/internal/entity"
	"gopay/pkg/apperrors"
)

type AuditRepository interface {
	Append(ctx context.Context, event entity.AuditEvent) (bool, error)
}

type RecordEventInput struct {
	EventID     int64
	EventType   string
	Topic       string
	AggregateID uuid.UUID
	Payload     json.RawMessage
}

type RecordEventResult struct {
	Recorded bool
}

type RecordEventUsecase struct {
	auditRepo AuditRepository
}

func NewRecordEventUsecase(auditRepo AuditRepository) *RecordEventUsecase {
	return &RecordEventUsecase{auditRepo: auditRepo}
}

func (u *RecordEventUsecase) Execute(ctx context.Context, input RecordEventInput) (RecordEventResult, error) {
	if input.EventType == "" || input.AggregateID == uuid.Nil {
		return RecordEventResult{}, fmt.Errorf("validate audit event: %w", apperrors.ErrInvalidInput)
	}

	event := entity.NewAuditEvent(
		input.EventID,
		input.EventType,
		input.Topic,
		input.AggregateID,
		input.Payload,
	)

	recorded, err := u.auditRepo.Append(ctx, event)
	if err != nil {
		return RecordEventResult{}, fmt.Errorf("record audit event: %w", err)
	}

	return RecordEventResult{Recorded: recorded}, nil
}
