package repository

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"gopay/payment-service/internal/entity"
	"gopay/pkg/apperrors"
)

type GormPaymentRepository struct {
	db *gorm.DB
}

func NewGormPaymentRepository(db *gorm.DB) *GormPaymentRepository {
	return &GormPaymentRepository{db: db}
}

type paymentModel struct {
	ID         uuid.UUID       `gorm:"type:uuid;primaryKey"`
	OrderID    uuid.UUID       `gorm:"type:uuid;not null;uniqueIndex"`
	CustomerID uuid.UUID       `gorm:"type:uuid;not null"`
	Amount     decimal.Decimal `gorm:"type:numeric(12,2);not null"`
	Currency   string          `gorm:"type:varchar(3);not null"`
	Status     string          `gorm:"type:varchar(20);not null"`
	Version    int             `gorm:"not null"`
	CreatedAt  time.Time       `gorm:"not null"`
	UpdatedAt  time.Time       `gorm:"not null"`
}

type outboxEventModel struct {
	ID          int64     `gorm:"primaryKey"`
	AggregateID uuid.UUID `gorm:"type:uuid;not null"`
	EventType   string    `gorm:"type:varchar(100);not null"`
	Payload     []byte    `gorm:"type:jsonb;not null"`
	Published   bool      `gorm:"not null;default:false"`
	CreatedAt   time.Time `gorm:"not null"`
}

func (paymentModel) TableName() string {
	return "payments"
}

func (outboxEventModel) TableName() string {
	return "outbox_events"
}

func (r *GormPaymentRepository) CreateSettledIfNotExists(ctx context.Context, payment entity.Payment) (bool, error) {
	payment.MarkCompleted()

	model := toPaymentModel(payment)

	payload, err := json.Marshal(entity.NewPaymentSettledPayload(payment))
	if err != nil {
		return false, fmt.Errorf("marshal payment settled payload: %v: %w", err, apperrors.ErrInternal)
	}

	outboxModel := outboxEventModel{
		AggregateID: payment.OrderID,
		EventType:   entity.PaymentSettledEventType,
		Payload:     payload,
		Published:   false,
		CreatedAt:   time.Now().UTC(),
	}
	created := false

	err = r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		result := tx.Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "order_id"}},
			DoNothing: true,
		}).Create(&model)

		if result.Error != nil {
			return fmt.Errorf("insert payment: %v: %w", result.Error, apperrors.ErrInternal)
		}

		if result.RowsAffected == 0 {
			return nil
		}
		created = true

		if err := tx.Create(&outboxModel).Error; err != nil {
			return fmt.Errorf("insert payment outbox event: %v: %w", err, apperrors.ErrInternal)
		}

		return nil
	})
	if err != nil {
		return false, fmt.Errorf("create settled payment transaction: %w", err)
	}

	return created, nil
}

func (r *GormPaymentRepository) CreateFailedIfNotExists(ctx context.Context, payment entity.Payment, reason string) (bool, error) {
	payment.MarkFailed()

	model := toPaymentModel(payment)

	payload, err := json.Marshal(entity.NewPaymentFailedPayload(payment, reason))
	if err != nil {
		return false, fmt.Errorf("marshal payment failed payload: %v: %w", err, apperrors.ErrInternal)
	}

	outboxModel := outboxEventModel{
		AggregateID: payment.OrderID,
		EventType:   entity.PaymentFailedEventType,
		Payload:     payload,
		Published:   false,
		CreatedAt:   time.Now().UTC(),
	}

	created := false

	err = r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		result := tx.Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "order_id"}},
			DoNothing: true,
		}).Create(&model)

		if result.Error != nil {
			return fmt.Errorf("insert failed payment: %v: %w", result.Error, apperrors.ErrInternal)
		}

		if result.RowsAffected == 0 {
			return nil
		}

		created = true

		if err := tx.Create(&outboxModel).Error; err != nil {
			return fmt.Errorf("insert payment failed outbox event: %v: %w", err, apperrors.ErrInternal)
		}

		return nil
	})
	if err != nil {
		return false, fmt.Errorf("create failed payment transaction: %w", err)
	}

	return created, nil
}

func toPaymentModel(payment entity.Payment) paymentModel {
	return paymentModel{
		ID:         payment.ID,
		OrderID:    payment.OrderID,
		CustomerID: payment.CustomerID,
		Amount:     payment.Amount,
		Currency:   payment.Currency,
		Status:     string(payment.Status),
		Version:    payment.Version,
		CreatedAt:  payment.CreatedAt,
		UpdatedAt:  payment.UpdatedAt,
	}
}
