package repository

import (
	"context"
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

func (paymentModel) TableName() string {
	return "payments"
}

func (r *GormPaymentRepository) CreateIfNotExists(ctx context.Context, payment entity.Payment) (bool, error) {

	model := toPaymentModel(payment)
	result := r.db.WithContext(ctx).
		Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "order_id"}},
			DoNothing: true,
		}).
		Create(&model)
	if result.Error != nil {
		return false, fmt.Errorf("insert payment: %v: %w", result.Error, apperrors.ErrInternal)
	}
	return result.RowsAffected == 1, nil

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
