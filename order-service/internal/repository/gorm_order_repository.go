package repository

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"gorm.io/gorm"

	"gopay/order-service/internal/entity"
	"gopay/pkg/apperrors"
)

type GormOrderRepository struct {
	db *gorm.DB
}

func NewGormOrderRepository(db *gorm.DB) *GormOrderRepository {
	return &GormOrderRepository{db: db}
}

type orderModel struct {
	ID         uuid.UUID       `gorm:"type:uuid;primaryKey"`
	CustomerID uuid.UUID       `gorm:"type:uuid;not null"`
	ProductID  uuid.UUID       `gorm:"type:uuid;not null"`
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

func (outboxEventModel) TableName() string {
	return "outbox_events"
}

func (orderModel) TableName() string {
	return "orders"
}

func (r *GormOrderRepository) Create(ctx context.Context, order entity.Order) error {
	orderModel := toOrderModel(order)
	payload, err := json.Marshal(entity.NewOrderCreatedPayload(order))
	if err != nil {
		return fmt.Errorf("marshal order created payload: %v: %w", err, apperrors.ErrInternal)
	}
	outboxModel := outboxEventModel{
		AggregateID: order.ID,
		EventType:   entity.OrderCreatedEventType,
		Payload:     payload,
		Published:   false,
		CreatedAt:   time.Now().UTC(),
	}
	err = r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Create(&orderModel).Error; err != nil {
			return fmt.Errorf("insert order: %v: %w", err, apperrors.ErrInternal)
		}
		if err := tx.Create(&outboxModel).Error; err != nil {
			return fmt.Errorf("insert outbox event: %v: %w", err, apperrors.ErrInternal)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("create order transaction: %w", err)
	}
	return nil
}

// !Mark confirmed comes form payment service and confirms that the payment was succesful
func (r *GormOrderRepository) MarkConfirmed(ctx context.Context, orderID uuid.UUID) (bool, error) {
	result := r.db.WithContext(ctx).
		Model(&orderModel{}).
		Where("id = ? AND status = ?", orderID, entity.OrderStatusPending).
		Updates(map[string]any{
			"status":     string(entity.OrderStatusConfirmed),
			"version":    gorm.Expr("version + 1"),
			"updated_at": time.Now().UTC(),
		})
	if result.Error != nil {
		return false, fmt.Errorf("confirm order: %v: %w", result.Error, apperrors.ErrInternal)
	}
	if result.RowsAffected == 1 {
		return true, nil
	}

	var existing orderModel
	err := r.db.WithContext(ctx).
		Select("status").
		Where("id = ?", orderID).
		First(&existing).Error

	if errors.Is(err, gorm.ErrRecordNotFound) {
		return false, fmt.Errorf("order not found: %w", apperrors.ErrNotFound)
	}

	if err != nil {
		return false, fmt.Errorf("load order status %v: %w", err, apperrors.ErrInternal)
	}

	if existing.Status == string(entity.OrderStatusConfirmed) {
		return false, nil
	}

	return false, fmt.Errorf("order cannot be confirmed from status %s: %w", existing.Status, apperrors.ErrConflict)

}

func (r *GormOrderRepository) MarkCancelled(ctx context.Context, orderID uuid.UUID) (bool, error) {
	result := r.db.WithContext(ctx).
		Model(&orderModel{}).
		Where("id = ? AND status = ?", orderID, entity.OrderStatusPending).
		Updates(map[string]any{
			"status":     string(entity.OrderStatusCancelled),
			"version":    gorm.Expr("version + 1"),
			"updated_at": time.Now().UTC(),
		})

	if result.Error != nil {
		return false, fmt.Errorf("cancel order: %v: %w", result.Error, apperrors.ErrInternal)
	}

	if result.RowsAffected == 1 {
		return true, nil
	}

	var existing orderModel
	err := r.db.WithContext(ctx).
		Select("status").
		Where("id = ?", orderID).
		First(&existing).Error

	if errors.Is(err, gorm.ErrRecordNotFound) {
		return false, fmt.Errorf("order not found: %w", apperrors.ErrNotFound)
	}

	if err != nil {
		return false, fmt.Errorf("load order status: %v: %w", err, apperrors.ErrInternal)
	}

	if existing.Status == string(entity.OrderStatusCancelled) {
		return false, nil
	}

	return false, fmt.Errorf("order cannot be cancelled from status %s: %w", existing.Status, apperrors.ErrConflict)
}

func toOrderModel(order entity.Order) orderModel {
	return orderModel{
		ID:         order.ID,
		CustomerID: order.CustomerID,
		ProductID:  order.ProductID,
		Amount:     order.Amount,
		Currency:   order.Currency,
		Status:     string(order.Status),
		Version:    order.Version,
		CreatedAt:  order.CreatedAt,
		UpdatedAt:  order.UpdatedAt,
	}
}
