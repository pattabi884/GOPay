package repository

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"gopay/audit-service/internal/entity"
	"gopay/pkg/apperrors"
)

type GormAuditRepository struct {
	db *gorm.DB
}

func NewGormAuditRepository(db *gorm.DB) *GormAuditRepository {
	return &GormAuditRepository{db: db}
}

type auditEventModel struct {
	ID          int64           `gorm:"primaryKey"`
	EventID     int64           `gorm:"not null"`
	EventType   string          `gorm:"type:varchar(100);not null"`
	AggregateID uuid.UUID       `gorm:"type:uuid;not null"`
	Topic       string          `gorm:"type:varchar(100);not null"`
	Payload     json.RawMessage `gorm:"type:jsonb;not null"`
	RecordedAt  time.Time       `gorm:"not null"`
}

func (auditEventModel) TableName() string {
	return "audit_events"
}

func (r *GormAuditRepository) Append(ctx context.Context, event entity.AuditEvent) (bool, error) {
	model := auditEventModel{
		EventID:     event.EventID,
		EventType:   event.EventType,
		AggregateID: event.AggregateID,
		Topic:       event.Topic,
		Payload:     event.Payload,
		RecordedAt:  event.RecordedAt,
	}

	result := r.db.WithContext(ctx).
		Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "event_type"}, {Name: "event_id"}},
			DoNothing: true,
		}).
		Create(&model)

	if result.Error != nil {
		return false, fmt.Errorf("insert audit event: %v: %w", result.Error, apperrors.ErrInternal)
	}

	return result.RowsAffected == 1, nil
}
