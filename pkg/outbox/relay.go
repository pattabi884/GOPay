package outbox

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"gorm.io/gorm"
)

type Event struct {
	ID          int64
	AggregateID uuid.UUID
	EventType   string
	Payload     json.RawMessage
	CreatedAt   time.Time
}

type Publisher interface {
	Publish(ctx context.Context, event Event) error
}

type RelayConfig struct {
	ServiceName  string
	DB           *gorm.DB
	Redis        *redis.Client
	Publisher    Publisher
	Logger       *slog.Logger
	PollInterval time.Duration
	BatchSize    int
}

type Relay struct {
	serviceName  string
	db           *gorm.DB
	redis        *redis.Client
	publisher    Publisher
	logger       *slog.Logger
	pollInterval time.Duration
	batchSize    int
}

type outboxEventModel struct {
	ID          int64           `gorm:"primaryKey"`
	AggregateID uuid.UUID       `gorm:"type:uuid;not null"`
	EventType   string          `gorm:"type:varchar(100);not null"`
	Payload     json.RawMessage `gorm:"type:jsonb;not null"`
	Published   bool            `gorm:"not null"`
	CreatedAt   time.Time       `gorm:"not null"`
}

func (outboxEventModel) TableName() string {
	return "outbox_events"
}

func NewRelay(cfg RelayConfig) *Relay {
	if cfg.PollInterval == 0 {
		cfg.PollInterval = 500 * time.Millisecond
	}
	if cfg.BatchSize == 0 {
		cfg.BatchSize = 100
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	return &Relay{
		serviceName:  cfg.ServiceName,
		db:           cfg.DB,
		redis:        cfg.Redis,
		publisher:    cfg.Publisher,
		logger:       cfg.Logger,
		pollInterval: cfg.PollInterval,
		batchSize:    cfg.BatchSize,
	}
}

func (r *Relay) Run(ctx context.Context) {
	ticker := time.NewTicker(r.pollInterval)
	defer ticker.Stop()

	r.logger.Info("outbox relay started", slog.String("service", r.serviceName))

	for {
		if err := r.publishBatch(ctx); err != nil {
			r.logger.Error("outbox relay batch failed", slog.String("error", err.Error()))
		}

		select {
		case <-ctx.Done():
			r.logger.Info("outbox relay stopped", slog.String("service", r.serviceName))
			return
		case <-ticker.C:
		}
	}
}

func (r *Relay) publishBatch(ctx context.Context) error {
	cursor, err := r.loadCursor(ctx)
	if err != nil {
		return fmt.Errorf("load cursor: %w", err)
	}

	var rows []outboxEventModel
	if err := r.db.WithContext(ctx).
		Where("id > ? AND published = ?", cursor, false).
		Order("id ASC").
		Limit(r.batchSize).
		Find(&rows).Error; err != nil {
		return fmt.Errorf("query outbox events: %w", err)
	}

	for _, row := range rows {
		event := Event{
			ID:          row.ID,
			AggregateID: row.AggregateID,
			EventType:   row.EventType,
			Payload:     row.Payload,
			CreatedAt:   row.CreatedAt,
		}

		if err := r.publisher.Publish(ctx, event); err != nil {
			return fmt.Errorf("publish event %d: %w", row.ID, err)
		}
		if err := r.markPublished(ctx, row.ID); err != nil {
			return fmt.Errorf("mark event %d published: %w", row.ID, err)
		}
		if err := r.saveCursor(ctx, row.ID); err != nil {
			return fmt.Errorf("save cursor %d: %w", row.ID, err)
		}
	}

	return nil
}

func (r *Relay) cursorKey() string {
	return fmt.Sprintf("outbox:cursor:%s", r.serviceName)
}

func (r *Relay) loadCursor(ctx context.Context) (int64, error) {
	value, err := r.redis.Get(ctx, r.cursorKey()).Result()
	if errors.Is(err, redis.Nil) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return strconv.ParseInt(value, 10, 64)
}

func (r *Relay) saveCursor(ctx context.Context, id int64) error {
	return r.redis.Set(ctx, r.cursorKey(), strconv.FormatInt(id, 10), 0).Err()
}

func (r *Relay) markPublished(ctx context.Context, id int64) error {
	return r.db.WithContext(ctx).
		Model(&outboxEventModel{}).
		Where("id = ? AND published = ?", id, false).
		Update("published", true).Error
}
