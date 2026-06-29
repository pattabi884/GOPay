package entity

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

type AuditEvent struct {
	EventID     int64
	EventType   string
	AggregateID uuid.UUID
	Topic       string
	Payload     json.RawMessage
	RecordedAt  time.Time
}

func NewAuditEvent(eventID int64, eventType, topic string, aggregateID uuid.UUID, payload json.RawMessage) AuditEvent {
	return AuditEvent{
		EventID:     eventID,
		EventType:   eventType,
		AggregateID: aggregateID,
		Topic:       topic,
		Payload:     payload,
		RecordedAt:  time.Now().UTC(),
	}
}
