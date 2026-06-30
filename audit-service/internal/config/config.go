package config

import (
	"os"
	"strings"
)

type Config struct {
	AppEnv                string
	PostgresDSN           string
	KafkaBrokers          string
	OrderCreatedTopic     string
	PaymentSettledTopic   string
	PaymentFailedTopic    string
	OrderCreatedGroupID   string
	PaymentSettledGroupID string
	PaymentFailedGroupID  string
}

func Load() Config {
	return Config{
		AppEnv:                getEnv("APP_ENV", "development"),
		PostgresDSN:           getEnv("POSTGRES_AUDIT_DSN", "postgres://gopay:gopay@127.0.0.1:5434/gopay_audit?sslmode=disable"),
		KafkaBrokers:          getEnv("KAFKA_BROKERS", "127.0.0.1:9092"),
		OrderCreatedTopic:     getEnv("ORDER_CREATED_TOPIC", "order.created"),
		PaymentSettledTopic:   getEnv("PAYMENT_SETTLED_TOPIC", "payment.settled"),
		PaymentFailedTopic:    getEnv("PAYMENT_FAILED_TOPIC", "payment.failed"),
		OrderCreatedGroupID:   getEnv("AUDIT_ORDER_CREATED_GROUP_ID", "audit-service-order-created-v1"),
		PaymentSettledGroupID: getEnv("AUDIT_PAYMENT_SETTLED_GROUP_ID", "audit-service-payment-settled-v1"),
		PaymentFailedGroupID:  getEnv("AUDIT_PAYMENT_FAILED_GROUP_ID", "audit-service-payment-failed-v1"),
	}
}

func (c Config) KafkaBrokersList() []string {
	parts := strings.Split(c.KafkaBrokers, ",")
	brokers := make([]string, 0, len(parts))
	for _, part := range parts {
		broker := strings.TrimSpace(part)
		if broker != "" {
			brokers = append(brokers, broker)
		}
	}
	return brokers
}

func getEnv(key, fallback string) string {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	return value
}
