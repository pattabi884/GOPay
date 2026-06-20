package config

import (
	"fmt"
	"os"
)

type Config struct {
	AppEnv                string
	Port                  string
	PostgresDSN           string
	RedisAddr             string
	KafkaBrokers          string
	PaymentSettledTopic   string
	PaymentSettledGroupID string
}

// 1. migration
// 2. entity/payment.go
// 3. repository/gorm_payment_repository.go
// 4. usecase/create_payment_from_order.go
// 5. consumer update
// 6. config update
// 7. main.go wiring

func Load() Config {
	return Config{
		AppEnv:                getEnv("APP_ENV", "development"),
		Port:                  getEnv("ORDER_SERVICE_PORT", "8081"),
		PostgresDSN:           getEnv("POSTGRES_ORDER_DSN", "postgres://gopay:gopay@127.0.0.1:55432/gopay_order?sslmode=disable"),
		RedisAddr:             getEnv("REDIS_ADDR", "127.0.0.1:6379"),
		KafkaBrokers:          getEnv("KAFKA_BROKERS", "127.0.0.1:9092"),
		PaymentSettledTopic:   getEnv("PAYMENT_SETTLED_TOPIC", "payment.settled"),
		PaymentSettledGroupID: getEnv("ORDER_PAYMENT_SETTLED_GROUP_ID", "order-service-payment-settled-v1"),
	}
}

func (c Config) HTTPAddr() string {
	return fmt.Sprintf(":%s", c.Port)
}

func getEnv(key, fallback string) string {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}

	return value
}
