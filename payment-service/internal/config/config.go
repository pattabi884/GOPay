package config

import (
	"os"
	"strings"
)

type Config struct {
	AppEnv                string
	KafkaBrokers          string
	PostgresDSN           string
	OrderCreatedTopic     string
	ConsumerGroupID       string
	RedisAddr             string
	MockPaymentOutcome    string
	HTTPPort              string
	RazorpayWebhookSecret string
	RazorpayKeySecret     string
}

func Load() Config {
	return Config{
		AppEnv:             getEnv("APP_ENV", "development"),
		KafkaBrokers:       getEnv("KAFKA_BROKERS", "127.0.0.1:9092"),
		PostgresDSN:        getEnv("POSTGRES_PAYMENT_DSN", "postgres://gopay:gopay@127.0.0.1:5433/gopay_payment?sslmode=disable"),
		OrderCreatedTopic:  getEnv("ORDER_CREATED_TOPIC", "order.created"),
		ConsumerGroupID:    getEnv("PAYMENT_ORDER_CREATED_GROUP_ID", "payment-service-order-created-v1"),
		RedisAddr:          getEnv("REDIS_ADDR", "127.0.0.1:6379"),
		MockPaymentOutcome: getEnv("PAYMENT_MOCK_OUTCOME", "settled"),
		HTTPPort:           getEnv("PAYMENT_SERVICE_PORT", "8082"),
		RazorpayWebhookSecret: getEnv("RAZORPAY_WEBHOOK_SECRET", ""),
		RazorpayKeySecret: getEnv("RAZORPAY_KEY_SECRET", ""),

	}
}

// HTTPAddr returns the listen address for the HTTP server, e.g. ":8082".
func (c Config) HTTPAddr() string {
	return ":" + c.HTTPPort
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
