package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"time"

	"syscall"

	"gopay/payment-service/internal/config"
	"gopay/payment-service/internal/consumer"
	"gopay/payment-service/internal/provider"
	"gopay/payment-service/internal/repository"
	"gopay/payment-service/internal/usecase"
	"gopay/pkg/outbox"
)

func main() {
	cfg := config.Load()
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	db, sqlDB, err := provider.NewPostgresDB(cfg.PostgresDSN)
	if err != nil {
		logger.Error("connect postgres", slog.String("error", err.Error()))
		os.Exit(1)
	}
	defer sqlDB.Close()

	paymentRepo := repository.NewGormPaymentRepository(db)
	createPaymentUsecase := usecase.NewCreatePaymentFromOrderUsecase(paymentRepo, cfg.MockPaymentOutcome)

	redisClient, err := provider.NewRedisClient(ctx, cfg.RedisAddr)
	if err != nil {
		logger.Error("connect redis", slog.String("error", err.Error()))
		os.Exit(1)
	}
	defer redisClient.Close()

	publisher := provider.NewKafkaPublisher(cfg.KafkaBrokersList(), logger)
	defer publisher.Close()

	relay := outbox.NewRelay(outbox.RelayConfig{
		ServiceName:  "payment-service",
		DB:           db,
		Redis:        redisClient,
		Publisher:    publisher,
		Logger:       logger,
		PollInterval: 500 * time.Millisecond,
		BatchSize:    100,
	})
	orderCreatedConsumer := consumer.NewOrderCreatedConsumer(
		cfg.KafkaBrokersList(),
		cfg.OrderCreatedTopic,
		cfg.ConsumerGroupID,
		createPaymentUsecase,
		logger,
	)
	defer orderCreatedConsumer.Close()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		relay.Run(ctx)
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		orderCreatedConsumer.Run(ctx)
	}()

	logger.Info("payment-service started",
		slog.String("env", cfg.AppEnv),
		slog.String("topic", cfg.OrderCreatedTopic),
		slog.String("group_id", cfg.ConsumerGroupID),
	)

	<-ctx.Done()

	logger.Info("payment-service shutting down")

	wg.Wait()
}
