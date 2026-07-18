package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"gopay/payment-service/internal/config"
	"gopay/payment-service/internal/consumer"
	"gopay/payment-service/internal/controller"
	"gopay/payment-service/internal/middleware"
	"gopay/payment-service/internal/provider"
	"gopay/payment-service/internal/repository"
	"gopay/payment-service/internal/server"
	"gopay/payment-service/internal/usecase"
	"gopay/pkg/outbox"
)

func main() {
	cfg := config.Load()
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	if cfg.RazorpayWebhookSecret == "" {
		logger.Warn("webhook secret is empty or not set ")
	}
	logger.Warn("")
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	healthController := controller.NewHealthController()
	webhookController := controller.NewWebhookController(logger)
	verifyRazorpay := middleware.VerifyRazorpay(cfg.RazorpayWebhookSecret)
	router := server.NewRouter(healthController, webhookController, verifyRazorpay)

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

	httpServer := &http.Server{
		Addr:    cfg.HTTPAddr(),
		Handler: router,
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		logger.Info("http server started", slog.String("addr", cfg.HTTPAddr()))
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("http server failed", slog.String("error", err.Error()))
			stop()
		}
	}()
	<-ctx.Done()
	logger.Info("payment-service shuttting down")
	shutdownCtx, cancle := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancle()
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		logger.Error("http server shutdown", slog.String("error", err.Error()))
	}
	wg.Wait()
	go func() {
		logger.Info("payment-service started",
			slog.String("env", cfg.AppEnv),
			slog.String("topic", cfg.OrderCreatedTopic),
			slog.String("group_id", cfg.ConsumerGroupID),
			slog.String("addr", cfg.HTTPAddr()),
		)
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("payment service stopped", slog.String("error", err.Error()))
			stop()
		}

	}()

	<-ctx.Done()

	logger.Info("payment-service shutting down")

	wg.Wait()
}
