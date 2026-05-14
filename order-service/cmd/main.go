package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"gopay/order-service/internal/config"
	"gopay/order-service/internal/controller"
	"gopay/order-service/internal/provider"
	"gopay/order-service/internal/repository"
	"gopay/order-service/internal/server"
	"gopay/order-service/internal/usecase"
	"gopay/pkg/outbox"
)

func main() {
	cfg := config.Load()

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	healthController := controller.NewHealthController()

	db, sqlDB, err := provider.NewPostgresDB(cfg.PostgresDSN)
	if err != nil {
		logger.Error("connect postgres", slog.String("error", err.Error()))
		os.Exit(1)
	}
	defer sqlDB.Close()

	orderRepo := repository.NewGormOrderRepository(db)
	createOrderUsecase := usecase.NewCreateOrderUsecase(orderRepo)

	orderController := controller.NewOrderController(createOrderUsecase)

	router := server.NewRouter(healthController, orderController)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	redisClient, err := provider.NewRedisClient(ctx, cfg.RedisAddr)
	if err != nil {
		logger.Error("connect redis", slog.String("error", err.Error()))
		os.Exit(1)
	}
	defer redisClient.Close()

	brokers := splitCSV(cfg.KafkaBrokers)
	publisher := provider.NewKafkaPublisher(brokers, logger)
	defer publisher.Close()

	relay := outbox.NewRelay(outbox.RelayConfig{
		ServiceName:  "order-service",
		DB:           db,
		Redis:        redisClient,
		Publisher:    publisher,
		Logger:       logger,
		PollInterval: 500 * time.Millisecond,
		BatchSize:    100,
	})

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		relay.Run(ctx)
	}()

	httpServer := &http.Server{
		Addr:    cfg.HTTPAddr(),
		Handler: router,
	}

	go func() {
		logger.Info("starting order-service",
			slog.String("addr", cfg.HTTPAddr()),
			slog.String("env", cfg.AppEnv),
		)

		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("order-service stopped", slog.String("error", err.Error()))
			stop()
		}
	}()

	<-ctx.Done()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		logger.Error("shutdown http server", slog.String("error", err.Error()))
	}

	wg.Wait()

}

func splitCSV(value string) []string {
	parts := strings.Split(value, ",")
	items := make([]string, 0, len(parts))

	for _, part := range parts {
		item := strings.TrimSpace(part)
		if item != "" {
			items = append(items, item)
		}
	}

	return items
}
