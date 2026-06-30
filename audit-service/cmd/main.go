package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"gopay/audit-service/internal/config"
	"gopay/audit-service/internal/consumer"
	"gopay/audit-service/internal/provider"
	"gopay/audit-service/internal/repository"
	"gopay/audit-service/internal/usecase"
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

	auditRepo := repository.NewGormAuditRepository(db)
	recordEventUsecase := usecase.NewRecordEventUsecase(auditRepo)

	brokers := cfg.KafkaBrokersList()

	consumers := []*consumer.AuditConsumer{
		consumer.NewAuditConsumer(brokers, cfg.OrderCreatedTopic, cfg.OrderCreatedGroupID, recordEventUsecase, logger),
		consumer.NewAuditConsumer(brokers, cfg.PaymentSettledTopic, cfg.PaymentSettledGroupID, recordEventUsecase, logger),
		consumer.NewAuditConsumer(brokers, cfg.PaymentFailedTopic, cfg.PaymentFailedGroupID, recordEventUsecase, logger),
	}

	var wg sync.WaitGroup
	for _, c := range consumers {
		wg.Add(1)
		go func(c *consumer.AuditConsumer) {
			defer wg.Done()
			c.Run(ctx)
		}(c)
		defer c.Close()
	}

	logger.Info("audit-service started", slog.String("env", cfg.AppEnv))

	<-ctx.Done()

	logger.Info("audit-service shutting down")

	wg.Wait()
}
