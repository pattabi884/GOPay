package server

import (
	"github.com/gin-gonic/gin"

	"gopay/payment-service/internal/controller"
)

func NewRouter(
	healthController *controller.HealthController,
	webhookController *controller.WebhookController,
	verifyRazorpay gin.HandlerFunc,
) *gin.Engine {
	router := gin.New()

	router.Use(gin.Recovery())

	router.GET("/healthz/live", healthController.Live)
	router.POST("/webhooks/razorpay", verifyRazorpay, webhookController.HandleRazorpay)

	return router
}
