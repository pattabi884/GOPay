package server

import (
	"github.com/gin-gonic/gin"

	"gopay/order-service/internal/controller"
)

func NewRouter(
	healthController *controller.HealthController,
	orderController *controller.OrderController,
) *gin.Engine {
	router := gin.New()

	router.Use(gin.Recovery())

	router.GET("/healthz/live", healthController.Live)
	router.POST("/orders", orderController.Create)

	return router
}
