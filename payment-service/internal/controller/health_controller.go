package controller

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

type HealthController struct{}

func NewHealthController() *HealthController {
	return &HealthController{}
}

func (h *HealthController) Live(ctx *gin.Context) {
	ctx.JSON(http.StatusOK, gin.H{
		"status":  "OK",
		"service": "payment-service",
	})
}
