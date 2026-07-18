package controller

import (
	"log/slog"
	"net/http"

	"github.com/gin-gonic/gin"
)

type webhookPayload struct {
	Event string `json:"event"`
}

type WebhookController struct {
	logger *slog.Logger
}

func NewWebhookController(logger *slog.Logger) *WebhookController {
	return &WebhookController{logger: logger}
}

func (w *WebhookController) HandleRazorpay(ctx *gin.Context) {
	var payload webhookPayload

	if err := ctx.ShouldBindJSON(&payload); err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "invalid payload"})
		return
	}
	w.logger.Info("razorpay webhook recived", slog.String("event", payload.Event))
	ctx.Status(http.StatusOK)
}