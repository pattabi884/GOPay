package controller

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

type WebhookController struct{}

func NewWebhookController() *WebhookController {
	return &WebhookController{}
}

func (w *WebhookController) HandleRazorpay(ctx *gin.Context) {
	ctx.Status(http.StatusOK)
}
