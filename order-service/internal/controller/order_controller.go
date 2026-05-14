package controller

import (
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"

	"gopay/order-service/internal/dto"
	"gopay/order-service/internal/usecase"
	"gopay/pkg/apperrors"
)

type OrderController struct {
	createOrderUsecase *usecase.CreateOrderUsecase
}

func NewOrderController(createOrderUsecase *usecase.CreateOrderUsecase) *OrderController {
	return &OrderController{
		createOrderUsecase: createOrderUsecase,
	}
}

func (c *OrderController) Create(ctx *gin.Context) {
	var req dto.CreateOrderRequest

	if err := ctx.ShouldBindJSON(&req); err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{
			"error": "invalid request body",
		})
		return
	}

	order, err := c.createOrderUsecase.Execute(ctx.Request.Context(), usecase.CreateOrderInput{
		CustomerID: req.CustomerID,
		ProductID:  req.ProductID,
		Amount:     req.Amount,
		Currency:   req.Currency,
	})
	if err != nil {
		status := mapErrorToStatus(err)

		ctx.JSON(status, gin.H{
			"error": errorMessage(err),
		})
		return
	}

	ctx.JSON(http.StatusCreated, toOrderResponse(order))
}

func mapErrorToStatus(err error) int {
	switch {
	case errors.Is(err, apperrors.ErrInvalidInput):
		return http.StatusBadRequest
	case errors.Is(err, apperrors.ErrNotFound):
		return http.StatusNotFound
	case errors.Is(err, apperrors.ErrConflict):
		return http.StatusConflict
	default:
		return http.StatusInternalServerError
	}
}
func errorMessage(err error) string {
	switch {
	case errors.Is(err, apperrors.ErrInvalidInput):
		return "invalid input"
	case errors.Is(err, apperrors.ErrNotFound):
		return "not found"
	case errors.Is(err, apperrors.ErrConflict):
		return "conflict"
	default:
		return "internal server error"
	}
}
