package controller

import (
	"time"

	"gopay/order-service/internal/dto"
	"gopay/order-service/internal/entity"
)

func toOrderResponse(order entity.Order) dto.OrderResponse {
	return dto.OrderResponse{
		ID:         order.ID.String(),
		CustomerID: order.CustomerID.String(),
		ProductID:  order.ProductID.String(),
		Amount:     order.Amount,
		Currency:   order.Currency,
		Status:     string(order.Status),
		Version:    order.Version,
		CreatedAt:  order.CreatedAt.Format(time.RFC3339),
		UpdatedAt:  order.UpdatedAt.Format(time.RFC3339),
	}
}
