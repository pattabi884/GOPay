package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/shopspring/decimal"
)

type razorpayCreateOrderRequest struct {
	Amount   int64  `json:"amount"` // PAISE, not rupees — Razorpay counts in the smallest unit
	Currency string `json:"currency"`
	Receipt  string `json:"receipt,omitempty"` // omitempty: dropped from JSON entirely if ""
}

// razorpayCreateOrderResponse: only the fields we use, out of ~15 they send.
type razorpayCreateOrderResponse struct {
	ID     string `json:"id"`
	Status string `json:"status"`
}

//the real plug Lower case feilds construction goes thruhh newrazorpayclient

type RazorpayClient struct {
	baseURL    string
	keyID      string
	keySecret  string
	httpClient *http.Client
}

// prod needs "https://api.razorpay.com"
func NewRazorpayClient(baseURL, keyID, keySecret string) *RazorpayClient {
	return &RazorpayClient{
		baseURL:   baseURL,
		keyID:     keyID,
		keySecret: keySecret,
		//timeout
		httpClient: &http.Client{Timeout: 10 * time.Second},
	}
}

func (r *RazorpayClient) CreateOrder(
	ctx context.Context,
	amount decimal.Decimal,
	currency,
	recipt string) (CreateOrderResult, error) {
	//razor pay sends paise we deal in rupees
	paise := amount.Mul(decimal.NewFromInt(100)).IntPart()
	//here the struct gets converted to json bytes
	payload, err := json.Marshal(razorpayCreateOrderRequest{
		Amount:   paise,
		Currency: currency,
		Receipt:  recipt,
	})
	if err != nil {
		return CreateOrderResult{}, fmt.Errorf("marshal create order request: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		r.baseURL+"/v1/orders", bytes.NewReader(payload))
	if err != nil {
		return CreateOrderResult{}, fmt.Errorf("build create order request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	// Basic auth: base64(keyID:keySecret) in the Authorization header.
	// Encoding, NOT encryption — safe only because HTTPS wraps the whole call.
	// This is the API key secret, NOT the webhook secret (two different locks).
	req.SetBasicAuth(r.keyID, r.keySecret)

	resp, err := r.httpClient.Do(req)
	if err != nil {
		// Transport failure: couldn't connect, timed out, ctx cancelled.
		// The "phone call didn't connect" case.
		return CreateOrderResult{}, fmt.Errorf("call razorpay create order: %w", err)
	}
	// Response body holds the network connection until closed. defer is mandatory here
	defer resp.Body.Close()

	// THE TRAP: err == nil only means the HTTP conversation happened.
	// Razorpay saying 400/401 is a successful conversation with a bad answer.
	if resp.StatusCode != http.StatusOK {
		errBody, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return CreateOrderResult{}, fmt.Errorf(
			"razorpay create order: status %d: %s", resp.StatusCode, string(errBody))
	}

	var out razorpayCreateOrderResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return CreateOrderResult{}, fmt.Errorf("decode create order response: %w", err)
	}

	// Wire shape → OUR shape. Callers never see razorpay* structs.
	return CreateOrderResult{
		ProviderOrderID: out.ID,
		Status:          out.Status,
	}, nil
}
