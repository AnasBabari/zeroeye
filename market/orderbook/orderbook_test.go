package orderbook

import (
	"testing"

	"github.com/shopspring/decimal"
	"github.com/tent-of-trials/market/types"
)

func TestSnapshotRecoverRoundTrip(t *testing.T) {
	config := Config{MaxDepth: 10, PriceDecimals: 8, VolumeDecimals: 8}
	book := NewOrderBook(types.Symbol("BTC-USD"), config)

	orders := []*types.Order{
		{
			ID:           "sell-1",
			Symbol:       "BTC-USD",
			Side:         types.Sell,
			Type:         types.Limit,
			Price:        decimal.RequireFromString("101.25"),
			Quantity:     decimal.RequireFromString("2"),
			RemainingQty: decimal.RequireFromString("2"),
		},
		{
			ID:           "buy-1",
			Symbol:       "BTC-USD",
			Side:         types.Buy,
			Type:         types.Limit,
			Price:        decimal.RequireFromString("100.50"),
			Quantity:     decimal.RequireFromString("1.5"),
			RemainingQty: decimal.RequireFromString("1.5"),
		},
	}

	for _, order := range orders {
		if _, err := book.AddOrder(order); err != nil {
			t.Fatalf("AddOrder() error = %v", err)
		}
	}

	snapshot, err := book.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot() error = %v", err)
	}

	recovered := NewOrderBook(types.Symbol("BTC-USD"), config)
	if err := recovered.Recover(snapshot); err != nil {
		t.Fatalf("Recover() error = %v", err)
	}

	resnapshot, err := recovered.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot() after recover error = %v", err)
	}
	if string(snapshot) != string(resnapshot) {
		t.Fatalf("snapshot output changed after recover\nbefore:\n%s\nafter:\n%s", snapshot, resnapshot)
	}

	bids := recovered.GetBids()
	if len(bids) != 1 || !bids[0].Price.Equal(decimal.RequireFromString("100.50")) {
		t.Fatalf("unexpected recovered bids: %#v", bids)
	}

	asks := recovered.GetAsks()
	if len(asks) != 1 || !asks[0].Price.Equal(decimal.RequireFromString("101.25")) {
		t.Fatalf("unexpected recovered asks: %#v", asks)
	}
}

func TestRecoverRejectsWrongSymbol(t *testing.T) {
	config := Config{MaxDepth: 10, PriceDecimals: 8, VolumeDecimals: 8}
	book := NewOrderBook(types.Symbol("BTC-USD"), config)

	snapshot, err := book.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot() error = %v", err)
	}

	other := NewOrderBook(types.Symbol("ETH-USD"), config)
	if err := other.Recover(snapshot); err == nil {
		t.Fatal("Recover() expected wrong-symbol error, got nil")
	}
}
