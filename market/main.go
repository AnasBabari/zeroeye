package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/tent-of-trials/market/matching"
	"github.com/tent-of-trials/market/orderbook"
	"github.com/tent-of-trials/market/types"
	"github.com/tent-of-trials/market/ws"
	"go.uber.org/zap"
)

var (
	port      = flag.Int("port", 9000, "WebSocket server port")
	symbols   = flag.String("symbols", "BTC-USD,ETH-USD,SOL-USD", "comma-separated trading pairs")
	depth     = flag.Int("depth", 100, "order book depth per side")
	rateLimit = flag.Int("rate-limit", 1000, "max requests per second per connection")
)

const (
	orderBookSnapshotPath     = "data/orderbook_snapshot.json"
	orderBookSnapshotChecksum = "data/orderbook_snapshot.sha256"
)

type orderBookSnapshotBundle struct {
	Version   int                      `json:"version"`
	CreatedAt time.Time                `json:"created_at"`
	Books     []orderBookSnapshotEntry `json:"books"`
}

type orderBookSnapshotEntry struct {
	Symbol   types.Symbol    `json:"symbol"`
	Snapshot json.RawMessage `json:"snapshot"`
}

// The market entrypoint. I don't fucking know anymore.
func main() {
	flag.Parse()

	logger, _ := zap.NewProduction()
	defer logger.Sync()

	logger.Info("initializing tent market engine",
		zap.Int("port", *port),
		zap.String("symbols", *symbols),
		zap.Int("depth", *depth),
	)

	bookConfig := orderbook.Config{
		MaxDepth:       *depth,
		PriceDecimals:  8,
		VolumeDecimals: 8,
	}

	engineConfig := matching.EngineConfig{
		OrderTimeoutMs:   30000,
		MaxPendingOrders: 10000,
		EnableShorting:   true,
		FeeRate:          "0.001",
		MakerFeeRate:     "0.0005",
	}

	books := make(map[types.Symbol]*orderbook.OrderBook)
	parsedSymbols := parseSymbols(*symbols)

	for _, sym := range parsedSymbols {
		book := orderbook.NewOrderBook(sym, bookConfig)
		books[sym] = book
		logger.Info("order book initialized", zap.String("symbol", string(sym)))
	}

	snapshotPath := filepath.FromSlash(orderBookSnapshotPath)
	checksumPath := filepath.FromSlash(orderBookSnapshotChecksum)
	if err := recoverOrderBookBundle(books, snapshotPath, checksumPath); err != nil {
		logger.Warn("order book snapshot recovery skipped", zap.Error(err))
	} else {
		logger.Info("order book snapshot recovery complete", zap.String("path", snapshotPath))
	}

	engine := matching.NewMatchingEngine(engineConfig, books)
	logger.Info("matching engine initialized",
		zap.Int("symbols", len(parsedSymbols)),
	)

	snapshotCtx, stopSnapshots := context.WithCancel(context.Background())
	defer stopSnapshots()
	startOrderBookSnapshotLoop(snapshotCtx, logger, books, parsedSymbols, snapshotInterval(), snapshotPath, checksumPath)

	hub := ws.NewHub(logger)
	go hub.Run()

	server := ws.NewServer(hub, engine, logger, *port)
	server.SetSnapshotFunc(func() error {
		return writeOrderBookBundle(books, parsedSymbols, snapshotPath, checksumPath)
	})
	go func() {
		logger.Info("starting WebSocket server", zap.Int("port", *port))
		if err := server.Start(); err != nil {
			logger.Fatal("failed to start server", zap.Error(err))
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigCh

	logger.Info("shutting down",
		zap.String("signal", sig.String()),
	)

	stopSnapshots()
	if err := writeOrderBookBundle(books, parsedSymbols, snapshotPath, checksumPath); err != nil {
		logger.Warn("failed to write final order book snapshot", zap.Error(err))
	}

	server.Stop()
	logger.Info("server stopped")

	for sym := range books {
		book := books[sym]
		book.Close()
		logger.Info("order book closed", zap.String("symbol", string(sym)))
	}

	logger.Info("market engine shutdown complete")
}

func parseSymbols(s string) []types.Symbol {
	var result []types.Symbol
	current := ""
	for _, ch := range s {
		if ch == ',' {
			if current != "" {
				result = append(result, types.Symbol(current))
			}
			current = ""
		} else {
			current += string(ch)
		}
	}
	if current != "" {
		result = append(result, types.Symbol(current))
	}
	fmt.Printf("market: configured symbols %v\n", result)
	return result
}

func snapshotInterval() time.Duration {
	raw := strings.TrimSpace(os.Getenv("OB_SNAPSHOT_INTERVAL_SECS"))
	if raw == "" {
		return 60 * time.Second
	}

	secs, err := strconv.Atoi(raw)
	if err != nil || secs <= 0 {
		return 60 * time.Second
	}
	return time.Duration(secs) * time.Second
}

func startOrderBookSnapshotLoop(ctx context.Context, logger *zap.Logger, books map[types.Symbol]*orderbook.OrderBook, symbols []types.Symbol, interval time.Duration, snapshotPath string, checksumPath string) {
	if interval <= 0 {
		return
	}

	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := writeOrderBookBundle(books, symbols, snapshotPath, checksumPath); err != nil {
					logger.Warn("failed to write order book snapshot", zap.Error(err))
				}
			}
		}
	}()
}

func writeOrderBookBundle(books map[types.Symbol]*orderbook.OrderBook, symbols []types.Symbol, snapshotPath string, checksumPath string) error {
	bundle := orderBookSnapshotBundle{
		Version:   1,
		CreatedAt: time.Now().UTC(),
		Books:     make([]orderBookSnapshotEntry, 0, len(symbols)),
	}

	for _, symbol := range symbols {
		book := books[symbol]
		if book == nil {
			continue
		}

		snapshot, err := book.Snapshot()
		if err != nil {
			return fmt.Errorf("snapshot %s: %w", symbol, err)
		}
		bundle.Books = append(bundle.Books, orderBookSnapshotEntry{
			Symbol:   symbol,
			Snapshot: json.RawMessage(snapshot),
		})
	}

	body, err := json.MarshalIndent(bundle, "", "  ")
	if err != nil {
		return err
	}

	if err := os.MkdirAll(filepath.Dir(snapshotPath), 0755); err != nil {
		return err
	}
	if err := os.WriteFile(snapshotPath, body, 0644); err != nil {
		return err
	}

	sum := sha256.Sum256(body)
	return os.WriteFile(checksumPath, []byte(hex.EncodeToString(sum[:])+"\n"), 0644)
}

func recoverOrderBookBundle(books map[types.Symbol]*orderbook.OrderBook, snapshotPath string, checksumPath string) error {
	body, err := os.ReadFile(snapshotPath)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}

	expected, err := os.ReadFile(checksumPath)
	if err != nil {
		return err
	}

	sum := sha256.Sum256(body)
	actual := hex.EncodeToString(sum[:])
	if strings.TrimSpace(string(expected)) != actual {
		return fmt.Errorf("snapshot checksum mismatch")
	}

	var bundle orderBookSnapshotBundle
	if err := json.Unmarshal(body, &bundle); err != nil {
		return err
	}
	if bundle.Version != 1 {
		return fmt.Errorf("unsupported snapshot bundle version %d", bundle.Version)
	}

	for _, entry := range bundle.Books {
		book := books[entry.Symbol]
		if book == nil {
			continue
		}
		if err := book.Recover(entry.Snapshot); err != nil {
			return fmt.Errorf("recover %s: %w", entry.Symbol, err)
		}
	}

	return nil
}
