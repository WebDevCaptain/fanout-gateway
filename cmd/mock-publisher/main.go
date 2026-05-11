package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand/v2"
	"os"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/shreyash/fanout-gateway/internal/models"
)

var (
	symbols = []string{"AAPL", "GOOGL", "MSFT", "AMZN", "TSLA"}
	prices  = map[string]float64{
		"AAPL":  150.0,
		"GOOGL": 2800.0,
		"MSFT":  300.0,
		"AMZN":  3300.0,
		"TSLA":  700.0,
	}
)

func main() {
	ctx := context.Background()

	// Connect to Redis
	redisAddr := "localhost:6379"
	if addr := os.Getenv("REDIS_ADDR"); addr != "" {
		redisAddr = addr
	}

	rdb := redis.NewClient(&redis.Options{
		Addr: redisAddr,
	})

	// Check connection
	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Fatalf("Could not connect to Redis: %v", err)
	}
	fmt.Println("Connected to Redis successfully")

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	fmt.Println("Starting mock market data generation...")

	for range ticker.C {
		for _, symbol := range symbols {
			// Update price with a small random variation
			change := (rand.Float64() - 0.5) * 2.0 // -1.0 to 1.0
			prices[symbol] += change

			tick := models.Tick{
				Symbol:    symbol,
				Price:     prices[symbol],
				Timestamp: time.Now(),
			}

			payload, err := json.Marshal(tick)
			if err != nil {
				log.Printf("Error marshaling tick: %v", err)
				continue
			}

			// Publish to Redis
			err = rdb.Publish(ctx, "market.ticks", payload).Err()
			if err != nil {
				log.Printf("Error publishing to Redis: %v", err)
			}
		}
	}
}
