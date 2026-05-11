package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/redis/go-redis/v9"
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

	// Subscribe to market.ticks
	pubsub := rdb.Subscribe(ctx, "market.ticks")
	defer pubsub.Close()

	fmt.Println("Subscribed to 'market.ticks'. Waiting for messages...")

	ch := pubsub.Channel()

	for msg := range ch {
		fmt.Printf("Received message from %s: %s\n", msg.Channel, msg.Payload)
	}
}
