package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
	"github.com/shreyash/fanout-gateway/internal/hub"
	"github.com/shreyash/fanout-gateway/internal/models"
	"github.com/shreyash/fanout-gateway/internal/ws"
)

func main() {
	ctx := context.Background()

	// Connect to Redis/Valkey
	redisAddr := "localhost:6379"
	if addr := os.Getenv("REDIS_ADDR"); addr != "" {
		redisAddr = addr
	}

	rdb := redis.NewClient(&redis.Options{
		Addr: redisAddr,
	})

	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Fatalf("Could not connect to Redis: %v", err)
	}
	fmt.Println("Connected to Redis successfully")

	// Initialize Hub
	h := hub.NewHub()

	// Redis Listener
	go func() {
		pubsub := rdb.Subscribe(ctx, "market.ticks")
		defer pubsub.Close()

		ch := pubsub.Channel()
		for {
			select {
			case <-ctx.Done():
				log.Println("Redis listener shutting down...")
				return
			case msg, ok := <-ch:
				if !ok {
					log.Println("Redis channel closed, listener exiting...")
					return
				}
				var tick models.Tick
				if err := json.Unmarshal([]byte(msg.Payload), &tick); err != nil {
					log.Printf("Error unmarshaling tick: %v", err)
					continue
				}
				h.Broadcast(tick)
			}
		}
	}()

	// Gin Router
	r := gin.Default()

	// Health check
	r.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	})

	// Hub stats
	r.GET("/stats", func(c *gin.Context) {
		c.JSON(http.StatusOK, h.GetStats())
	})

	// WebSocket endpoint
	r.GET("/ws", func(c *gin.Context) {
		ws.ServeWS(c, h)
	})

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	fmt.Printf("Gateway starting on port %s\n", port)
	if err := r.Run(":" + port); err != nil {
		log.Fatalf("Failed to run server: %v", err)
	}
}
