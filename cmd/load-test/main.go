package main

import (
	"flag"
	"fmt"
	"log"
	"net/url"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

func main() {
	connections := flag.Int("c", 1000, "Number of concurrent connections")
	addr := flag.String("addr", "localhost:8080", "Gateway address")
	symbol := flag.String("symbol", "AAPL", "Symbol to subscribe to")
	flag.Parse()

	u := url.URL{Scheme: "ws", Host: *addr, Path: "/ws"}
	log.Printf("Connecting to %s", u.String())

	var wg sync.WaitGroup
	var connectedCount int32
	var messageCount int32

	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)

	// Context for graceful shutdown of clients
	// (not strictly necessary for a load test but good practice)

	for i := 0; i < *connections; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			defer atomic.AddInt32(&connectedCount, -1)

			c, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
			if err != nil {
				// Silently fail connection errors to avoid flooding logs
				return
			}
			defer c.Close()

			atomic.AddInt32(&connectedCount, 1)

			// Subscribe
			sub := map[string]string{
				"action": "subscribe",
				"symbol": *symbol,
			}
			if err := c.WriteJSON(sub); err != nil {
				return
			}

			// Read loop
			for {
				_, _, err := c.ReadMessage()
				if err != nil {
					return
				}
				atomic.AddInt32(&messageCount, 1)
			}
		}(i)

		// Throttle connection attempts to avoid overwhelming the gateway during handshake
		if i%50 == 0 && i > 0 {
			time.Sleep(50 * time.Millisecond)
		}
		if i%500 == 0 && i > 0 {
			fmt.Printf("Attempted %d connections...\n", i)
		}
	}

	// Metrics reporter
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		for range ticker.C {
			conn := atomic.LoadInt32(&connectedCount)
			msgs := atomic.SwapInt32(&messageCount, 0)
			fmt.Printf("[%s] Active Clients: %d | Throughput: %d msgs/5s (%.1f msgs/s)\n", 
				time.Now().Format("15:04:05"), conn, msgs, float64(msgs)/5.0)
		}
	}()

	fmt.Printf("Load test running with %d target connections. Press Ctrl+C to stop.\n", *connections)
	<-interrupt
	log.Println("Stopping load test...")
}
