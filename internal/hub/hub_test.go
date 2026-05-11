package hub

import (
	"testing"
	"time"

	"github.com/shreyash/fanout-gateway/internal/models"
)

func TestHubRegisterUnregister(t *testing.T) {
	h := NewHub()
	client := &Client{
		ID:   "test-client",
		Send: make(chan models.Tick, 1),
	}

	h.Register(client)
	if _, ok := h.clients[client.ID]; !ok {
		t.Errorf("Client not registered")
	}

	h.Unregister(client.ID)
	if _, ok := h.clients[client.ID]; ok {
		t.Errorf("Client not unregistered")
	}
}

func TestHubBroadcast(t *testing.T) {
	h := NewHub()
	client := &Client{
		ID:      "test-client",
		Send:    make(chan models.Tick, 1),
		Symbols: make(map[string]bool),
	}

	h.Register(client)
	h.Subscribe(client.ID, "AAPL")

	tick := models.Tick{
		Symbol:    "AAPL",
		Price:     150.0,
		Timestamp: time.Now(),
	}

	h.Broadcast(tick)

	select {
	case received := <-client.Send:
		if received.Symbol != tick.Symbol || received.Price != tick.Price {
			t.Errorf("Received wrong tick: %+v", received)
		}
	case <-time.After(100 * time.Millisecond):
		t.Errorf("Timed out waiting for broadcast")
	}
}

func TestHubMultipleSubscriptions(t *testing.T) {
	h := NewHub()
	client1 := &Client{ID: "c1", Send: make(chan models.Tick, 1), Symbols: make(map[string]bool)}
	client2 := &Client{ID: "c2", Send: make(chan models.Tick, 1), Symbols: make(map[string]bool)}

	h.Register(client1)
	h.Register(client2)

	h.Subscribe("c1", "AAPL")
	h.Subscribe("c1", "GOOG")
	h.Subscribe("c2", "AAPL")

	tickAAPL := models.Tick{Symbol: "AAPL", Price: 150.0}
	tickGOOG := models.Tick{Symbol: "GOOG", Price: 2800.0}

	h.Broadcast(tickAAPL)

	// Both should receive AAPL
	select {
	case <-client1.Send:
	default:
		t.Errorf("Client 1 did not receive AAPL")
	}
	select {
	case <-client2.Send:
	default:
		t.Errorf("Client 2 did not receive AAPL")
	}

	h.Broadcast(tickGOOG)

	// Only client 1 should receive GOOG
	select {
	case <-client1.Send:
	default:
		t.Errorf("Client 1 did not receive GOOG")
	}
	select {
	case <-client2.Send:
		t.Errorf("Client 2 received GOOG but was not subscribed")
	default:
	}
}
