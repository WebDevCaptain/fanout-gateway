package hub

import (
	"testing"
	"time"

	"github.com/shreyash/fanout-gateway/internal/models"
)

func TestHubRegisterUnregister(t *testing.T) {
	h := NewHub()
	client := NewClient("test-client", 1)

	h.Register(client)
	h.mu.RLock()
	if _, ok := h.clients[client.ID()]; !ok {
		t.Errorf("Client not registered")
	}
	h.mu.RUnlock()

	h.Unregister(client.ID())
	h.mu.RLock()
	if _, ok := h.clients[client.ID()]; ok {
		t.Errorf("Client not unregistered")
	}
	h.mu.RUnlock()
}

func TestHubBroadcast(t *testing.T) {
	h := NewHub()
	client := NewClient("test-client", 1)

	h.Register(client)
	h.Subscribe(client.ID(), "AAPL")

	tick := models.Tick{
		Symbol:    "AAPL",
		Price:     150.0,
		Timestamp: time.Now(),
	}

	h.Broadcast(tick)

	select {
	case received := <-client.send:
		if received.Symbol != tick.Symbol || received.Price != tick.Price {
			t.Errorf("Received wrong tick: %+v", received)
		}
	case <-time.After(100 * time.Millisecond):
		t.Errorf("Timed out waiting for broadcast")
	}
}

func TestHubMultipleSubscriptions(t *testing.T) {
	h := NewHub()
	client1 := NewClient("c1", 1)
	client2 := NewClient("c2", 1)

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
	case <-client1.send:
	default:
		t.Errorf("Client 1 did not receive AAPL")
	}
	select {
	case <-client2.send:
	default:
		t.Errorf("Client 2 did not receive AAPL")
	}

	h.Broadcast(tickGOOG)

	// Only client 1 should receive GOOG
	select {
	case <-client1.send:
	default:
		t.Errorf("Client 1 did not receive GOOG")
	}
	select {
	case <-client2.send:
		t.Errorf("Client 2 received GOOG but was not subscribed")
	default:
	}
}
