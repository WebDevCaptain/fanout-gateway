package hub

import (
	"testing"
	"time"
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

	payload := []byte(`{"symbol":"AAPL","price":150}`)

	h.Broadcast("AAPL", payload)

	select {
	case received := <-client.send:
		if string(received) != string(payload) {
			t.Errorf("Received wrong payload: %s", string(received))
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

	payloadAAPL := []byte(`{"symbol":"AAPL"}`)
	payloadGOOG := []byte(`{"symbol":"GOOG"}`)

	h.Broadcast("AAPL", payloadAAPL)

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

	h.Broadcast("GOOG", payloadGOOG)

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

func TestHubUnsubscribe(t *testing.T) {
	h := NewHub()
	client := NewClient("test-client", 1)

	h.Register(client)
	h.Subscribe(client.ID(), "AAPL")

	// Verify subscription
	h.mu.RLock()
	if _, ok := h.subscriptions["AAPL"][client.ID()]; !ok {
		t.Errorf("Client not subscribed")
	}
	h.mu.RUnlock()

	// Unsubscribe
	h.Unsubscribe(client.ID(), "AAPL")

	// Verify unsubscription
	h.mu.RLock()
	if subs, ok := h.subscriptions["AAPL"]; ok {
		if _, ok := subs[client.ID()]; ok {
			t.Errorf("Client still subscribed after Unsubscribe")
		}
		if len(subs) == 0 {
			t.Errorf("Empty subscription map should have been deleted")
		}
	}
	h.mu.RUnlock()

	// Check client's internal state
	client.mu.RLock()
	if _, ok := client.symbols["AAPL"]; ok {
		t.Errorf("Symbol still in client's symbols map")
	}
	client.mu.RUnlock()
}
