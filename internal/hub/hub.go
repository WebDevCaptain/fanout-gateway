package hub

import (
	"sync"

	"github.com/shreyash/fanout-gateway/internal/models"
)

// Client represents a connected WebSocket client.
// Its fields are private to ensure thread-safe access through the Hub.
type Client struct {
	id      string
	send    chan models.Tick
	symbols map[string]bool
	mu      sync.RWMutex
}

// NewClient creates a new hub client
func NewClient(id string, bufferSize int) *Client {
	return &Client{
		id:      id,
		send:    make(chan models.Tick, bufferSize),
		symbols: make(map[string]bool),
	}
}

func (c *Client) ID() string {
	return c.id
}

func (c *Client) Send() chan<- models.Tick {
	return c.send
}

// Hub maintains the set of active clients and broadcasts messages
type Hub struct {
	// Registered clients by symbol: symbol -> map[clientID]*Client
	subscriptions map[string]map[string]*Client
	// All active clients: clientID -> *Client
	clients map[string]*Client
	
	mu sync.RWMutex
}

func NewHub() *Hub {
	return &Hub{
		subscriptions: make(map[string]map[string]*Client),
		clients:       make(map[string]*Client),
	}
}

// Register adds a new client to the hub
func (h *Hub) Register(c *Client) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.clients[c.id] = c
}

// Unregister removes a client from the hub and all its subscriptions.
// It does NOT close the client's send channel; that is the responsibility
// of the component that created the client.
func (h *Hub) Unregister(clientID string) {
	h.mu.Lock()
	defer h.mu.Unlock()

	client, ok := h.clients[clientID]
	if !ok {
		return
	}

	// Remove from all symbol subscriptions
	client.mu.Lock()
	for symbol := range client.symbols {
		if subs, ok := h.subscriptions[symbol]; ok {
			delete(subs, clientID)
			if len(subs) == 0 {
				delete(h.subscriptions, symbol)
			}
		}
	}
	client.mu.Unlock()

	delete(h.clients, clientID)
}

// Subscribe adds a symbol to a client's interests
func (h *Hub) Subscribe(clientID string, symbol string) {
	h.mu.Lock()
	defer h.mu.Unlock()

	client, ok := h.clients[clientID]
	if !ok {
		return
	}

	client.mu.Lock()
	if client.symbols == nil {
		client.symbols = make(map[string]bool)
	}
	client.symbols[symbol] = true
	client.mu.Unlock()

	if _, ok := h.subscriptions[symbol]; !ok {
		h.subscriptions[symbol] = make(map[string]*Client)
	}
	h.subscriptions[symbol][clientID] = client
}

// Broadcast sends a tick to all clients subscribed to its symbol
func (h *Hub) Broadcast(tick models.Tick) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	if clients, ok := h.subscriptions[tick.Symbol]; ok {
		for _, client := range clients {
			// Non-blocking send to avoid hanging the hub if a client is slow
			select {
			case client.send <- tick:
			default:
				// Buffer full, skip or handle (e.g., disconnect slow client)
			}
		}
	}
}

// HubStats holds basic metrics for the hub
type HubStats struct {
	ActiveClients int            `json:"active_clients"`
	Subscriptions map[string]int `json:"subscriptions"`
}

// GetStats returns the current state of the hub
func (h *Hub) GetStats() HubStats {
	h.mu.RLock()
	defer h.mu.RUnlock()

	stats := HubStats{
		ActiveClients: len(h.clients),
		Subscriptions: make(map[string]int),
	}

	for symbol, clients := range h.subscriptions {
		stats.Subscriptions[symbol] = len(clients)
	}

	return stats
}
