package ws

import (
	"fmt"
	"log"
	"math/rand/v2"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/shreyash/fanout-gateway/internal/hub"
	"github.com/shreyash/fanout-gateway/internal/models"
)

const (
	// Time allowed to write a message to the peer.
	writeWait = 10 * time.Second

	// Time allowed to read the next pong message from the peer.
	pongWait = 60 * time.Second

	// Send pings to peer with this period. Must be less than pongWait.
	pingPeriod = (pongWait * 9) / 10

	// Maximum message size allowed from peer.
	maxMessageSize = 512
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin: func(r *http.Request) bool {
		return true // Allow all origins for now
	},
}

// WSClient is a middleman between the websocket connection and the hub.
type WSClient struct {
	hub       *hub.Hub
	hubClient *hub.Client
	conn      *websocket.Conn
}

// readPump pumps messages from the websocket connection to the hub.
//
// The application runs readPump in a per-connection goroutine. The application
// ensures that there is at most one reader on a connection by executing all
// reads from this goroutine.
func (c *WSClient) readPump() {
	defer func() {
		c.hub.Unregister(c.hubClient.ID())
		c.hubClient.Close()
		c.conn.Close()
	}()

	c.conn.SetReadLimit(maxMessageSize)
	c.conn.SetReadDeadline(time.Now().Add(pongWait))
	c.conn.SetPongHandler(func(string) error {
		c.conn.SetReadDeadline(time.Now().Add(pongWait))
		return nil
	})

	for {
		var msg models.ClientMessage
		err := c.conn.ReadJSON(&msg)
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				log.Printf("error: %v", err)
			}
			break
		}

		switch msg.Action {
		case models.ActionSubscribe:
			if msg.Symbol == "" {
				log.Printf("Client %s sent empty symbol for subscribe", c.hubClient.ID())
				continue
			}
			log.Printf("Client %s subscribing to %s", c.hubClient.ID(), msg.Symbol)
			c.hub.Subscribe(c.hubClient.ID(), msg.Symbol)
		case models.ActionUnsubscribe:
			if msg.Symbol == "" {
				log.Printf("Client %s sent empty symbol for unsubscribe", c.hubClient.ID())
				continue
			}
			log.Printf("Client %s unsubscribing from %s", c.hubClient.ID(), msg.Symbol)
			c.hub.Unsubscribe(c.hubClient.ID(), msg.Symbol)
		default:
			log.Printf("Unknown action: %s", msg.Action)
		}
	}
}

// writePump pumps messages from the hub to the websocket connection.
//
// A goroutine running writePump is started for each connection. The
// application ensures that there is at most one writer on a connection by
// executing all writes from this goroutine.
func (c *WSClient) writePump() {
	ticker := time.NewTicker(pingPeriod)
	defer func() {
		ticker.Stop()
		c.conn.Close()
	}()

	for {
		select {
		case payload, ok := <-c.hubClient.Listen():
			c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if !ok {
				// The hub closed the channel.
				c.conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}

			if err := c.conn.WriteMessage(websocket.TextMessage, payload); err != nil {
				return
			}

		case <-ticker.C:
			c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

// ServeWS handles websocket requests from the peer.
func ServeWS(ctx *gin.Context, h *hub.Hub) {
	conn, err := upgrader.Upgrade(ctx.Writer, ctx.Request, nil)
	if err != nil {
		log.Printf("Failed to upgrade to websocket: %v", err)
		return
	}

	clientID := fmt.Sprintf("%s-%d-%d", ctx.ClientIP(), time.Now().UnixNano(), rand.Uint64())
	hubClient := hub.NewClient(clientID, 256)

	client := &WSClient{
		hub:       h,
		hubClient: hubClient,
		conn:      conn,
	}

	h.Register(hubClient)

	// Start pumps
	go client.writePump()
	go client.readPump()
}
