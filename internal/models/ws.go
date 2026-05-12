package models

type ClientAction string

const (
	ActionSubscribe   ClientAction = "subscribe"
	ActionUnsubscribe ClientAction = "unsubscribe"
)

type ClientMessage struct {
	Action ClientAction `json:"action"`
	Symbol string       `json:"symbol"`
}
