package bridge

const (
	Connect     = "connect"
	Publish     = "publish"
	Subscribe   = "subscribe"
	Unsubscribe = "unsubscribe"
	Disconnect  = "disconnect"
)

type Elements struct {
	ClientID  string `json:"clientID"`
	Username  string `json:"username"`
	Topic     string `json:"topic"`
	Payload   string `json:"payload"`
	Timestamp int64  `json:"ts"`
	Size      int32  `json:"size"`
	Action    string `json:"action"`
}

type Bridge interface {
	Publish(e *Elements) error
}
