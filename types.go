package aibot

import "time"

const (
	DefaultWSURL = "wss://openws.work.weixin.qq.com"

	CmdSubscribe       = "aibot_subscribe"
	CmdHeartbeat       = "ping"
	CmdResponse        = "aibot_respond_msg"
	CmdResponseWelcome = "aibot_respond_welcome_msg"
	CmdResponseUpdate  = "aibot_respond_update_msg"
	CmdSendMessage     = "aibot_send_msg"
	CmdCallback        = "aibot_msg_callback"
	CmdEventCallback   = "aibot_event_callback"
)

const (
	MessageTypeText  = "text"
	MessageTypeImage = "image"
	MessageTypeMixed = "mixed"
	MessageTypeVoice = "voice"
	MessageTypeFile  = "file"
)

const (
	EventTypeEnterChat         = "enter_chat"
	EventTypeTemplateCardEvent = "template_card_event"
	EventTypeFeedbackEvent     = "feedback_event"
)

type FrameHeaders struct {
	ReqID string `json:"req_id,omitempty"`
}

type Frame struct {
	Cmd     string         `json:"cmd,omitempty"`
	Headers FrameHeaders   `json:"headers"`
	Body    map[string]any `json:"body,omitempty"`
	ErrCode int            `json:"errcode,omitempty"`
	ErrMsg  string         `json:"errmsg,omitempty"`
}

func (f Frame) ReqID() string {
	return f.Headers.ReqID
}

type ClientOptions struct {
	BotID                string
	Secret               string
	ReconnectInterval    time.Duration
	MaxReconnectAttempts int
	HeartbeatInterval    time.Duration
	RequestTimeout       time.Duration
	WSURL                string
	Logger               Logger
}

func (o ClientOptions) normalized() ClientOptions {
	if o.ReconnectInterval <= 0 {
		o.ReconnectInterval = time.Second
	}
	if o.MaxReconnectAttempts == 0 {
		o.MaxReconnectAttempts = 10
	}
	if o.HeartbeatInterval <= 0 {
		o.HeartbeatInterval = 30 * time.Second
	}
	if o.RequestTimeout <= 0 {
		o.RequestTimeout = 10 * time.Second
	}
	if o.WSURL == "" {
		o.WSURL = DefaultWSURL
	}
	if o.Logger == nil {
		o.Logger = NewDefaultLogger("AiBotSDK")
	}
	return o
}
