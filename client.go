package aibot

import (
	"context"
	"fmt"
	"sync"
)

type Client struct {
	options ClientOptions
	logger  Logger

	mu      sync.Mutex
	started bool

	emitter        *Emitter
	api            *APIClient
	wsManager      *WSConnectionManager
	messageHandler *MessageHandler
}

func NewClient(options ClientOptions) *Client {
	options = options.normalized()

	client := &Client{
		options:        options,
		logger:         options.Logger,
		emitter:        NewEmitter(),
		api:            NewAPIClient(options.Logger, options.RequestTimeout),
		messageHandler: NewMessageHandler(options.Logger),
	}

	client.wsManager = NewWSConnectionManager(
		options.Logger,
		options.HeartbeatInterval,
		options.ReconnectInterval,
		options.MaxReconnectAttempts,
		options.WSURL,
	)
	client.wsManager.SetCredentials(options.BotID, options.Secret)
	client.wsManager.SetCallbacks(
		func() {
			client.emitter.Emit(Event{Name: "connected"})
		},
		func() {
			client.logger.Info("Authenticated")
			client.emitter.Emit(Event{Name: "authenticated"})
		},
		func(reason string) {
			client.emitter.Emit(Event{Name: "disconnected", Reason: reason})
		},
		func(frame Frame) {
			client.messageHandler.HandleFrame(frame, client.emitter)
		},
		func(attempt int) {
			client.emitter.Emit(Event{Name: "reconnecting", Attempt: attempt})
		},
		func(err error) {
			client.emitter.Emit(Event{Name: "error", Error: err})
		},
	)

	return client
}

func (c *Client) On(name string, handler EventHandler) {
	c.emitter.On(name, handler)
}

func (c *Client) Connect(ctx context.Context) error {
	c.mu.Lock()
	if c.started {
		c.mu.Unlock()
		c.logger.Warn("Client already connected")
		return nil
	}
	c.started = true
	c.mu.Unlock()

	c.logger.Info("Establishing WebSocket connection...")
	if err := c.wsManager.Connect(ctx); err != nil {
		c.mu.Lock()
		c.started = false
		c.mu.Unlock()
		return err
	}

	return nil
}

func (c *Client) Disconnect() error {
	c.mu.Lock()
	if !c.started {
		c.mu.Unlock()
		c.logger.Warn("Client not connected")
		return nil
	}
	c.started = false
	c.mu.Unlock()

	c.logger.Info("Disconnecting...")
	if err := c.wsManager.Disconnect(); err != nil {
		return err
	}
	c.logger.Info("Disconnected")
	return nil
}

func (c *Client) Reply(ctx context.Context, frame Frame, body map[string]any, cmd string) (Frame, error) {
	if cmd == "" {
		cmd = CmdResponse
	}
	reqID := frame.ReqID()
	if reqID == "" {
		return Frame{}, fmt.Errorf("reply frame missing req_id")
	}
	return c.wsManager.SendReply(ctx, reqID, body, cmd)
}

func (c *Client) ReplyStream(ctx context.Context, frame Frame, streamID, content string, finish bool, msgItem []map[string]any, feedback map[string]any) (Frame, error) {
	stream := map[string]any{
		"id":      streamID,
		"finish":  finish,
		"content": content,
	}
	if finish && len(msgItem) > 0 {
		stream["msg_item"] = msgItem
	}
	if len(feedback) > 0 {
		stream["feedback"] = feedback
	}

	return c.Reply(ctx, frame, map[string]any{
		"msgtype": "stream",
		"stream":  stream,
	}, "")
}

func (c *Client) ReplyWelcome(ctx context.Context, frame Frame, body map[string]any) (Frame, error) {
	return c.Reply(ctx, frame, body, CmdResponseWelcome)
}

func (c *Client) ReplyTemplateCard(ctx context.Context, frame Frame, templateCard map[string]any, feedback map[string]any) (Frame, error) {
	card := cloneMap(templateCard)
	if len(feedback) > 0 {
		card["feedback"] = feedback
	}

	return c.Reply(ctx, frame, map[string]any{
		"msgtype":       "template_card",
		"template_card": card,
	}, "")
}

func (c *Client) ReplyStreamWithCard(ctx context.Context, frame Frame, streamID, content string, finish bool, msgItem []map[string]any, streamFeedback map[string]any, templateCard map[string]any, cardFeedback map[string]any) (Frame, error) {
	stream := map[string]any{
		"id":      streamID,
		"finish":  finish,
		"content": content,
	}
	if finish && len(msgItem) > 0 {
		stream["msg_item"] = msgItem
	}
	if len(streamFeedback) > 0 {
		stream["feedback"] = streamFeedback
	}

	body := map[string]any{
		"msgtype": "stream_with_template_card",
		"stream":  stream,
	}
	if len(templateCard) > 0 {
		card := cloneMap(templateCard)
		if len(cardFeedback) > 0 {
			card["feedback"] = cardFeedback
		}
		body["template_card"] = card
	}

	return c.Reply(ctx, frame, body, "")
}

func (c *Client) UpdateTemplateCard(ctx context.Context, frame Frame, templateCard map[string]any, userIDs []string) (Frame, error) {
	body := map[string]any{
		"response_type": "update_template_card",
		"template_card": templateCard,
	}
	if len(userIDs) > 0 {
		body["userids"] = userIDs
	}

	return c.Reply(ctx, frame, body, CmdResponseUpdate)
}

func (c *Client) SendMessage(ctx context.Context, chatID string, body map[string]any) (Frame, error) {
	reqID := GenerateReqID(CmdSendMessage)
	fullBody := cloneMap(body)
	fullBody["chatid"] = chatID
	return c.wsManager.SendReply(ctx, reqID, fullBody, CmdSendMessage)
}

func (c *Client) DownloadFile(ctx context.Context, rawURL, aesKey string) ([]byte, string, error) {
	c.logger.Info("Downloading and decrypting file...")

	data, filename, err := c.api.DownloadFileRaw(ctx, rawURL)
	if err != nil {
		c.logger.Error("File download/decrypt failed: %v", err)
		return nil, "", err
	}

	if aesKey == "" {
		c.logger.Warn("No aes_key provided, returning raw file data")
		return data, filename, nil
	}

	decrypted, err := DecryptFile(data, aesKey)
	if err != nil {
		c.logger.Error("File download/decrypt failed: %v", err)
		return nil, "", err
	}

	c.logger.Info("File downloaded and decrypted successfully")
	return decrypted, filename, nil
}

func (c *Client) IsConnected() bool {
	return c.wsManager.IsConnected()
}

func (c *Client) API() *APIClient {
	return c.api
}

func (c *Client) Run(ctx context.Context) error {
	ctx = ensureContext(ctx)
	if err := c.Connect(ctx); err != nil {
		return err
	}

	<-ctx.Done()
	return c.Disconnect()
}

func cloneMap(src map[string]any) map[string]any {
	if len(src) == 0 {
		return map[string]any{}
	}
	dst := make(map[string]any, len(src))
	for key, value := range src {
		dst[key] = value
	}
	return dst
}
