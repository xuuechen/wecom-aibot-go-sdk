package aibot

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strings"
	"sync"
	"time"

	"golang.org/x/net/websocket"
)

type replyQueueItem struct {
	ctx    context.Context
	frame  Frame
	result chan sendReplyResult
}

type sendReplyResult struct {
	frame Frame
	err   error
}

type replyAck struct {
	frame Frame
	err   error
}

type WSConnectionManager struct {
	logger Logger

	wsURL                string
	heartbeatInterval    time.Duration
	reconnectBaseDelay   time.Duration
	maxReconnectAttempts int
	reconnectMaxDelay    time.Duration
	replyAckTimeout      time.Duration
	maxReplyQueueSize    int

	mu                sync.RWMutex
	writeMu           sync.Mutex
	conn              *websocket.Conn
	manualClose       bool
	reconnecting      bool
	reconnectAttempts int
	missedPongCount   int
	maxMissedPong     int
	receiveCancel     context.CancelFunc
	heartbeatCancel   context.CancelFunc
	botID             string
	botSecret         string

	replyMu          sync.Mutex
	replyQueues      map[string][]*replyQueueItem
	pendingAcks      map[string]chan replyAck
	processingQueues map[string]bool

	onConnected     func()
	onAuthenticated func()
	onDisconnected  func(string)
	onMessage       func(Frame)
	onReconnecting  func(int)
	onError         func(error)
}

func NewWSConnectionManager(logger Logger, heartbeatInterval, reconnectBaseDelay time.Duration, maxReconnectAttempts int, wsURL string) *WSConnectionManager {
	if wsURL == "" {
		wsURL = DefaultWSURL
	}

	return &WSConnectionManager{
		logger:               logger,
		wsURL:                wsURL,
		heartbeatInterval:    heartbeatInterval,
		reconnectBaseDelay:   reconnectBaseDelay,
		maxReconnectAttempts: maxReconnectAttempts,
		reconnectMaxDelay:    30 * time.Second,
		replyAckTimeout:      5 * time.Second,
		maxReplyQueueSize:    100,
		maxMissedPong:        2,
		replyQueues:          make(map[string][]*replyQueueItem),
		pendingAcks:          make(map[string]chan replyAck),
		processingQueues:     make(map[string]bool),
	}
}

func (m *WSConnectionManager) SetCallbacks(onConnected func(), onAuthenticated func(), onDisconnected func(string), onMessage func(Frame), onReconnecting func(int), onError func(error)) {
	m.onConnected = onConnected
	m.onAuthenticated = onAuthenticated
	m.onDisconnected = onDisconnected
	m.onMessage = onMessage
	m.onReconnecting = onReconnecting
	m.onError = onError
}

func (m *WSConnectionManager) SetCredentials(botID, botSecret string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.botID = botID
	m.botSecret = botSecret
}

func (m *WSConnectionManager) Connect(ctx context.Context) error {
	ctx = ensureContext(ctx)

	if err := m.cleanupConn(); err != nil {
		return err
	}

	cfg, err := websocket.NewConfig(m.wsURL, websocketOrigin(m.wsURL))
	if err != nil {
		return err
	}
	cfg.TlsConfig = &tls.Config{MinVersion: tls.VersionTLS12}
	cfg.Dialer = &net.Dialer{Timeout: 10 * time.Second}

	type dialResult struct {
		conn *websocket.Conn
		err  error
	}
	dialCh := make(chan dialResult, 1)
	go func() {
		conn, dialErr := websocket.DialConfig(cfg)
		dialCh <- dialResult{conn: conn, err: dialErr}
	}()

	var conn *websocket.Conn
	select {
	case <-ctx.Done():
		return ctx.Err()
	case result := <-dialCh:
		if result.err != nil {
			return result.err
		}
		conn = result.conn
	}

	receiveCtx, receiveCancel := context.WithCancel(context.Background())

	m.mu.Lock()
	m.conn = conn
	m.receiveCancel = receiveCancel
	m.reconnectAttempts = 0
	m.missedPongCount = 0
	m.manualClose = false
	m.reconnecting = false
	m.mu.Unlock()

	m.logger.Info("WebSocket connection established, sending auth...")
	if m.onConnected != nil {
		m.onConnected()
	}

	go m.receiveLoop(receiveCtx, conn)

	if err := m.sendAuth(); err != nil {
		_ = m.cleanupConn()
		return err
	}

	return nil
}

func (m *WSConnectionManager) sendAuth() error {
	m.mu.RLock()
	botID := m.botID
	botSecret := m.botSecret
	m.mu.RUnlock()

	frame := Frame{
		Cmd:     CmdSubscribe,
		Headers: FrameHeaders{ReqID: GenerateReqID(CmdSubscribe)},
		Body: map[string]any{
			"bot_id": botID,
			"secret": botSecret,
		},
	}

	if err := m.Send(frame); err != nil {
		m.logger.Error("Failed to send auth frame: %v", err)
		return err
	}

	m.logger.Info("Auth frame sent")
	return nil
}

func (m *WSConnectionManager) receiveLoop(ctx context.Context, conn *websocket.Conn) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		var frame Frame
		if err := websocket.JSON.Receive(conn, &frame); err != nil {
			if ctx.Err() != nil {
				return
			}

			reason := err.Error()
			m.logger.Warn("WebSocket connection closed: %s", reason)
			m.handleConnectionClosed(conn, fmt.Sprintf("WebSocket connection closed (%s)", reason))
			return
		}

		m.handleFrame(frame)
	}
}

func (m *WSConnectionManager) handleFrame(frame Frame) {
	switch frame.Cmd {
	case CmdCallback:
		m.logger.Debug("Received push message: %s", truncateJSON(frame.Body))
		if m.onMessage != nil {
			m.onMessage(frame)
		}
		return
	case CmdEventCallback:
		m.logger.Debug("Received event callback: %s", truncateJSON(frame.Body))
		if m.onMessage != nil {
			m.onMessage(frame)
		}
		return
	}

	reqID := frame.ReqID()
	if m.handleReplyAck(reqID, frame) {
		return
	}

	if strings.HasPrefix(reqID, CmdSubscribe) {
		if frame.ErrCode != 0 {
			err := fmt.Errorf("authentication failed: %s (code: %d)", frame.ErrMsg, frame.ErrCode)
			m.logger.Error("Authentication failed: errcode=%d, errmsg=%s", frame.ErrCode, frame.ErrMsg)
			if m.onError != nil {
				m.onError(err)
			}
			return
		}

		m.logger.Info("Authentication successful")
		m.startHeartbeat()
		if m.onAuthenticated != nil {
			m.onAuthenticated()
		}
		return
	}

	if strings.HasPrefix(reqID, CmdHeartbeat) {
		if frame.ErrCode != 0 {
			m.logger.Warn("Heartbeat ack error: errcode=%d, errmsg=%s", frame.ErrCode, frame.ErrMsg)
			return
		}

		m.mu.Lock()
		m.missedPongCount = 0
		m.mu.Unlock()
		m.logger.Debug("Received heartbeat ack")
		return
	}

	m.logger.Warn("Received unknown frame: %s", truncateJSON(frame))
	if m.onMessage != nil {
		m.onMessage(frame)
	}
}

func (m *WSConnectionManager) startHeartbeat() {
	m.stopHeartbeat()

	ctx, cancel := context.WithCancel(context.Background())
	m.mu.Lock()
	m.heartbeatCancel = cancel
	m.mu.Unlock()

	go m.heartbeatLoop(ctx)
	m.logger.Debug("Heartbeat timer started, interval: %s", m.heartbeatInterval)
}

func (m *WSConnectionManager) stopHeartbeat() {
	m.mu.Lock()
	cancel := m.heartbeatCancel
	m.heartbeatCancel = nil
	m.mu.Unlock()

	if cancel != nil {
		cancel()
		m.logger.Debug("Heartbeat timer stopped")
	}
}

func (m *WSConnectionManager) heartbeatLoop(ctx context.Context) {
	ticker := time.NewTicker(m.heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := m.sendHeartbeat(); err != nil {
				m.logger.Error("Failed to send heartbeat: %v", err)
			}
		}
	}
}

func (m *WSConnectionManager) sendHeartbeat() error {
	m.mu.Lock()
	if m.missedPongCount >= m.maxMissedPong {
		m.mu.Unlock()
		m.logger.Warn("No heartbeat ack received for %d consecutive pings, connection considered dead", m.maxMissedPong)
		if conn := m.currentConn(); conn != nil {
			_ = conn.Close()
		}
		return nil
	}

	m.missedPongCount++
	missed := m.missedPongCount
	m.mu.Unlock()

	frame := Frame{
		Cmd:     CmdHeartbeat,
		Headers: FrameHeaders{ReqID: GenerateReqID(CmdHeartbeat)},
	}
	if err := m.Send(frame); err != nil {
		return err
	}

	if missed > 1 {
		m.logger.Debug("Heartbeat sent (awaiting %d pong)", missed)
	} else {
		m.logger.Debug("Heartbeat sent")
	}
	return nil
}

func (m *WSConnectionManager) Send(frame Frame) error {
	conn := m.currentConn()
	if conn == nil {
		return fmt.Errorf("websocket not connected, unable to send data")
	}

	m.writeMu.Lock()
	defer m.writeMu.Unlock()
	return websocket.JSON.Send(conn, frame)
}

func (m *WSConnectionManager) SendReply(ctx context.Context, reqID string, body map[string]any, cmd string) (Frame, error) {
	ctx = ensureContext(ctx)

	item := &replyQueueItem{
		ctx: ctx,
		frame: Frame{
			Cmd:     cmd,
			Headers: FrameHeaders{ReqID: reqID},
			Body:    body,
		},
		result: make(chan sendReplyResult, 1),
	}

	m.replyMu.Lock()
	queue := m.replyQueues[reqID]
	if len(queue) >= m.maxReplyQueueSize {
		m.replyMu.Unlock()
		return Frame{}, fmt.Errorf("reply queue for reqId %s exceeds max size (%d)", reqID, m.maxReplyQueueSize)
	}

	m.replyQueues[reqID] = append(queue, item)
	if !m.processingQueues[reqID] {
		m.processingQueues[reqID] = true
		go m.processReplyQueue(reqID)
	}
	m.replyMu.Unlock()

	select {
	case <-ctx.Done():
		return Frame{}, ctx.Err()
	case result := <-item.result:
		return result.frame, result.err
	}
}

func (m *WSConnectionManager) processReplyQueue(reqID string) {
	defer func() {
		m.replyMu.Lock()
		delete(m.processingQueues, reqID)
		if len(m.replyQueues[reqID]) == 0 {
			delete(m.replyQueues, reqID)
		}
		m.replyMu.Unlock()
	}()

	for {
		m.replyMu.Lock()
		queue := m.replyQueues[reqID]
		if len(queue) == 0 {
			m.replyMu.Unlock()
			return
		}
		item := queue[0]
		m.replyMu.Unlock()

		if err := item.ctx.Err(); err != nil {
			m.popReplyQueueHead(reqID, item)
			deliverSendReplyResult(item.result, sendReplyResult{err: err})
			continue
		}

		if err := m.Send(item.frame); err != nil {
			m.logger.Error("Failed to send reply for reqId %s: %v", reqID, err)
			m.popReplyQueueHead(reqID, item)
			deliverSendReplyResult(item.result, sendReplyResult{err: err})
			continue
		}

		m.logger.Debug("Reply message sent via WebSocket, reqId: %s", reqID)

		ackCh := make(chan replyAck, 1)
		m.replyMu.Lock()
		m.pendingAcks[reqID] = ackCh
		m.replyMu.Unlock()

		timer := time.NewTimer(m.replyAckTimeout)
		var result sendReplyResult

		select {
		case ack := <-ackCh:
			result = sendReplyResult{frame: ack.frame, err: ack.err}
		case <-timer.C:
			result = sendReplyResult{err: fmt.Errorf("reply ack timeout (%s) for reqId: %s", m.replyAckTimeout, reqID)}
		case <-item.ctx.Done():
			result = sendReplyResult{err: item.ctx.Err()}
		}

		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}

		m.replyMu.Lock()
		if ch, ok := m.pendingAcks[reqID]; ok && ch == ackCh {
			delete(m.pendingAcks, reqID)
		}
		m.replyMu.Unlock()

		m.popReplyQueueHead(reqID, item)
		deliverSendReplyResult(item.result, result)
	}
}

func (m *WSConnectionManager) popReplyQueueHead(reqID string, expected *replyQueueItem) {
	m.replyMu.Lock()
	defer m.replyMu.Unlock()

	queue := m.replyQueues[reqID]
	if len(queue) == 0 {
		return
	}
	if queue[0] != expected {
		return
	}

	m.replyQueues[reqID] = queue[1:]
	if len(m.replyQueues[reqID]) == 0 {
		delete(m.replyQueues, reqID)
	}
}

func (m *WSConnectionManager) handleReplyAck(reqID string, frame Frame) bool {
	if reqID == "" {
		return false
	}

	m.replyMu.Lock()
	ackCh, ok := m.pendingAcks[reqID]
	if ok {
		delete(m.pendingAcks, reqID)
	}
	m.replyMu.Unlock()

	if !ok {
		return false
	}

	if frame.ErrCode != 0 {
		m.logger.Warn("Reply ack error: reqId=%s, errcode=%d, errmsg=%s", reqID, frame.ErrCode, frame.ErrMsg)
		deliverReplyAck(ackCh, replyAck{err: fmt.Errorf("reply ack error: errcode=%d, errmsg=%s", frame.ErrCode, frame.ErrMsg)})
		return true
	}

	m.logger.Debug("Reply ack received for reqId: %s", reqID)
	deliverReplyAck(ackCh, replyAck{frame: frame})
	return true
}

func (m *WSConnectionManager) clearPendingMessages(reason string) {
	m.replyMu.Lock()

	pending := make([]chan replyAck, 0, len(m.pendingAcks))
	for _, ch := range m.pendingAcks {
		pending = append(pending, ch)
	}

	queueItems := make([]*replyQueueItem, 0)
	for _, queue := range m.replyQueues {
		queueItems = append(queueItems, queue...)
	}

	m.pendingAcks = make(map[string]chan replyAck)
	m.replyQueues = make(map[string][]*replyQueueItem)
	m.processingQueues = make(map[string]bool)
	m.replyMu.Unlock()

	for _, ch := range pending {
		deliverReplyAck(ch, replyAck{err: errors.New(reason)})
	}
	for _, item := range queueItems {
		deliverSendReplyResult(item.result, sendReplyResult{
			err: fmt.Errorf("%s, reply for reqId: %s cancelled", reason, item.frame.ReqID()),
		})
	}
}

func (m *WSConnectionManager) Disconnect() error {
	m.mu.Lock()
	m.manualClose = true
	m.mu.Unlock()

	m.stopHeartbeat()
	m.clearPendingMessages("Connection manually closed")
	if err := m.cleanupConn(); err != nil {
		return err
	}

	m.logger.Info("WebSocket connection manually closed")
	return nil
}

func (m *WSConnectionManager) cleanupConn() error {
	m.mu.Lock()
	conn := m.conn
	m.conn = nil
	receiveCancel := m.receiveCancel
	m.receiveCancel = nil
	heartbeatCancel := m.heartbeatCancel
	m.heartbeatCancel = nil
	m.mu.Unlock()

	if receiveCancel != nil {
		receiveCancel()
	}
	if heartbeatCancel != nil {
		heartbeatCancel()
	}
	if conn != nil {
		return conn.Close()
	}
	return nil
}

func (m *WSConnectionManager) handleConnectionClosed(conn *websocket.Conn, reason string) {
	m.stopHeartbeat()
	m.clearPendingMessages(reason)

	m.mu.Lock()
	if m.conn == conn {
		m.conn = nil
	}
	manualClose := m.manualClose
	if cancel := m.receiveCancel; cancel != nil {
		cancel()
		m.receiveCancel = nil
	}
	m.mu.Unlock()

	if m.onDisconnected != nil {
		m.onDisconnected(strings.TrimPrefix(reason, "WebSocket connection closed "))
	}
	if !manualClose {
		m.scheduleReconnect()
	}
}

func (m *WSConnectionManager) scheduleReconnect() {
	m.mu.Lock()
	if m.manualClose || m.reconnecting {
		m.mu.Unlock()
		return
	}

	if m.maxReconnectAttempts != -1 && m.reconnectAttempts >= m.maxReconnectAttempts {
		m.mu.Unlock()
		err := fmt.Errorf("max reconnect attempts exceeded")
		m.logger.Error("Max reconnect attempts reached (%d), giving up", m.maxReconnectAttempts)
		if m.onError != nil {
			m.onError(err)
		}
		return
	}

	m.reconnectAttempts++
	attempt := m.reconnectAttempts
	m.reconnecting = true
	m.mu.Unlock()

	delay := m.reconnectBaseDelay * time.Duration(1<<(attempt-1))
	if delay > m.reconnectMaxDelay {
		delay = m.reconnectMaxDelay
	}

	m.logger.Info("Reconnecting in %s (attempt %d)...", delay, attempt)
	if m.onReconnecting != nil {
		m.onReconnecting(attempt)
	}

	go func() {
		timer := time.NewTimer(delay)
		defer timer.Stop()
		<-timer.C

		m.mu.Lock()
		if m.manualClose {
			m.reconnecting = false
			m.mu.Unlock()
			return
		}
		m.reconnecting = false
		m.mu.Unlock()

		if err := m.Connect(context.Background()); err != nil {
			m.logger.Error("Reconnect failed: %v", err)
			if m.onError != nil {
				m.onError(err)
			}
			m.scheduleReconnect()
		}
	}()
}

func (m *WSConnectionManager) IsConnected() bool {
	return m.currentConn() != nil
}

func (m *WSConnectionManager) currentConn() *websocket.Conn {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.conn
}

func websocketOrigin(rawURL string) string {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return "https://openws.work.weixin.qq.com"
	}

	switch parsed.Scheme {
	case "ws":
		parsed.Scheme = "http"
	case "wss":
		parsed.Scheme = "https"
	default:
		parsed.Scheme = "https"
	}
	parsed.Path = "/"
	parsed.RawQuery = ""
	parsed.Fragment = ""
	return parsed.String()
}

func ensureContext(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return ctx
}

func deliverReplyAck(ch chan replyAck, ack replyAck) {
	select {
	case ch <- ack:
	default:
	}
}

func deliverSendReplyResult(ch chan sendReplyResult, result sendReplyResult) {
	select {
	case ch <- result:
	default:
	}
}

func truncateJSONFrame(frame Frame) string {
	raw, err := json.Marshal(frame)
	if err != nil {
		return "<invalid-json>"
	}
	if len(raw) > 200 {
		return string(raw[:200])
	}
	return string(raw)
}
