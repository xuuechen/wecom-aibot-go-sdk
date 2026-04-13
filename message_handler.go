package aibot

import (
	"encoding/json"
	"fmt"
)

type MessageHandler struct {
	logger Logger
}

func NewMessageHandler(logger Logger) *MessageHandler {
	return &MessageHandler{logger: logger}
}

func (h *MessageHandler) HandleFrame(frame Frame, emitter *Emitter) {
	body := frame.Body
	if len(body) == 0 {
		h.logger.Warn("Received invalid message format: %s", truncateJSON(frame))
		return
	}

	msgType, _ := body["msgtype"].(string)
	if msgType == "" {
		h.logger.Warn("Received invalid message format: %s", truncateJSON(frame))
		return
	}

	if frame.Cmd == CmdEventCallback {
		h.handleEventCallback(frame, emitter)
		return
	}

	h.handleMessageCallback(frame, emitter)
}

func (h *MessageHandler) handleMessageCallback(frame Frame, emitter *Emitter) {
	emitter.Emit(Event{Name: "message", Frame: &frame})

	switch frame.Body["msgtype"] {
	case MessageTypeText:
		emitter.Emit(Event{Name: "message.text", Frame: &frame})
	case MessageTypeImage:
		emitter.Emit(Event{Name: "message.image", Frame: &frame})
	case MessageTypeMixed:
		emitter.Emit(Event{Name: "message.mixed", Frame: &frame})
	case MessageTypeVoice:
		emitter.Emit(Event{Name: "message.voice", Frame: &frame})
	case MessageTypeFile:
		emitter.Emit(Event{Name: "message.file", Frame: &frame})
	default:
		h.logger.Debug("Received unhandled message type: %v", frame.Body["msgtype"])
	}
}

func (h *MessageHandler) handleEventCallback(frame Frame, emitter *Emitter) {
	emitter.Emit(Event{Name: "event", Frame: &frame})

	eventData, _ := frame.Body["event"].(map[string]any)
	eventType, _ := eventData["eventtype"].(string)
	if eventType == "" {
		h.logger.Debug("Received event callback without eventtype: %s", truncateJSON(frame.Body))
		return
	}

	emitter.Emit(Event{Name: fmt.Sprintf("event.%s", eventType), Frame: &frame})
}

func truncateJSON(value any) string {
	raw, err := json.Marshal(value)
	if err != nil {
		return "<invalid-json>"
	}
	if len(raw) > 200 {
		return string(raw[:200])
	}
	return string(raw)
}
