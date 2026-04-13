package aibot

import (
	"testing"
	"time"
)

func TestMessageHandlerDispatchesMessageEvents(t *testing.T) {
	handler := NewMessageHandler(NewDefaultLogger("test"))
	emitter := NewEmitter()
	ch := make(chan string, 2)

	emitter.On("message", func(Event) { ch <- "message" })
	emitter.On("message.text", func(Event) { ch <- "message.text" })

	handler.HandleFrame(Frame{
		Cmd: CmdCallback,
		Headers: FrameHeaders{
			ReqID: "req1",
		},
		Body: map[string]any{
			"msgtype": MessageTypeText,
			"text":    map[string]any{"content": "hello"},
		},
	}, emitter)

	assertEvent(t, ch, "message")
	assertEvent(t, ch, "message.text")
}

func TestMessageHandlerDispatchesEventCallbacks(t *testing.T) {
	handler := NewMessageHandler(NewDefaultLogger("test"))
	emitter := NewEmitter()
	ch := make(chan string, 2)

	emitter.On("event", func(Event) { ch <- "event" })
	emitter.On("event.enter_chat", func(Event) { ch <- "event.enter_chat" })

	handler.HandleFrame(Frame{
		Cmd: CmdEventCallback,
		Headers: FrameHeaders{
			ReqID: "req2",
		},
		Body: map[string]any{
			"msgtype": "event",
			"event": map[string]any{
				"eventtype": EventTypeEnterChat,
			},
		},
	}, emitter)

	assertEvent(t, ch, "event")
	assertEvent(t, ch, "event.enter_chat")
}

func assertEvent(t *testing.T, ch <-chan string, expected string) {
	t.Helper()
	select {
	case actual := <-ch:
		if actual != expected {
			t.Fatalf("expected event %s, got %s", expected, actual)
		}
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for event %s", expected)
	}
}
