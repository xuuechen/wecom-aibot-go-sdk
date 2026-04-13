package aibot

import "sync"

type Event struct {
	Name    string
	Frame   *Frame
	Attempt int
	Reason  string
	Error   error
}

type EventHandler func(Event)

type Emitter struct {
	mu       sync.RWMutex
	handlers map[string][]EventHandler
}

func NewEmitter() *Emitter {
	return &Emitter{
		handlers: make(map[string][]EventHandler),
	}
}

func (e *Emitter) On(name string, handler EventHandler) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.handlers[name] = append(e.handlers[name], handler)
}

func (e *Emitter) Emit(event Event) {
	e.mu.RLock()
	handlers := append([]EventHandler(nil), e.handlers[event.Name]...)
	e.mu.RUnlock()

	for _, handler := range handlers {
		handler(event)
	}
}
