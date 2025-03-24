package stream

type listener[T any] struct {
	name    string
	handler func(event T)
}
type Event[T any] struct {
	Name string
	Data T
}

func NewEventStream[T any](cap int) *EventStream[T] {
	return &EventStream[T]{
		events:    make(chan Event[T], cap),
		listeners: make(chan *listener[T]),
	}
}

type EventStream[T any] struct {
	events    chan Event[T]
	listeners chan *listener[T]
}

func (p *EventStream[T]) Close() {
	close(p.events)
	close(p.listeners)
}

func (p *EventStream[T]) Emit(name string, data T) {
	p.events <- Event[T]{
		Name: name,
		Data: data,
	}
}

func (p *EventStream[T]) On(name string, handler func(data T)) {
	p.listeners <- &listener[T]{
		name:    name,
		handler: handler,
	}
}

func (p *EventStream[T]) Run() {
	listeners := make(map[string][]*listener[T])
	for {
		select {
		case l, ok := <-p.listeners:
			if !ok {
				return
			}
			listeners[l.name] = append(listeners[l.name], l)
		case e, ok := <-p.events:
			if !ok {
				return
			}
			for _, l := range listeners[e.Name] {
				l.handler(e.Data)
			}
		}
	}
}
