package stream

type listener struct {
	name    string
	handler func(event Event)
}

type Event interface {
	EventName() string
}

func NewEventStream(cap int) *EventStream {
	return &EventStream{
		events:    make(chan Event, cap),
		listeners: make(chan *listener, 1024),
	}
}

type EventStream struct {
	events    chan Event
	listeners chan *listener
}

func (p *EventStream) Close() {
	close(p.events)
	close(p.listeners)
}

func (p *EventStream) Emit(event Event) {
	p.events <- event
}

func (p *EventStream) On(name string, handler func(event Event)) {
	p.listeners <- &listener{
		name:    name,
		handler: handler,
	}
}

func (p *EventStream) Run() {
	listeners := make(map[string][]*listener)
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
			for _, l := range listeners[e.EventName()] {
				l.handler(e)
			}
		}
	}
}
