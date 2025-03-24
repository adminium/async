package stream

type listener struct {
	name    string
	handler func(event any)
}

type Event struct {
	Name string
	Data any
}

func NewEventStream(cap int) *EventStream {
	return &EventStream{
		events:    make(chan Event, cap),
		listeners: make(chan *listener),
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

func (p *EventStream) Emit(name string, data any) {
	p.events <- Event{
		Name: name,
		Data: data,
	}
}

func (p *EventStream) On(name string, handler func(data any)) {
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
			for _, l := range listeners[e.Name] {
				l.handler(e.Data)
			}
		}
	}
}
