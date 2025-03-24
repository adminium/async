package stream

import (
	"testing"
	"time"
)

var _ Event = (*OddNumber)(nil)
var _ Event = (*EvenNumber)(nil)

type OddNumber struct {
	Value int
}

func (o OddNumber) EventName() string {
	return "odd"
}

type EvenNumber struct {
	Value int
}

func (e EvenNumber) EventName() string {
	return "even"
}

func TestEventStream(t *testing.T) {

	es := NewEventStream(2)
	es.On("odd", func(data Event) {
		e := data.(OddNumber)
		t.Log("odd number: ", e.Value)
	})
	es.On("odd", func(data Event) {
		e := data.(OddNumber)
		t.Log("odd number2: ", e.Value)
	})
	go es.Run()
	time.Sleep(time.Second)
	for i := 0; i < 100; i++ {
		if i%2 == 0 {
			es.Emit(EvenNumber{Value: i})
		} else {
			es.Emit(OddNumber{Value: i})
		}
	}

	es.Close()

	time.Sleep(time.Second)
}
