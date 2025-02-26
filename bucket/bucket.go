package bucket

import (
	"fmt"
	"sync"
	"time"
)

type Handler[T any] func(data []T)

func NewBucket[T any](threshold uint, interval time.Duration, handler Handler[T]) *Bucket[T] {
	return &Bucket[T]{
		data:      make([]T, 0),
		threshold: threshold,
		interval:  interval,
		handler:   handler,
	}
}

type Bucket[T any] struct {
	lock      sync.Mutex
	data      []T           // 存放数据的 slice
	interval  time.Duration // 处理时间间隔
	handler   Handler[T]
	done      chan struct{}
	closed    bool
	threshold uint
	now       time.Time
}

func (b *Bucket[T]) Stop() {
	b.lock.Lock()
	defer b.lock.Unlock()

	if b.closed {
		return
	}

	b.closed = true
	b.done <- struct{}{}
}

func (b *Bucket[T]) Left() time.Duration {
	b.lock.Lock()
	defer func() {
		b.lock.Unlock()
	}()
	return b.now.Add(b.interval).Sub(time.Now())
}

func (b *Bucket[T]) Interval() time.Duration {
	return b.interval
}

func (b *Bucket[T]) Len() int {
	b.lock.Lock()
	defer func() {
		b.lock.Unlock()
	}()
	return len(b.data)
}

// Push 仅当桶处于 closed 状态时会报错
func (b *Bucket[T]) Push(data T) (err error) {

	b.lock.Lock()
	defer func() {
		b.lock.Unlock()
	}()

	if b.closed {
		err = fmt.Errorf("bucket has closed")
		return
	}

	b.data = append(b.data, data)

	return
}

func (b *Bucket[T]) Start() {

	defer func() {
		close(b.done)
	}()

	b.closed = false
	b.now = time.Now()
	timer := time.NewTimer(b.interval)
	defer func() {
		timer.Stop()
	}()

	for {
		select {
		case <-b.done:
			b.closed = true
			return
		case <-timer.C:
			b.process(timer)
		default:
			b.lock.Lock()
			l := len(b.data)
			b.lock.Unlock()
			if uint(l) >= b.threshold {
				b.process(timer)
			}
		}
	}
}

func (b *Bucket[T]) process(timer *time.Timer) {

	b.lock.Lock()
	defer b.lock.Unlock()

	data := make([]T, len(b.data))
	copy(data, b.data)
	b.data = make([]T, 0)

	if b.handler != nil && len(data) > 0 {
		b.handler(data)
	}

	b.now = time.Now()
	timer.Reset(b.interval)
}
