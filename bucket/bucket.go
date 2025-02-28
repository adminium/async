package bucket

import (
	"fmt"
	"github.com/adminium/async"
	"sync"
	"sync/atomic"
	"time"
)

type Handler[T any] func(data []T)

func NewBucket[T any](threshold uint, interval time.Duration, handler Handler[T]) *Bucket[T] {
	return &Bucket[T]{
		data:      make([]T, 0),
		done:      make(chan struct{}, 1),
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
	once      sync.Once
	log       async.ILogger
	count     atomic.Int32
}

func (b *Bucket[T]) SetLog(log async.ILogger) {
	b.log = log
}

func (b *Bucket[T]) Infof(template string, args ...interface{}) {
	if b.log == nil {
		return
	}
	b.log.Infof(template, args...)
}

func (b *Bucket[T]) Stop() {
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
	b.count.Add(1)

	return
}

func (b *Bucket[T]) Start() {
	b.once.Do(func() {
		defer func() {
			b.closed = true
			close(b.done)
			b.Infof("close bucket")
		}()

		b.closed = false
		b.now = time.Now()
		timer := time.NewTimer(b.interval)
		defer func() {
			timer.Stop()
		}()

		b.Infof("start bucket, threshold: %d, interval: %s", b.threshold, b.interval)

		for {
			select {
			case <-b.done:
				return
			case <-timer.C:
				b.process(timer)
			default:
				if uint(b.count.Load()) >= b.threshold {
					b.process(timer)
				}
			}
		}
	})
}

func (b *Bucket[T]) process(timer *time.Timer) {

	b.lock.Lock()
	defer b.lock.Unlock()

	data := make([]T, len(b.data))
	copy(data, b.data)
	b.data = make([]T, 0)
	b.count.Store(0)

	if b.handler != nil && len(data) > 0 {
		b.handler(data)
	}

	b.now = time.Now()
	timer.Reset(b.interval)
}
