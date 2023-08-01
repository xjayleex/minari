package queue

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/xjayleex/minari-libs/api/proto/messages"
	"github.com/xjayleex/minari-libs/logpack"
	libqueue "github.com/xjayleex/minari-libs/queue"
	"github.com/xjayleex/minari-libs/queue/memqueue"
)

var ErrQueueClosed = fmt.Errorf("could not publish events: queue is closed")
var ErrQueueFull = fmt.Errorf("could not publish events: queue is full")

type EntryID libqueue.EntryID
type Metrics libqueue.Metrics

type Queue struct {
	config     Config
	eventQueue libqueue.Queue
	producer   libqueue.Producer
}

func New(c Config) (*Queue, error) {
	// FIXME with disk queue setting
	// NOTE : below logger is incorrect, originally using global logger
	logger, err := logpack.NewLogger("queue")
	if err != nil {
		return nil, err
	}
	var eventQueue libqueue.Queue
	// shallow copy for memqueue settings
	mc := *c.MemSettings
	eventQueue = memqueue.New(logger, mc)
	// DELETEME : for test below
	waiter := &producerACKWaiter{}
	producer := eventQueue.Producer(libqueue.ProducerConfig{
		ACK: waiter.ack,
		OnDrop: func(interface{}) {
		},
		DropOnCancel: false,
	})
	//producer := eventQueue.Producer(libqueue.ProducerConfig{})
	return &Queue{
		config:     c,
		eventQueue: eventQueue,
		producer:   producer,
	}, nil

}

func (queue *Queue) Close() error {
	return queue.eventQueue.Close()
}

func (queue *Queue) Publish(ctx context.Context, event *messages.Event) (EntryID, error) {
	// TODO pass the real channel once libbeat supports it
	id, published := queue.producer.Publish(event /*, ctx.Done()*/)
	if !published {
		return EntryID(0), ErrQueueClosed
	}
	return EntryID(id), nil
}

func (queue *Queue) TryPublish(event *messages.Event) (EntryID, error) {
	id, published := queue.producer.TryPublish(event)
	if !published {
		return EntryID(0), ErrQueueFull
	}
	return EntryID(id), nil
}

func (queue *Queue) Get(size int) (*WrappedBatch, error) {
	batch, err := queue.eventQueue.Get(size)
	if err != nil {
		return nil, err
	}
	return &WrappedBatch{batch: batch}, nil
}

func (queue *Queue) Metrics() (Metrics, error) {
	// TODO
	return Metrics{}, errors.New("Metrics() is not implemented yet")
}

func (queue *Queue) PersistedIndex() (EntryID, error) {
	if queue.config.useDiskQueue() {
		// TODO
		return EntryID(0), nil
	} else {
		metrics, err := queue.eventQueue.Metrics()
		if err != nil {
			return EntryID(0), err
		}
		// When a memory queue event is persisted, it is removed from the queue,
		// so we return the oldest ramaining entry ID.
		return EntryID(metrics.OldestEntryID), nil
	}
}

type WrappedBatch struct {
	batch          libqueue.Batch
	doneCounts     uint64
	CompletionHook func()
}

func (b *WrappedBatch) Events() []*messages.Event {
	events := make([]*messages.Event, b.batch.Count())
	for i := 0; i < b.batch.Count(); i++ {
		events[i], _ = b.batch.Entry(i).(*messages.Event)
	}
	return events
}

func (b *WrappedBatch) Done(count uint64) {
	if atomic.AddUint64(&b.doneCounts, count) >= uint64(b.batch.Count()) {
		b.batch.Done()
		if b.CompletionHook != nil {
			b.CompletionHook()
		}
	}
}

// producerACKWaiter is a helper that can listen to queue producer callbacks
// and wait on them from the test thread, so we can test the queue's asynchronous
// behavior without relying on time.Sleep.
type producerACKWaiter struct {
	sync.Mutex

	// The number of acks received from a producer callback.
	acked int

	// The number of acks that callers have waited for in waitForEvents.
	waited int

	// When non-nil, this channel is being listened to by a test thread
	// blocking on ACKs, and incoming producer callbacks are forwarded
	// to it.
	ackChan chan int
}

func (w *producerACKWaiter) ack(count int) {
	w.Lock()
	defer w.Unlock()
	w.acked += count
	if w.ackChan != nil {
		w.ackChan <- count
	}
}

func (w *producerACKWaiter) waitForEvents(count int) {
	w.Lock()
	defer w.Unlock()
	if w.ackChan != nil {
		panic("don't call producerACKWaiter.waitForEvents from multiple goroutines")
	}

	avail := w.acked - w.waited
	if count <= avail {
		w.waited += count
		return
	}
	w.waited = w.acked
	count -= avail
	// We have advanced as far as we can, we have to wait for
	// more incoming ACKs.
	// Set a listener and unlock, so ACKs can come in on another
	// goroutine.
	w.ackChan = make(chan int)
	w.Unlock()

	newAcked := 0
	for newAcked < count {
		newAcked += <-w.ackChan
	}
	// When we're done, turn off the listener channel and update
	// the number of events waited on.
	w.Lock()
	w.ackChan = nil
	w.waited += count
}
