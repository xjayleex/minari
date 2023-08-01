package output

import (
	"sync"

	"github.com/xjayleex/minari-libs/api/proto/messages"
	"github.com/xjayleex/minari-libs/logpack"
	"github.com/xjayleex/minari/shipper/queue"
)

var (
	ConsoleBatchSize = 10
)

type ConsoleConfig struct {
	Enabled bool `config:"enabled"`
}

type ConsoleOutput struct {
	wg    sync.WaitGroup
	queue *queue.Queue
	log   logpack.Logger
}

func NewConsoleOutput(queue *queue.Queue) *ConsoleOutput {
	return &ConsoleOutput{
		wg:    sync.WaitGroup{},
		queue: queue,
		log:   logpack.G(),
	}
}

func (out *ConsoleOutput) Start() error {
	out.wg.Add(1)
	go func() {
		defer out.wg.Done()
		for {
			batch, err := out.queue.Get(ConsoleBatchSize)
			if err != nil {
				// queue.Get only failes when the queue was closed,
				// time for the output to shutdown.
				break // return nil
			}
			events := batch.Events()
			for _, event := range events {
				out.send(event)
			}
			batch.Done(uint64(len(events)))

		}
	}()

	return nil
}

func (out *ConsoleOutput) WaitClose() {
	out.wg.Wait()
}

func (out *ConsoleOutput) send(event *messages.Event) {
	out.log.Infof("%v\n", event)
}
