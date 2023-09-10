package redis

import (
	"sync"

	"github.com/xjayleex/minari-libs/logpack"
	"github.com/xjayleex/minari/shipper/queue"
)

func makeRedisStream(config Config) (*Client, error) {
	// return newRedisStreamClient(interfaae)
	return nil, nil
}

type Config struct {
	Consumer ConsumerConfig `config:"consumer"`
}

type RedisStreamOutput struct {
	logger logpack.Logger

	config *Config
	queue  *queue.Queue

	wg sync.WaitGroup
}

func NewRedisStream(config *Config, queue *queue.Queue) *RedisStreamOutput {
	logger, err := logpack.NewLogger("redis-stream-output")
	// TODO: logpack lib needs to be modified so taht logpack.NewLogger does not return err.
	if err != nil {
		logger = logpack.G()
	}

	out := &RedisStreamOutput{
		logger: logger,
		config: config,
		queue:  queue,
	}
	return out
}

func (out *RedisStreamOutput) Start() error {
	client, err := makeRedisStream(*out.config)
	if err != nil {
		out.logger.Errorf("Unable to make redis stream output")
		return err
	}

	err = client.Connect()
	if err != nil {
		out.logger.Errorf("Unable to connect to client :%v, err")
		return err
	}

	out.wg.Add(1)
	go func() {
		var size int = 1000
		defer out.wg.Done()

		for {
			batch, err := out.queue.Get(size)
			if err != nil {
				out.logger.Error("Shutting redis stream output down, queue closed: %v", err)
				break
			}
			events := batch.Events()
			remaining := uint64(len(events))
			for len(events) > 0 {
				out.logger.Debugf("Sending %d events to redis stream client to publsh", len(events))
				events, _ = client.produceEvents(events)
				completed := remaining - uint64(len(events))
				remaining = uint64(len(events))
				batch.Done(completed)
				// TODO: error handling or retry with backoff?
				out.logger.Debugf("Finished sending batch with %v errors", len(events))
			}

			if remaining > 0 {
				// TODO: this condition(remaining > 0) can not be reached.
				batch.Done(remaining)
			}

		}
	}()

	return nil
}

func (r *RedisStreamOutput) WaitClose() {
	r.wg.Wait()
}
