package s3

import (
	"fmt"
	"sync"

	"github.com/xjayleex/minari-libs/api/proto/messages"
	"github.com/xjayleex/minari-libs/logpack"
	"github.com/xjayleex/minari/shipper/queue"
)

type S3Output struct {
	logger logpack.Logger
	wg     sync.WaitGroup
	config *Config
	queue  *queue.Queue
}

func NewS3Output(config *Config, queue *queue.Queue) *S3Output {
	logger, err := logpack.NewLogger("s3compat-output")
	if err != nil {
		logger = logpack.G()
	}
	return &S3Output{
		logger: logger,
		wg:     sync.WaitGroup{},
		config: config,
		queue:  queue,
	}
}

func (out *S3Output) Start() error {
	q := out.queue
	s3, err := makeS3Client(*out.config)
	if err != nil {
		return fmt.Errorf("error creating s3-compat storage client %w", err)
	}

	out.wg.Add(1)
	go func() {
		defer out.wg.Done()
		for {
			batch, err := q.Get(10)
			if err != nil {
				//TODO:
			}
			events := batch.Events()

		}
	}()

	return nil
}

func (out *S3Output) targetBucketEvents(event *messages.Event) string {
	// TODO: bucket naming convention from event meta
	return "fixme"
}

func (out *S3Output) WaitClose() {
	out.wg.Wait()
}
