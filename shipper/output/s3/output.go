package s3

import (
	"errors"
	"sync"

	"github.com/xjayleex/minari-libs/logpack"
	"github.com/xjayleex/minari/shipper/queue"
)

type S3Output struct {
	logger logpack.Logger
	wg     sync.WaitGroup
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
		queue:  queue,
	}
}

func (out *S3Output) Start() error {
	out.wg.Add(1)
	go func() {
		defer out.wg.Done()
		for {

		}
	}()
	return errors.New("TODO:")
}
