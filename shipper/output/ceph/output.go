package ceph

import (
	"errors"
	"sync"

	"github.com/xjayleex/minari-libs/logpack"
	"github.com/xjayleex/minari/shipper/queue"
)

type CephOutput struct {
	logger logpack.Logger
	wg     sync.WaitGroup
	queue  *queue.Queue
}

func NewCephOutput(config *Config, queue *queue.Queue) *CephOutput {
	logger, err := logpack.NewLogger("ceph-output")
	if err != nil {
		logger = logpack.G()
	}
	return &CephOutput{
		logger: logger,
		wg:     sync.WaitGroup{},
		queue:  queue,
	}
}

func (out *CephOutput) Start() error {
	out.wg.Add(1)
	go func() {
		defer out.wg.Done()
		for {

		}
	}
	return errors.New("TODO:")
}
