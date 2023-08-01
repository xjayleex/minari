package pubserver

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/xjayleex/minari-libs/api/proto/messages"
	"github.com/xjayleex/minari-libs/logpack"
	"github.com/xjayleex/minari/shipper/config"
	"github.com/xjayleex/minari/shipper/output"
	"github.com/xjayleex/minari/shipper/queue"
)

type Output interface {
	Start() error
	WaitClose()
}

func OutputFromConfig(cfg output.Config, q *queue.Queue) (Output, error) {
	if cfg.Console != nil {
		return output.NewConsoleOutput(q), nil
	}

	return nil, errors.New("no available output")
}

type PubServerRunner struct {
	logger logpack.Logger

	queueLock sync.Mutex
	queue     *queue.Queue
	// monitoring *monitoring.QueueMonitor
	monitoring interface{}
	output     Output
}

func NewOutputServer() *PubServerRunner {
	return &PubServerRunner{
		logger:    logpack.G(),
		queueLock: sync.Mutex{},
	}
}

func (r *PubServerRunner) Start(cfg config.ShipperRootConfig) (err error) {
	r.logger.Debugf("trying to initialize queue.")
	r.queue, err = queue.New(cfg.Shipper.Queue)
	if err != nil {
		return fmt.Errorf("could not create queue: %w", err)
	}

	r.logger.Debugf("succeed on initializing queue.")
	// TODO : Queue monitoring codes.

	r.output, err = OutputFromConfig(cfg.Shipper.Output, r.queue)
	if err != nil {
		return fmt.Errorf("error on proceeding shipper output confg: %w", err)
	}
	r.logger.Debugf("output is initialized")

	err = r.output.Start()
	if err != nil {
		return fmt.Errorf("could not start output: %w", err)
	}
	r.logger.Debugf("started shipper output server")
	return nil
}

func (r *PubServerRunner) Close() error {
	r.queueLock.Lock()
	defer r.queueLock.Unlock()
	if r.monitoring != nil {
		// TODO
	}

	if r.queue != nil {
		r.logger.Debugf("try to stopping queue...")
		err := r.queue.Close()
		if err != nil {
			r.logger.Error(err)
		}
		r.queue = nil
		r.logger.Debugf("made queue to be stopped.")
	}

	if r.output != nil {
		// The output will shutdown once the queue is close.
		// We call Wait to give it a chance to finish with events it has already read.
		r.logger.Debugf("wait for output shutdown")
		r.output.WaitClose()
		r.output = nil
		r.logger.Debugf("all pending events are flushed")
	}
	r.logger.Debugf("output has stopped")
	return nil
}

func (r *PubServerRunner) IsInitialized() bool {
	r.queueLock.Lock()
	defer r.queueLock.Unlock()
	return r.queue != nil
}

func (r *PubServerRunner) Publish(ctx context.Context, event *messages.Event) (queue.EntryID, error) {
	r.queueLock.Lock()
	defer r.queueLock.Unlock()
	if r.queue == nil {
		return 0, fmt.Errorf("shipper is initializing")
	}
	return r.queue.Publish(ctx, event)
}

func (r *PubServerRunner) TryPublish(event *messages.Event) (queue.EntryID, error) {
	r.queueLock.Lock()
	defer r.queueLock.Unlock()
	if r.queue == nil {
		return 0, fmt.Errorf("shipper is initializing")
	}
	return r.queue.TryPublish(event)
}

func (r *PubServerRunner) PersistedIndex() (queue.EntryID, error) {
	r.queueLock.Lock()
	defer r.queueLock.Unlock()
	if r.queue == nil {
		return 0, fmt.Errorf("shipper is initializing")
	}

	return r.queue.PersistedIndex()
}
