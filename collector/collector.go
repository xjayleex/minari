package collector

import (
	"context"
	"errors"
	"sync"

	"github.com/xjayleex/minari-libs/logpack"
	"github.com/xjayleex/minari/collector/source"
	"github.com/xjayleex/minari/lib/config"
)

type Collector struct {
	logger logpack.Logger
	cfg    Config

	Source source.DataSource
}

type Config struct {
	SourceConfig  *config.C
	ShipperConfig *config.C
}

func NewCollector(config Config, logger logpack.Logger) *Collector {
	// c := &Collector{}
	// TODO: generate actual datasource logic
	// source := SourceFromConfig
	// c.source = source
	// c.source.Run()
	c := &Collector{
		cfg:    config,
		logger: logger,
	}
	return c
}

func (c *Collector) Run(ctx context.Context) error {
	src, err := source.Load("", nil, c.cfg.SourceConfig)
	if err != nil {
		return err
	}

	shipper, err := newShipper(c.cfg.ShipperConfig)
	if err != nil {
		return err
	}

	err = shipper.Connect()
	if err != nil {
		return err
	}

	wg := sync.WaitGroup{}
	batchChan := make(chan source.Batch, 1)
	runnerLoop := runner{
		client: shipper,
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		err = runnerLoop.Run(ctx, batchChan)
		if err != nil {
			c.logger.Debugf("Collector; runnerLoop err ; %w", err)
		} else {
			c.logger.Debugf("Collecotor; runnerLoop is done.")
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		src.Run(batchChan)
	}()

	wg.Wait()

	return errors.New("")
}

type runner struct {
	client *shipper
}

func (l *runner) Run(ctx context.Context, batchChan <-chan source.Batch) error {
	// Note that shipper connection is established in Collector's Run() method
	if l.client == nil {
		return errors.New()
	}

	for {
		//TODO:
		select {
		case batch := <-batchChan:
			l.client.Publish()
		case <-ctx.Done():
		}
	}
	return nil
}
