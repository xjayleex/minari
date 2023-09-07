package collector

import (
	"context"
	"errors"
	"sync"

	"github.com/xjayleex/minari-libs/api/proto/messages"

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
	source, err := source.Load("", nil, c.cfg.SourceConfig)
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

	eventChan := make(chan *messages.Event, 1)
	runnerLoop := runner{
		client: shipper,
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		err = runnerLoop.Run(ctx, eventChan)
		if err != nil {
			c.logger.Debugf("Collector; runnerLoop err ; %w", err)
		} else {
			c.logger.Debugf("Collecotor; runnerLoop is done.")
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		source.Run(eventChan)
	}()

	wg.Wait()

	return errors.New("")
}

type runner struct {
	client *shipper
	// eventChan <-chan *messages.Event
}

func (l *runner) Run(ctx context.Context, eventChan <-chan *messages.Event) error {
	// l.eventChan = eventChan
	for {
		//TODO:
		select {
		case event := <-eventChan:
		case <-ctx.Done():
		}
	}
	return nil
}
