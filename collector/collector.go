package collector

import (
	"context"

	pb "github.com/xjayleex/minari-libs/api/proto/grpc"
	"github.com/xjayleex/minari-libs/api/proto/messages"

	"github.com/xjayleex/minari-libs/logpack"
	"github.com/xjayleex/minari/collector/datasource"
	shipperserver "github.com/xjayleex/minari/shipper"
	shipperconfig "github.com/xjayleex/minari/shipper/config"
	"google.golang.org/grpc"
)

type Collector struct {
	logger logpack.Logger
	cfg    Config

	Source datasource.DataSource
}

type Config struct {
	ShipperConfig shipperconfig.ShipperRootConfig
	SourceConfig  datasource.Config
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
	source, err := datasource.FromConfig(c.cfg.SourceConfig)

	creds, err := shipperserver.Creds(c.cfg.ShipperConfig.Shipper.ShipperConn)
	if err != nil {
		return err
	}
	conn, err := grpc.DialContext(
		ctx,
		c.cfg.ShipperConfig.Shipper.ShipperConn.Server,
		grpc.WithTransportCredentials(creds),
	)

	if err != nil {
		return err
	}

	shipper := pb.NewProducerClient(conn)
	ech := make(chan messages.Event, 1)

	mediator := mediator{
		shipper: shipper,
		ech:     ech,
	}

}

type mediator struct {
	shipper pb.ProducerClient
	ech     chan messages.Event
}

type loopRunner struct {
	client pb.ProducerClient
	ech    <-chan *messages.Event
}

func (l *loopRunner) Run() error {
	return nil
}
