package collector

import (
	"context"
	"errors"
	"time"

	pb "github.com/xjayleex/minari-libs/api/proto/grpc"
	"github.com/xjayleex/minari-libs/api/proto/messages"
	"github.com/xjayleex/minari/collector/datasource"
	server "github.com/xjayleex/minari/shipper"
	shipperconfig "github.com/xjayleex/minari/shipper/config"
	"google.golang.org/grpc"
)

const (
	DialTimeout = time.Second * 60
)

type shipper struct {
	cfg shipperconfig.ShipperRootConfig

	conn   *grpc.ClientConn
	client pb.ProducerClient

	observer Observer
}

func makeShipper() {
	//FIXME:
}

var transfomer = toShipperEvent

func (s *shipper) Connect() error {
	creds, err := server.Creds(s.cfg.Shipper.ShipperConn)
	if err != nil {
		return err
	}

	opts := []grpc.DialOption{
		grpc.WithConnectParams(grpc.ConnectParams{
			MinConnectTimeout: DialTimeout,
		}),
		grpc.WithBlock(),
		grpc.WithTransportCredentials(creds),
	}

	ctx, cancel := context.WithTimeout(context.Background(), DialTimeout)
	defer cancel()

	s.conn, err = grpc.DialContext(
		ctx,
		s.cfg.Shipper.ShipperConn.Server, opts...,
	)

	if err != nil {
		return err
	}

	s.client = pb.NewProducerClient(s.conn)

	return nil
}

func (s *shipper) Publish(ctx context.Context, batch Batch) error {
	err := s.publish(ctx, batch)
	if err != nil {
		s.Close()
	}
	return err
}

func (s *shipper) publish(ctx context.Context, batch Batch) error {
	if s.conn == nil {
		return errors.New("connection is not established")
	}

	events := batch.Events()
	s.observer.NewBatch(len(events))

	toSend := make([]*messages.Event, 0, len(events))

	var dropped int = 0

	for _, e := range events {
		converted, err := transfomer(e)
		if err != nil {
			//TODO: log
			dropped += 1
			continue
		}
		toSend = append(toSend, converted)
	}

	return errors.New("TODO:")
}

func (s *shipper) Close() {}

func toShipperEvent(e datasource.Event) (*messages.Event, error) {
	return nil, errors.New("TODO:")
}
