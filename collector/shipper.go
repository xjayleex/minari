package collector

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	pb "github.com/xjayleex/minari-libs/api/proto/grpc"
	"github.com/xjayleex/minari-libs/api/proto/messages"
	"github.com/xjayleex/minari-libs/logpack"
	"github.com/xjayleex/minari/collector/datasource"
	server "github.com/xjayleex/minari/shipper"
	shipperconfig "github.com/xjayleex/minari/shipper/config"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	DialTimeout = time.Second * 60
)

type shipper struct {
	cfg shipperconfig.ShipperRootConfig

	logger   logpack.Logger
	observer Observer

	uuid string

	conn      *grpc.ClientConn
	client    pb.ProducerClient
	ackClient pb.Producer_PersistedIndexClient

	// The publish function sends to ackLoopChan to notify the ack worker of
	// new pending batches
	ackBatchChan chan pendingBatch

	// The ack RPC listener sends to ackIndexChan to notify the ack worker
	// of the new persisted index
	ackIndexChan chan uint64

	// ackWaitGroup is used to synchronize the shutdown of the ack listener
	// and the ack worker when a connection is closed.
	ackWaitGroup sync.WaitGroup

	// ackCancel cancels the context for the ack listener and the ack worker,
	// notifying them to shut down.
	ackCancel context.CancelFunc
}

var converter = toShipperEvent

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

	//FIXME:
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

	var droppedCount int = 0

	for i, e := range events {
		converted, err := converter(e)
		if err != nil {
			// conversion errors are not recoverable, so we have to drop the event completely
			s.logger.Errorf("%d/%d: %q, dropped", i+1, len(events), err)

			droppedCount += 1
			continue
		}
		toSend = append(toSend, converted)
	}

	convertedCount := len(toSend)

	s.observer.Dropped(droppedCount)

	var lastAcceptedIndex uint64

	ctx, cancel := context.WithTimeout(ctx, s.cfg.Client.Timeout)
	defer cancel()

	for len(toSend) > 0 {
		reply, err := s.client.PublishEvents(ctx, &messages.PublishRequest{
			Uuid:   s.uuid,
			Events: toSend,
		})

		if err != nil {
			if status.Code(err) == codes.ResourceExhausted {
				if batch.SplitRetry() {
					s.observer.Split()
				} else {
					batch.Drop()
					s.observer.Dropped(len(events))
				}
				return nil
			}
			batch.Cancelled()
			s.observer.Cancelled(len(events))
			return fmt.Errorf("failed to publish the batch to the shipper, none of the %d events were accepted: %w", len(toSend), err)
		}

		if int(reply.AcceptedCount) > len(toSend) {
			return fmt.Errorf(
				"server returned unexpected results, expected maximum accepted items %d, got %d",
				len(toSend),
				reply.AcceptedCount,
			)
		}

		toSend = toSend[reply.AcceptedCount:]
		lastAcceptedIndex = reply.AcceptedIndex
	}

	s.logger.Debugf("total of %d events have been accepted from batch, %d dropped", convertedCount, droppedCount)

	batch.FreeEntries()
	s.ackBatchChan <- pendingBatch{
		batch:        batch,
		index:        lastAcceptedIndex,
		eventCount:   len(events),
		droppedCount: droppedCount,
	}

	return nil
}

func (s *shipper) Close() error {
	if s.conn == nil {
		return fmt.Errorf("connection is not established yet")
	}

	s.ackCancel()
	s.ackWaitGroup.Wait()

	err := s.conn.Close()
	s.conn = nil
	s.client = nil
	return err
}

func toShipperEvent(e datasource.Event) (*messages.Event, error) {
	return nil, errors.New("TODO:")
}

type pendingBatch struct {
	batch        Batch
	index        uint64
	eventCount   int
	droppedCount int
}
