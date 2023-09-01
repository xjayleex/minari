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
	"github.com/xjayleex/minari-libs/thirdparty/helpers"
	"github.com/xjayleex/minari/collector/datasource"
	server "github.com/xjayleex/minari/shipper"
	shipperconfig "github.com/xjayleex/minari/shipper/config"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
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

	return s.startACKLoop()
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

func (s *shipper) startACKLoop() error {
	ctx, cancel := context.WithCancel(context.Background())
	s.ackCancel = cancel
	indexClient, err := s.client.PersistedIndex(ctx, &messages.PersistedIndexRequest{
		PollingInterval: durationpb.New(s.cfg.Client.AckPollingInterval),
	})

	if err != nil {
		return fmt.Errorf("faield to connect to the server: %w", err)
	}

	indexReply, err := indexClient.Recv()
	if err != nil {
		return fmt.Errorf("failed to fetch server information: %w", err)
	}
	s.uuid = indexReply.GetUuid()

	s.logger.Debugf("connection to %s (%s) established", s.cfg.Shipper.ShipperConn.Server)

	s.ackClient = indexClient
	s.ackBatchChan = make(chan pendingBatch)
	s.ackIndexChan = make(chan uint64)
	s.ackWaitGroup.Add(2)

	go func() {
		s.ackWorker(ctx)
		s.ackWaitGroup.Done()
	}()

	go func() {
		err := s.ackListener(ctx)
		s.ackWaitGroup.Done()
		if err != nil {
			s.logger.Errorf("acks listener stopped: %w", err)
			s.Close()
		}
	}()

	return nil
}

// listens for newly published batches awaiting acknowledgment,
// and for new persisted indexes that should be forwarded to already-published
// batches.
func (s *shipper) ackWorker(ctx context.Context) {
	s.logger.Debugf("starting ack loop with server %s", s.uuid)

	pel := []pendingBatch{}
	for {
		select {
		case <-ctx.Done():
			for _, p := range pel {
				p.batch.Cancelled()
			}
			return
		case newBatch := <-s.ackBatchChan:
			pel = append(pel, newBatch)
		case newIndex := <-s.ackIndexChan:
			lastProcessed := 0
			for _, p := range pel {
				if p.index > newIndex {
					break
				}

				p.batch.ACK()
				ackedCount := p.eventCount - p.droppedCount
				s.observer.Acked(ackedCount)
				s.logger.Debugf("%d events have been acknowledged, %d dropped", ackedCount, p.droppedCount)
				lastProcessed += 1
			}

			if lastProcessed != 0 {
				remaining := len(pel) - lastProcessed
				copy(pel[0:], pel[lastProcessed:])
				// adjust pending list length to 'remaining'
				pel = pel[:remaining]
			}
		}
	}
}

// the only job of this loop is to listen to the persisted index RPC stream
// and forward its value to the ack worker.
func (s *shipper) ackListener(ctx context.Context) error {
	s.logger.Debugf("starting ack listener loop with server %s", s.uuid)

	for {
		indexReply, err := s.ackClient.Recv()
		if err != nil {
			select {
			case <-ctx.Done():
				// If out context has been closed, this is an intentional closed
				// connection, so return no error.
				return nil
			default:
				// If the context itself is not closed then this means a real
				// connection error.
				return fmt.Errorf("ack listener closed connection: %w", err)
			}
		}
		s.ackIndexChan <- indexReply.PersistedIndex
	}
}

var InvalidTypedEvent = errors.New("invalid event data type")

func toShipperEvent(e datasource.Event) (*messages.Event, error) {
	meta, err := helpers.NewValue(e.Meta)
	if err != nil {
		return nil, fmt.Errorf("failed to convert event meta to protobuf: %w", err)
	}

	source := &messages.Source{}
	datastream := &messages.DataStream{}

	inputIdVal, err := e.Meta.GetValue("input_id")
	if err == nil {
		source.InputId, _ = inputIdVal.(string)
	}

	streamIdVal, err := e.Meta.GetValue("stream_id")
	if err == nil {
		source.StreamId, _ = streamIdVal.(string)
	}

	dsType, err := e.Meta.GetValue("data_stream.type")
	if err == nil {
		datastream.Type, _ = dsType.(string)
	}

	dsNamespace, err := e.Meta.GetValue("data_stream.namespace")
	if err == nil {
		datastream.Namespace, _ = dsNamespace.(string)
	}

	dsDataset, err := e.Meta.GetValue("data_stream.dataset")
	if err == nil {
		datastream.Dataset, _ = dsDataset.(string)
	}

	m := &messages.Event{
		Timestamp:  timestamppb.New(e.Timestamp),
		Metadata:   meta.GetStructValue(),
		Source:     source,
		DataStream: datastream,
	}

	if binary, ok := e.TypedEvent.(*messages.Event_Binary); ok {
		m.TypedEvent = binary
	} else {
		if fields, ok := e.TypedEvent.(*messages.Event_Fields); ok {
			m.TypedEvent = fields
		} else {
			return nil, InvalidTypedEvent
		}
	}

	return m, nil
}

type pendingBatch struct {
	batch        Batch
	index        uint64
	eventCount   int
	droppedCount int
}
