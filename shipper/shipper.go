package shipper

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/gofrs/uuid"
	pb "github.com/xjayleex/minari-libs/api/proto/grpc"
	"github.com/xjayleex/minari-libs/api/proto/messages"
	"github.com/xjayleex/minari-libs/logpack"
	"github.com/xjayleex/minari/shipper/queue"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Server interface {
	Close() error
	SetStrictMode(bool)
	SetInitError(msg string)
	pb.ProducerServer
}

type shipperServer struct {
	uuid      string
	logger    logpack.Logger
	publisher Publisher

	close   *sync.Once
	ctx     context.Context
	stopper func()

	strictMode bool

	pb.UnimplementedProducerServer
}

func NewShipperServer(publisher Publisher) (Server, error) {
	logger, err := logpack.NewLogger("shipper-server")
	// NOTE : panic below
	if err != nil {
		return nil, err
	}

	if publisher == nil {
		logger.Debugf("gRPC endpoint has no output")
	}
	id, err := uuid.NewV4()
	if err != nil {
		return nil, fmt.Errorf("error generating shipper UUID: %w", err)
	}

	ctx, stopper := context.WithCancel(context.Background())

	return &shipperServer{
		uuid:                        id.String(),
		logger:                      logger,
		publisher:                   publisher,
		close:                       &sync.Once{},
		ctx:                         ctx,
		stopper:                     stopper,
		strictMode:                  false,
		UnimplementedProducerServer: pb.UnimplementedProducerServer{},
	}, nil
}

func (x *shipperServer) SetStrictMode(strict bool) {
	x.strictMode = strict
}

func (x *shipperServer) PublishEvents(ctx context.Context, req *messages.PublishRequest) (*messages.PublishReply, error) {
	if !x.publisher.IsInitialized() {
		return nil, status.Errorf(codes.Unavailable, "publisher is not available")
	}

	resp := &messages.PublishReply{
		Uuid: x.uuid,
	}

	// TODO : server uuid && request uuid valdation logic required

	if len(req.Events) == 0 {
		return nil, status.Error(codes.InvalidArgument, "publish request must contain at least one event")
	}

	if x.strictMode {
		for _, e := range req.Events {
			err := x.validateEvent(e)
			if err != nil {
				return nil, status.Error(codes.InvalidArgument, err.Error())
			}
		}
	}

	acceptedIndex, err := x.publisher.Publish(ctx, req.Events[0])
	if err != nil {
		return nil, status.Error(codes.Unavailable, err.Error())
	} else {
		resp.AcceptedCount += 1
	}

	for _, event := range req.Events[1:] {
		id, err := x.publisher.TryPublish(event)
		if err == nil {
			resp.AcceptedCount += 1
			acceptedIndex = id
			continue
		}

		if errors.Is(err, queue.ErrQueueFull) {
			x.logger.Debugf("queue is full, not all events accepted. Events = %d, accepted = %d", len(req.Events), resp.AcceptedCount)
		} else {
			x.logger.Errorf("failed to enqueue an event. Events = %d, accpeted = %d: %w", len(req.Events), resp.AcceptedCount, err)
		}
		break
	}

	resp.AcceptedIndex = uint64(acceptedIndex)
	x.logger.Debugf("finished publishing a batch. Events = %d, accepted = %d, accepted index = %d", len(req.Events), resp.AcceptedCount, resp.AcceptedIndex)
	return resp, nil
}

func (x *shipperServer) PersistedIndex(req *messages.PersistedIndexRequest, producer pb.Producer_PersistedIndexServer) error {
	if !x.publisher.IsInitialized() {
		return status.Error(codes.Unavailable, "shipper server initializing...")
	}

	x.logger.Debug("new subscriber for persisted index change")
	defer x.logger.Debug("unsubscribed from persited index change")

	persistedIndex, err := x.publisher.PersistedIndex()
	if err != nil {
		return status.Error(codes.Unavailable, err.Error())
	}

	err = producer.Send(&messages.PersistedIndexReply{
		Uuid:           x.uuid,
		PersistedIndex: uint64(persistedIndex),
	})

	if err != nil {
		return err
	}

	pollInterval := req.PollingInterval.AsDuration()

	if pollInterval == 0 {
		return nil
	}

	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-producer.Context().Done():
			return fmt.Errorf("producer context: %w", producer.Context().Err())
		case <-x.ctx.Done():
			return fmt.Errorf("server is stopped: %w", x.ctx.Err())
		case <-ticker.C:
			newPersistedIndex, err := x.publisher.PersistedIndex()
			if err != nil || newPersistedIndex == persistedIndex {
				continue
			}

			persistedIndex = newPersistedIndex
			err = producer.Send(&messages.PersistedIndexReply{
				Uuid:           x.uuid,
				PersistedIndex: uint64(persistedIndex),
			})

			if err != nil {
				return fmt.Errorf("failed to send the update: %w", err)
			}
		}
	}
}

func (x *shipperServer) Close() error {
	x.close.Do(func() { x.stopper() })
	return nil
}

func (x *shipperServer) SetInitError(msg string) {}

func (x *shipperServer) validateEvent(m *messages.Event) error {
	var msgs []string

	if err := m.Timestamp.CheckValid(); err != nil {
		msgs = append(msgs, fmt.Sprintf("timestamp: %s", err))
	}

	if err := x.validateDataStream(m.DataStream); err != nil {
		msgs = append(msgs, fmt.Sprintf("datastream: %s", err))
	}

	if err := x.validateSource(m.Source); err != nil {
		msgs = append(msgs, fmt.Sprintf("source: %s", err))
	}

	if len(msgs) == 0 {
		return nil
	}

	return errors.New(strings.Join(msgs, "; "))
}

func (x *shipperServer) validateDataStream(*messages.DataStream) error {
	return nil
}

func (x *shipperServer) validateSource(*messages.Source) error {
	return nil
}

type Publisher interface {
	Publish(ctx context.Context, event *messages.Event) (queue.EntryID, error)
	TryPublish(event *messages.Event) (queue.EntryID, error)
	PersistedIndex() (queue.EntryID, error)
	IsInitialized() bool
}
