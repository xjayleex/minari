package shipper

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/docker/go-units"
	pb "github.com/xjayleex/minari-libs/api/proto/grpc"
	"github.com/xjayleex/minari-libs/logpack"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

var loggerName = "input-handler"

type InputHandler struct {
	Shipper Server

	publisher       Publisher
	logger          logpack.Logger
	server          *grpc.Server
	startMutex      sync.Mutex
	listenerAddress string
}

func NewGRPCServer(publisher Publisher) *InputHandler {
	srv := &InputHandler{
		publisher: publisher,
	}
	return srv
}

func (srv *InputHandler) Start(grpcTLS credentials.TransportCredentials, endpoint string) (err error) {
	srv.logger, err = logpack.NewLogger(loggerName)
	if err != nil {
		return err
	}

	if srv.server != nil {
		srv.logger.Debugf("shipper gRPC server already started, continuing...")
		return nil
	}

	// TODO : should do sanity check on endpoint?
	listenAddr := strings.TrimPrefix(endpoint, "unix://")
	srv.listenerAddress = listenAddr

	srv.logger.Debugf("initializing the Shipper Server...")
	opts := []grpc.ServerOption{
		grpc.Creds(grpcTLS),
		grpc.MaxRecvMsgSize(64 * units.MiB),
	}

	srv.server = grpc.NewServer(opts...)
	srv.Shipper, err = NewShipperServer(srv.publisher)

	if err != nil {
		return fmt.Errorf("could not initialize gRPC: %w", err)
	}

	pb.RegisterProducerServer(srv.server, srv.Shipper)

	srv.startMutex.Lock()

	dir := filepath.Dir(listenAddr)

	if _, err := os.Stat(dir); os.IsNotExist(err) {
		err = os.MkdirAll(dir, 0o755)
		if err != nil {
			srv.startMutex.Unlock()
			return fmt.Errorf("could not create directories for unix domain socket %s: %w", dir, err)
		}
	}

	// uds file already exists
	if _, err = os.Stat(listenAddr); err == nil {
		srv.logger.Debugf("listen address %s already exists, removing", listenAddr)
		err = os.Remove(listenAddr)
		if err != nil {
			return fmt.Errorf("error removing unix socket %s: %w", listenAddr, err)
		}
	}

	listener, err := newUnixListener(listenAddr)
	if err != nil {
		srv.startMutex.Unlock()
		return fmt.Errorf("failed to listen on %s: %w", listenAddr, err)
	}

	go func() {
		err = srv.server.Serve(listener)
		if err != nil {
			srv.logger.Errorf("gRPC server shut down with error: %s", err)
		}
	}()

	srv.logger.Debugf("%s gRPC Server started", "gerogev shipper")

	defer srv.startMutex.Unlock()

	testDialer := func(addr string) (net.Conn, error) {
		return net.Dial("unix", strings.TrimPrefix(addr, "unix://"))
	}
	conn, err := testDialer(listenAddr)

	if err != nil {
		srv.server.Stop()
		return fmt.Errorf("failed to test connection with gRPC server on %s:%w", listenAddr, err)
	}
	srv.logger.Debugf("succeed on dial test for the shipper gRPC server")
	_ = conn.Close()
	return nil
}

func (srv *InputHandler) Stop() {
	srv.startMutex.Lock()
	defer srv.startMutex.Unlock()

	if srv.Shipper != nil {
		err := srv.Shipper.Close()
		if err != nil {
			srv.logger.Debugf("Error stoppping shipper input: %s", err)
		}
		srv.Shipper = nil
	}

	if srv.server != nil {
		srv.server.GracefulStop()
		srv.server = nil
	}
	err := os.Remove(srv.listenerAddress)
	if err != nil {
		srv.logger.Debugf("error removing unix domain socket for grpc listener: %s", err)
	}
}
