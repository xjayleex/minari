package control

import (
	"context"
	"errors"
	"os"
	"os/signal"
	"syscall"

	"github.com/xjayleex/minari-libs/logpack"
	"github.com/xjayleex/minari/shipper"
	"github.com/xjayleex/minari/shipper/config"
	"github.com/xjayleex/minari/shipper/pubserver"
)

func LoadAndRun() error {
	cfg, err := config.FromConfigFile()
	// Should block here.
	switch {
	case err == nil:
		return RunUnmanaged(context.Background(), cfg)
	case errors.Is(err, config.ErrConfigIsNotSet):
		return RunManaged(cfg)
	default:
		return err
	}
}

func RunUnmanaged(ctx context.Context, cfg config.ShipperRootConfig) error {
	logger, err := logpack.NewLogger("shipper-server")
	if err != nil {
		return err
	}

	runner := pubserver.NewOutputServer()
	err = runner.Start(cfg)
	if err != nil {
		return err
	}
	defer runner.Close()
	creds, err := shipper.Creds(cfg.Shipper.ShipperConn)
	if err != nil {
		return err
	}

	srv := shipper.NewGRPCServer(runner)

	err = srv.Start(creds, cfg.Shipper.ShipperConn.Server)

	if err != nil {
		return err
	}

	// For Graceful Term.
	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc, syscall.SIGTERM, syscall.SIGINT, syscall.SIGHUP)

	select {
	case sig := <-sigc:
		switch sig {
		case syscall.SIGTERM, syscall.SIGINT:
			logger.Info("Received sigterm/sigint, stopping")
		case syscall.SIGHUP:
			logger.Info("Received sighup, stopping")
		}
	case <-ctx.Done():
		logger.Info("got context done", "error", ctx.Err())
	}

	return nil
}

func RunManaged(cfg config.ShipperRootConfig) error {
	return errors.New("unimplmened shipper server run mode")
}
