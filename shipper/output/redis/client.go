package redis

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/xjayleex/minari-libs/api/proto/messages"
	"github.com/xjayleex/minari-libs/logpack"
	"github.com/xjayleex/retry-go"
)

type Client struct {
	mux    sync.Mutex
	logger logpack.Logger

	c           *redis.Client
	conn        *redis.Conn
	pingTimeout time.Duration

	config *redis.Options
}

func DefaultConfig() *redis.Options {
	options := &redis.Options{
		Network:    "tcp",
		Addr:       "",
		ClientName: "",
		//Dialer: func(ctx context.Context, network string, addr string) (net.Conn, error) {
		//},
		//OnConnect: func(ctx context.Context, cn *goredis.Conn) error {
		//},
		Protocol: 3,
		//Username: "",
		//Password: "",
		//CredentialsProvider: func() (string, string) {
		//},
		DB:                    0,
		MaxRetries:            3,
		MinRetryBackoff:       8 * time.Millisecond,
		MaxRetryBackoff:       60 * time.Second,
		DialTimeout:           0,
		ReadTimeout:           0,
		WriteTimeout:          0,
		ContextTimeoutEnabled: false,
		// PoolFIFO:              false,
		// PoolSize:              0,
		// PoolTimeout:           0,
		// MinIdleConns:          0,
		// MaxIdleConns:          0,
		// ConnMaxIdleTime:       0,
		// ConnMaxLifetime:       0,
		// TLSConfig:             &tls.Config{},
		// Limiter:               nil,
	}
	return options
}

func newRedisStreamClient(config *Config) (*Client, error) {
	logger, err := logpack.NewLogger("redis-client")
	if err != nil {
		return nil, err
	}
	return &Client{
		mux:    sync.Mutex{},
		logger: logger,
	}, nil
}

func (client *Client) Connect() error {
	client.mux.Lock()
	defer client.mux.Unlock()

	client.logger.Debugf("Connecting with redis server")

	c := redis.NewClient(client.config)
	conn := c.Conn()

	ctx, cancel := context.WithTimeout(context.Background(), client.pingTimeout)
	defer cancel()

	err := pingWithDelay(ctx, conn)
	if err != nil {
		client.logger.Debugf("failed on connect to redis server : %w", err)
		return err
	}

	client.conn = conn
	client.c = c

	return nil
}

func (client *Client) Close() error {
	return errors.New("unimplemented")
}

// ProduceEvents sends all events to redis (stream). On error a slice with all
// events not published or confirmed to be processed by kafka will be returned.
// The input slice backing memory will be reused by return the value.
func (client *Client) produceEvents(data []*messages.Event) ([]*messages.Event, error) {
	// TODO: implement me
	return nil, errors.New("unimplemented")
}

func pingWithDelay(ctx context.Context, conn *redis.Conn) error {
	err := retry.Do(
		func() error {
			ping := conn.Ping(ctx)
			if _, err := ping.Result(); err != nil {
				return err
			} else {
				return nil
			}
		},
		retry.Attempts(5),
		retry.Delay(time.Second*2),
	)
	return err
}
