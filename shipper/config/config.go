package config

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/xjayleex/minari-libs/queue/diskqueue"
	"github.com/xjayleex/minari-libs/queue/memqueue"
	scratchconfig "github.com/xjayleex/minari/lib/config"
	"github.com/xjayleex/minari/shipper/output"
	"github.com/xjayleex/minari/shipper/queue"
)

var (
	cfgFilePath       string
	ErrConfigIsNotSet = errors.New("config file is not set")
)

func init() {
	flag.StringVar(&cfgFilePath, "c", "", "config file path for minari shipper")
}

type ShipperRootConfig struct {
	Type    string        `config:"type"`
	Shipper ShipperConfig `config:"shipper"`
}

type ShipperConfig struct {
	// Monitor monitoring.Config `config:"monitoring"`
	Queue       queue.Config  `config:"queue"`
	Output      output.Config `config:"output"`
	StrictMode  bool          `config:"strict_mode"`
	ShipperConn ShipperConnectionConfig
}

type ShipperConnectionConfig struct {
	Server string     `config:"server"`
	TLS    ShipperTLS `config:"tls"`
}

type ShipperTLS struct {
	CAs  []string `config:"certificate_authorities"`
	Cert string   `config:"certificate"`
	Key  string   `config:"Key"`
}

func FromConfigFile() (ShipperRootConfig, error) {
	// TODO: impl unpack cfg from config file with resolved cfg file path.
	if cfgFilePath == "" {
		return ShipperRootConfig{}, ErrConfigIsNotSet
	}

	contents, err := os.ReadFile(cfgFilePath)

	if err != nil {
		return ShipperRootConfig{}, fmt.Errorf("error reading input file %s: %w", cfgFilePath, err)
	}

	raw, err := scratchconfig.NewConfigWithYAML(contents, "")
	if err != nil {
		return ShipperRootConfig{}, fmt.Errorf("error reading config from yaml: %w", err)
	}

	unpacker := func(cfg *ShipperRootConfig) error {
		return raw.Unpack(cfg)
	}

	return readConfig(unpacker)
}

type rawUnpacker func(cfg *ShipperRootConfig) error

func readConfig(unpacker rawUnpacker) (cfg ShipperRootConfig, err error) {
	cfg = DefaultConfig()
	err = unpacker(&cfg)
	if err != nil {
		return cfg, fmt.Errorf("error unpacking shipper config: %w", err)
	}

	return cfg, nil
}

func DefaultConfig() ShipperRootConfig {
	return ShipperRootConfig{
		Type: "default-config",
		Shipper: ShipperConfig{
			Queue: queue.Config{
				MemSettings: &memqueue.Setting{
					ACKListener:    nil,
					Events:         256,
					FlushMinEvents: 32,
					FlushTimeout:   time.Second,
					InputQueueSize: 0,
				},
				DiskConfig: &diskqueue.Config{},
			},
			Output: output.Config{
				Console: &output.ConsoleConfig{
					Enabled: true,
				},
			},
			StrictMode: false,
			ShipperConn: ShipperConnectionConfig{
				Server: "unix:///tmp/minari_shipper_grpc.scoket",
				TLS: ShipperTLS{
					CAs:  []string{},
					Cert: "",
					Key:  "",
				},
			},
		},
	}
}
