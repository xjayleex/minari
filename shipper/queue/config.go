package queue

import (
	"fmt"
	"time"

	"github.com/xjayleex/minari-libs/queue/diskqueue"
	"github.com/xjayleex/minari-libs/queue/memqueue"
)

type Config struct {
	MemSettings *memqueue.Setting
	DiskConfig  *diskqueue.Config
}

func DefaultConfig() Config {
	return Config{
		MemSettings: &memqueue.Setting{
			Events:         4096,
			FlushMinEvents: 2048,
			FlushTimeout:   1 * time.Second,
		},
		DiskConfig: nil,
	}
}

func (c *Config) Validate() error {
	if c.MemSettings == nil && c.DiskConfig == nil {
		return fmt.Errorf("neither memqueue or diskqueue setting are not supplied")
	}
	return nil
}

func (c *Config) useDiskQueue() bool {
	// TODO
	return false
}
