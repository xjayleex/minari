package ceph

import (
	"github.com/xjayleex/minari-libs/logpack"
	"github.com/xjayleex/minari/shipper/queue"
)

type CephOutput struct {
	logger logpack.Logger
	config *Config
	queue  *queue.Queue
}

func NewCephOutput(config *Config)
