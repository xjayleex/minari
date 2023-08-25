package collector

import (
	"github.com/xjayleex/minari/collector/datasource"
)

type Batch interface {
	Events() []datasource.Event
}

type batch []datasource.Event

func (b batch) Events() []datasource.Event {
	return ([]datasource.Event)(b)
}
