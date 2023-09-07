package source

import (
	"fmt"

	"github.com/xjayleex/minari-libs/api/proto/messages"
	"github.com/xjayleex/minari/lib/config"
)

var sourceReg = map[string]Factory{}

type DataSource interface {
	Run(eventChan chan<- *messages.Event) error
	Stop() error
}

type Factory func(cfg *config.C, stats Observer) (DataSource, error)

func FindFactory(name string) Factory {
	return sourceReg[name]
}

// can occur panic if try to register already existing source type
func RegisterType(name string, f Factory) {
	if sourceReg[name] != nil {
		panic(fmt.Errorf("data source type %s exists already", name))
	}
	sourceReg[name] = f
}

func Load(name string, stats Observer, cfg *config.C) (DataSource, error) {
	factory := FindFactory(name)
	if factory == nil {
		return nil, fmt.Errorf("output type %s undefined", name)
	}

	if stats == nil {
		// TODO: NilObserver
		stats = NewNilObserver()

	}
	return factory(cfg, stats)
}
