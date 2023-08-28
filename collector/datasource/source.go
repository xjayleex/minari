package datasource

import (
	"fmt"

	"github.com/xjayleex/minari-libs/api/proto/messages"
)

var sourceReg = map[string]Factory{}

type DataSource interface {
	Run(eventChan chan<- messages.Event) error
	Stop() error
}

type Config struct {
	Type string
}

func FromConfig(config Config) (DataSource, error) {
	if config.Type == "mock" {
		return &MockSource{}, nil
	}
	return nil, fmt.Errorf("unimplemented data source type %s", config.Type)
}

type Factory func() (DataSource, error)

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
