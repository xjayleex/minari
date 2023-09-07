package mock

import (
	"errors"

	"github.com/xjayleex/minari-libs/api/proto/messages"
	"github.com/xjayleex/minari/collector/source"
	"github.com/xjayleex/minari/lib/config"
)

func init() {
	source.RegisterType("mock", makeMockSource)
}

func makeMockSource(config *config.C, stats source.Observer) (source.DataSource, error) {
	src := &MockSource{}

	return src, errors.New("FIXME: unimplemented")
}

type MockSource struct {
	eventChan chan<- *messages.Event
}

func (s *MockSource) Run(eventChan chan<- *messages.Event) error {
	s.eventChan = eventChan
	return nil
}

func (s *MockSource) Stop() error {
	return nil
}
