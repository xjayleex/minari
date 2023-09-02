package datasource

import (
	"errors"

	"github.com/xjayleex/minari-libs/api/proto/messages"
)

func init() {
	RegisterType("mock", makeMockSource)
}

func makeMockSource() (DataSource, error) {
	src := &MockSource{}
	return src, errors.New("FIXME: unimplemented")
}

type MockSource struct {
	ec chan messages.Event
}

func (s *MockSource) Run(eventChan chan<- messages.Event) error {
	return nil
}

func (s *MockSource) Stop() error {
	return nil
}
