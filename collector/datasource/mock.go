package datasource

func init() {
	//TODO: datasource.Register("mock-source", makeMockSource )
}

// func makeMockSource() *MockSource {}

type MockSource struct{}

func (s *MockSource) Start() error {
	return nil
}

func (s *MockSource) Stop() error {
	return nil
}
