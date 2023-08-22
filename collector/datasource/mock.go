package datasource

type MockDataSource struct{}

func (s *MockDataSource) Start() error {
	return nil
}

func (s *MockDataSource) Stop() error {
	return nil
}
