package datasource

import "fmt"

type DataSource interface {
	Start() error
	Stop() error
}

type Config struct {
	Type string
}

func FromConfig(config Config) (DataSource, error) {
	if config.Type == "mock" {
		return &MockDataSource{}, nil
	}
	return nil, fmt.Errorf("unimplemented data source type %s", config.Type)
}
