package ceph

func makeCeph(config *Config) (*Client, error) {
	return nil, nil
}

type Config struct {
	namer func(meta interface{}) string
}
