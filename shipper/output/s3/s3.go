package s3

func makeS3(config *Config) (*Client, error) {
	return nil, nil
}

type Config struct {
	namer func(meta interface{}) string
}
