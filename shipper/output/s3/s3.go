package s3

import (
	"errors"
	"fmt"
)

func makeS3(config *Config) (*Client, error) {
	return nil, nil
}

type Config struct {
	Namer    func(meta interface{}) string
	Endpoint string
	Region   string
	KeyPairs []KeyPair
}

func (c *Config) Validate() error {
	if c.KeyPairs == nil {
		return fmt.Errorf("invalid config %s", ": nil key pair")
	} else {
		if len(c.KeyPairs) == 0 {
			return fmt.Errorf("invalid config %s", ": key pair not exists")
		}
	}

	if c.Namer == nil {
		return errors.New("no namer function to naming objects for s3 compat storage")
	}
	return nil
}

type KeyPair struct {
	Access string
	Secret string
}
