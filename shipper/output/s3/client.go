package s3

import (
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/xjayleex/minari-libs/logpack"
)

type Client struct {
	logger logpack.Logger
	compat *minio.Client
}

func makeS3Client(config Config) (*Client, error) {
	c, err := newMinioClient(config)
	return c, err
}

func newMinioClient(config Config) (*Client, error) {
	creds := credentials.NewStaticV4(config.KeyPairs[0].Access, config.KeyPairs[0].Secret, "")
	compat, err := minio.New(config.Endpoint, &minio.Options{
		Creds:  creds,
		Secure: false,
		Region: config.Region,
	})

	if err != nil {
		return nil, err
	}

	logger, err := logpack.NewLogger("minio-client")
	if err != nil {
		return nil, err
	}
	return &Client{
		logger: logger,
		compat: compat,
	}, nil
}
