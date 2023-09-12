package output

import "github.com/xjayleex/minari/shipper/output/s3"

type Config struct {
	Console *ConsoleConfig
	S3      *s3.Config
	//Sodas   *sodas.Config
}
