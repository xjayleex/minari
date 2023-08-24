package shipper

import (
	"net"

	"github.com/xjayleex/minari/shipper/config"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

func newUnixListener(addr string) (net.Listener, error) {
	return net.Listen("unix", addr)
}

func Creds(c config.ShipperConnectionConfig) (credentials.TransportCredentials, error) {
	if c.TLS.Cert == "" {
		return insecure.NewCredentials(), nil
	}
	creds, err := credentials.NewServerTLSFromFile(c.TLS.Cert, c.TLS.Key)

	if err != nil && !c.TLS.Strict {
		return insecure.NewCredentials(), nil
	}
	return creds, err
}
