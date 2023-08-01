package shipper

import (
	"net"
)

func newUnixListener(addr string) (net.Listener, error) {
	return net.Listen("unix", addr)
}
