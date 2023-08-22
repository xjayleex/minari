package datasource

type ProxyConfig struct{}

type ProxyPolicy interface{}

type NoProxy struct{}

func (p *NoProxy) Rotate() {}

func (p *NoProxy) Get() string {
	return "NotImplemented"
}
