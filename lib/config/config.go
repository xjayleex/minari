package config

import (
	"github.com/elastic/go-ucfg"
	"github.com/elastic/go-ucfg/yaml"
)

var (
	configOpts = []ucfg.Option{
		ucfg.PathSep("."),
		ucfg.ResolveEnv,
		ucfg.VarExp,
	}
)

type C ucfg.Config

func NewConfig() *C {
	return fromConfig(ucfg.New())
}

func fromConfig(in *ucfg.Config) *C {
	return (*C)(in)
}

func NewConfigWithYAML(in []byte, source string) (*C, error) {
	opts := append(
		[]ucfg.Option{
			ucfg.MetaData(ucfg.Meta{Source: source}),
		},
		configOpts...,
	)
	c, err := yaml.NewConfig(in, opts...)
	return fromConfig(c), err
}

func (c *C) Unpack(to interface{}) error {
	return c.access().Unpack(to, configOpts...)
}

func (c *C) access() *ucfg.Config {
	return (*ucfg.Config)(c)
}
