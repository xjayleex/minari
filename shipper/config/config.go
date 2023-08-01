package config

import (
	"github.com/xjayleex/minari/shipper/output"
	"github.com/xjayleex/minari/shipper/queue"
)

type ShipperRootConfig struct {
	Type    string        `config:"type"`
	Shipper ShipperConfig `config:"shipper"`
}

type ShipperConfig struct {
	// Monitor monitoring.Config `config:"monitoring"`
	Queue       queue.Config  `config:"queue"`
	Output      output.Config `config:"output"`
	StrictMode  bool          `config:"strict_mode"`
	ShipperConn ShipperConnectionConfig
}

type ShipperConnectionConfig struct {
	Server string `config:"server"`
	// TLS ShipperTLS `config:"ssl"` //  `config:"tls"`
}

//type ShipperTLS struct {
//	CAs []string `config:"certificate_authorities"`
//	Cert string `config:"certificate"`
//	Key string	`config:"Key"`
//}
