package datasource

import (
	"time"

	"github.com/xjayleex/minari-libs/thirdparty/mapstr"
)

type Event struct {
	Timestamp  time.Time
	Meta       mapstr.M
	Private    interface{}
	TimeSeries bool
	TypedEvent interface{}
}
