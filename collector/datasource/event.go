package datasource

import (
	"time"

	"github.com/xjayleex/minari-libs/api/proto/messages"
)

type Event struct {
	Timestamp  time.Time
	Private    interface{}
	TimeSeries bool
	TypedEvent struct {
		BinaryEvent messages.Event_Binary
		FieldsEvent messages.Event_Fields
	}
}
