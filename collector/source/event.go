package source

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

type OutputMeta struct {
	Extension    string
	CompressType uint8
}

type Batch interface {
	Events() []Event
	ACK()
	Drop()
	Retry()
	RetryEvents(events []Event)
	// Split this batch's events into two smaller batches and retry them both.
	// If SplitRetry returns false, the batch could not be split, and the caller
	// is reponsible for reporting the error(including calling batch.Drop() if
	// necessary)
	SplitRetry() bool
	FreeEntries()
	// Send was aborted, try again but don't decrease the batch's TTL counter.
	Cancelled()
}

type batch []Event

func (b batch) Events() []Event {
	return ([]Event)(b)
}

func (b batch) ACK() {
	panic("not implemented") // TODO: Implement
}

func (b batch) Drop() {
	panic("not implemented") // TODO: Implement
}

func (b batch) Retry() {
	panic("not implemented") // TODO: Implement
}

func (b batch) RetryEvents(events []Event) {
	panic("not implemented") // TODO: Implement
}

func (b batch) SplitRetry() bool {
	panic("not implemented") // TODO: Implement
}

func (b batch) FreeEntries() {
	panic("not implemented") // TODO: Implement
}

// Send was aborted, try again but don't decrease the batch's TTL counter.
func (b batch) Cancelled() {
	panic("not implemented") // TODO: Implement
}
