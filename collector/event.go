package collector

import (
	"github.com/xjayleex/minari/collector/datasource"
)

type Batch interface {
	Events() []datasource.Event
	ACK()
	Drop()
	Retry()
	RetryEvents(events []datasource.Event)
	// Split this batch's events into two smaller batches and retry them both.
	// If SplitRetry returns false, the batch could not be split, and the caller
	// is reponsible for reporting the error(including calling batch.Drop() if
	// necessary)
	SplitRetry() bool
	FreeEntries()
	// Send was aborted, try again but don't decrease the batch's TTL counter.
	Cancelled()
}

type batch []datasource.Event

func (b batch) Events() []datasource.Event {
	return ([]datasource.Event)(b)
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

func (b batch) RetryEvents(events []datasource.Event) {
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
