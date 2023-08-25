package collector

type Observer interface {
	NewBatch(int)     // report new batch being processed with number of events
	Acked(int)        // report number of acked events
	Failed(int)       // report number of failed events
	Dropped(int)      // report number of dropped events
	Duplicate(int)    // report number of evnts detected as duplicates (e.g. on resends)
	Cancelled(int)    // report number of cancelled events
	Split()           // report a batch was split for being too large to ingest
	WriteError(error) // report an I/O error on write
	WriteBytes(int)   // report number of bytes being written
	ReadError(error)  // report an I/O error on read
	ReadBytes(int)    // report numbe of bytes being read
	ErrTooMany(int)   // report too many requests response
}

var nilObserver = (*emptyObserver)(nil)

type emptyObserver struct{}

func (*emptyObserver) NewBatch(_ int)     {}
func (*emptyObserver) Acked(_ int)        {}
func (*emptyObserver) Failed(_ int)       {}
func (*emptyObserver) Dropped(_ int)      {}
func (*emptyObserver) Duplicate(_ int)    {}
func (*emptyObserver) Cancelled(_ int)    {}
func (*emptyObserver) Split()             {}
func (*emptyObserver) WriteError(_ error) {}
func (*emptyObserver) WriteBytes(_ int)   {}
func (*emptyObserver) ReadError(_ error)  {}
func (*emptyObserver) ReadBytes(_ int)    {}
func (*emptyObserver) ErrTooMany(_ int)   {}
