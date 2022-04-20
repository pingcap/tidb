package split

import "time"

// Constants for split retry machinery.
const (
	SplitRetryTimes       = 32
	SplitRetryInterval    = 50 * time.Millisecond
	SplitMaxRetryInterval = time.Second

	SplitCheckMaxRetryTimes = 64
	SplitCheckInterval      = 8 * time.Millisecond
	SplitMaxCheckInterval   = time.Second

	ScatterWaitMaxRetryTimes = 64
	ScatterWaitInterval      = 50 * time.Millisecond
	ScatterMaxWaitInterval   = time.Second
	ScatterWaitUpperInterval = 180 * time.Second

	ScanRegionPaginationLimit = 128

	RejectStoreCheckRetryTimes  = 64
	RejectStoreCheckInterval    = 100 * time.Millisecond
	RejectStoreMaxCheckInterval = 2 * time.Second
)
