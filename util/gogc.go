package util

import (
	"os"
	"runtime/debug"
	"strconv"
	"sync/atomic"

	"github.com/pingcap/tidb/metrics"
)

var gogcValue int64

func init() {
	gogcValue = 100
	if val, err := strconv.Atoi(os.Getenv("GOGC")); err == nil {
		gogcValue = int64(val)
	}
}

// SetGOGC update GOGC and related metrics.
func SetGOGC(val int) {
	debug.SetGCPercent(val)
	metrics.GOGC.Set(float64(val))
	atomic.StoreInt64(&gogcValue, int64(val))
}

// GetGOGC returns the current value of GOGC.
func GetGOGC() int {
	return int(atomic.LoadInt64(&gogcValue))
}
