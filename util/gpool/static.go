package gpool

import (
	"time"

	"github.com/pingcap/tidb/util/window"
	"go.uber.org/atomic"
)

type Statistic struct {
	// stat is the statistic of the task complete count.
	taskCntStat window.RollingCounter[uint64]
	// rtStat is the statistic of the task complete time.
	rtStat window.RollingCounter[uint64]

	inFlight        atomic.Uint64
	maxTaskCntCache atomic.Uint64
	maxPASSCache    atomic.Uint64
	minRtCache      atomic.Uint64

	bucketPerSecond int64
}

func NewStatistic() Statistic {
	const win = time.Second * 10
	const size = 100
	opts := window.RollingCounterOpts{
		Size:           size,
		BucketDuration: win / size,
	}
	bucketDuration := win / opts.BucketDuration
	return Statistic{
		taskCntStat:     window.NewRollingCounter[uint64](opts),
		rtStat:          window.NewRollingCounter[uint64](opts),
		bucketPerSecond: int64(time.Second / bucketDuration),
	}
}
