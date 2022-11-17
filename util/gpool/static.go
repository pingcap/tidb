package gpool

import (
	"math"
	"sync/atomic"
	"time"

	"github.com/pingcap/tidb/util/mathutil"
	"github.com/pingcap/tidb/util/window"
)

const bucketSize = 100
const win = time.Second * 10

// DoneFunc is done function.
type DoneFunc func()

type counterCache struct {
	time time.Time
	val  uint64
}

type Statistic struct {
	// stat is the statistic of the task complete count.
	taskCntStat window.RollingCounter[uint64]
	// rtStat is the statistic of the task complete time.
	rtStat window.RollingCounter[uint64]

	maxPASSCache atomic.Pointer[counterCache]
	minRtCache   atomic.Pointer[counterCache]
	queueSize    atomic.Int64
	// inFlight is from the task create to the task complete.
	inFlight        atomic.Int64
	maxTaskCntCache atomic.Uint64
	bucketPerSecond uint64
}

func NewStatistic() Statistic {

	opts := window.RollingCounterOpts{
		Size:           bucketSize,
		BucketDuration: win / bucketSize,
	}
	bucketDuration := win / opts.BucketDuration
	return Statistic{
		taskCntStat:     window.NewRollingCounter[uint64](opts),
		rtStat:          window.NewRollingCounter[uint64](opts),
		bucketPerSecond: uint64(time.Second / bucketDuration),
	}
}

func (s *Statistic) GetQueueSize() int64 {
	return s.queueSize.Load()
}

func (s *Statistic) InQueue() {
	s.queueSize.Add(1)
}

func (s *Statistic) OutQueue() {
	s.queueSize.Add(-1)
}

func (s *Statistic) MaxInFlight() int64 {
	return int64(math.Floor(float64(s.MaxPASS()*s.MinRT()*s.bucketPerSecond)/1000.0) + 0.5)
}

func (s *Statistic) MinRT() uint64 {
	rc := s.minRtCache.Load()
	if rc != nil {
		if s.timespan(rc.time) < 1 {
			return rc.val
		}
	}
	rawMinRT := s.rtStat.Reduce(func(iterator window.Iterator[uint64]) uint64 {
		var result uint64 = math.MaxUint64
		for i := 1; iterator.Next() && i < bucketSize; i++ {
			bucket := iterator.Bucket()
			if len(bucket.Points) == 0 {
				continue
			}
			var total uint64
			for _, p := range bucket.Points {
				total += p
			}
			avg := total / uint64(bucket.Count)
			result = mathutil.Min(result, avg)
		}
		return result
	})
	if rawMinRT <= 0 {
		rawMinRT = 1
	}
	s.minRtCache.Store(&counterCache{
		val:  rawMinRT,
		time: time.Now(),
	})
	return rawMinRT
}

func (s *Statistic) MaxPASS() uint64 {
	ps := s.maxPASSCache.Load()
	if ps != nil {
		if s.timespan(ps.time) < 1 {
			return ps.val
		}
	}
	rawMaxPass := s.taskCntStat.Reduce(func(iterator window.Iterator[uint64]) uint64 {
		var result uint64 = 1
		for i := 1; iterator.Next() && i < bucketSize; i++ {
			bucket := iterator.Bucket()
			var count uint64 = 0.0
			for _, p := range bucket.Points {
				count += p
			}
			result = mathutil.Max(result, count)
		}
		return result
	})
	s.maxPASSCache.Store(&counterCache{
		val:  rawMaxPass,
		time: time.Now(),
	})
	return rawMaxPass
}

func (s *Statistic) timespan(lastTime time.Time) int {
	v := int(time.Since(lastTime) / win)
	if v > -1 {
		return v
	}
	return bucketSize
}

func (s *Statistic) Static() (DoneFunc, error) {
	s.inFlight.Add(1)
	start := time.Now().UnixNano()
	ms := float64(time.Millisecond)
	return func() {
		rt := uint64(math.Ceil(float64(time.Now().UnixNano()-start)) / ms)
		s.rtStat.Add(rt)
		s.inFlight.Add(-1)
		s.taskCntStat.Add(1)
	}, nil
}
