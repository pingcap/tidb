// Copyright 2022 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package pooltask

import (
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/mathutil"
	"github.com/pingcap/tidb/util/window"
)

const bucketSize = 100
const win = time.Second * 10

type counterCache struct {
	time time.Time
	val  uint64
}

// Statistic is the statistic of the pool.
type Statistic struct {
	mu sync.RWMutex
	// taskCntStat is the statistic of the task complete count.
	taskCntStat window.RollingCounter[uint64]
	// rtStat is the statistic of the task complete time.
	rtStat       window.RollingCounter[uint64]
	maxPASSCache atomic.Pointer[counterCache]
	minRtCache   atomic.Pointer[counterCache]
	longRTT      *mathutil.ExponentialMovingAverage
	queueSize    atomic.Int64
	shortRTT     atomic.Uint64
	// inFlight is from the task create to the task complete.
	inFlight        atomic.Int64
	bucketPerSecond uint64
	event           chan event
	exit            chan struct{}
	wg              util.WaitGroupWrapper
}

// NewStatistic returns a new statistic.
func NewStatistic() *Statistic {
	opts := window.RollingCounterOpts{
		Size:           bucketSize,
		BucketDuration: win / bucketSize,
	}
	bucketDuration := win / opts.BucketDuration
	return &Statistic{
		taskCntStat:     window.NewRollingCounter[uint64](opts),
		rtStat:          window.NewRollingCounter[uint64](opts),
		bucketPerSecond: uint64(time.Second / bucketDuration),
		longRTT:         mathutil.NewExponentialMovingAverage(0.2, 10),
		event:           make(chan event, 100),
	}
}

// GetQueueSize returns the queue size.
func (s *Statistic) GetQueueSize() int64 {
	return s.queueSize.Load()
}

// InQueue is called when the task is in the queue.
func (s *Statistic) InQueue() {
	s.queueSize.Add(1)
}

// OutQueue is called when the task is out of the queue.
func (s *Statistic) OutQueue() {
	s.queueSize.Add(-1)
}

// MaxInFlight returns the maxInFlight.
func (s *Statistic) MaxInFlight() int64 {
	return int64(math.Floor(float64(s.MaxPASS()*s.MinRT()*s.bucketPerSecond)/1000.0) + 0.5)
}

// MinRT is the minimum processing time.
func (s *Statistic) MinRT() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
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

// MaxPASS is the maximum processing count per unit time.
func (s *Statistic) MaxPASS() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
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
			var count uint64
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

func (*Statistic) timespan(lastTime time.Time) int {
	v := int(time.Since(lastTime) / win)
	if v > -1 {
		return v
	}
	return bucketSize
}

// InFlight returns the inflight.
func (s *Statistic) InFlight() int64 {
	return s.inFlight.Load()
}

// LongRTT returns the longRTT.
func (s *Statistic) LongRTT() float64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.longRTT.Get()
}

// UpdateLongRTT updates the longRTT.
func (s *Statistic) UpdateLongRTT(f func(float64) float64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.longRTT.Update(f)
}

// ShortRTT returns the shortRTT.
func (s *Statistic) ShortRTT() uint64 {
	return s.shortRTT.Load()
}

// Static is to static the job.
func (s *Statistic) Static() (DoneFunc, error) {
	s.inFlight.Add(1)
	start := time.Now().UnixNano()
	ms := float64(time.Millisecond)
	return func() {
		rt := uint64(math.Ceil(float64(time.Now().UnixNano()-start)) / ms)
		s.event <- event{rt: rt}
	}, nil
}

func (s *Statistic) Start() {
	s.wg.Run(func() {
		for {
			select {
			case <-s.exit:
				return
			case e := <-s.event:
				s.mu.Lock()
				s.longRTT.Add(float64(e.rt))
				s.rtStat.Add(e.rt)
				s.taskCntStat.Add(1)
				s.mu.Unlock()
				s.shortRTT.Store(e.rt)
				s.inFlight.Add(-1)
			}
		}
	})
}

func (s *Statistic) Stop() {
	close(s.exit)
	s.wg.Wait()
}

type event struct {
	rt uint64
}
