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

package window

import (
	"fmt"
	"time"

	"golang.org/x/exp/constraints"
)

// Metric is a sample interface.
// Implementations of Metrics in metric package are Counter, Gauge,
// PointGauge, RollingCounter and RollingGauge.
type Metric[T constraints.Integer | constraints.Float] interface {
	// Add adds the given value to the counter.
	Add(T)
	// Value gets the current value.
	// If the metric's type is PointGauge, RollingCounter, RollingGauge,
	// it returns the sum value within the window.
	Value() T
}

// Aggregation contains some common aggregation function.
// Each aggregation can compute summary statistics of window.
type Aggregation[T constraints.Integer | constraints.Float] interface {
	// Min finds the min value within the window.
	Min() T
	// Max finds the max value within the window.
	Max() T
	// Avg computes average value within the window.
	Avg() T
	// Sum computes sum value within the window.
	Sum() T
}

// RollingCounter represents a ring window based on time duration.
// e.g. [[1], [3], [5]]
type RollingCounter[T constraints.Integer | constraints.Float] interface {
	Metric[T]
	Aggregation[T]

	Timespan() int
	// Reduce applies the reduction function to all buckets within the window.
	Reduce(func(Iterator[T]) T) T
}

// RollingCounterOpts contains the arguments for creating RollingCounter.
type RollingCounterOpts struct {
	Size           int
	BucketDuration time.Duration
}

type rollingCounter[T constraints.Integer | constraints.Float] struct {
	policy *RollingPolicy[T]
}

// NewRollingCounter creates a new RollingCounter bases on RollingCounterOpts.
func NewRollingCounter[T constraints.Integer | constraints.Float](opts RollingCounterOpts) RollingCounter[T] {
	window := NewWindow[T](Options{Size: opts.Size})
	policy := NewRollingPolicy[T](window, RollingPolicyOpts{BucketDuration: opts.BucketDuration})
	return &rollingCounter[T]{
		policy: policy,
	}
}

func (r *rollingCounter[T]) Add(val T) {
	if val < 0 {
		panic(fmt.Errorf("stat/metric: cannot decrease in value. val: %v", val))
	}
	r.policy.Add(val)
}

func (r *rollingCounter[T]) Reduce(f func(Iterator[T]) T) T {
	return r.policy.Reduce(f)
}

func (r *rollingCounter[T]) Avg() T {
	return r.policy.Reduce(Avg[T])
}

func (r *rollingCounter[T]) Min() T {
	return r.policy.Reduce(Min[T])
}

func (r *rollingCounter[T]) Max() T {
	return r.policy.Reduce(Max[T])
}

func (r *rollingCounter[T]) Sum() T {
	return r.policy.Reduce(Sum[T])
}

func (r *rollingCounter[T]) Value() T {
	return r.Sum()
}

func (r *rollingCounter[T]) Timespan() int {
	r.policy.mu.RLock()
	defer r.policy.mu.RUnlock()
	return r.policy.timespan()
}
