// Copyright 2023 PingCAP, Inc.
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

package mock

import (
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/atomic"
)

// MetricsCounter is a mock for metrics counter
type MetricsCounter struct {
	prometheus.Counter
	val atomic.Float64
}

// Add adds the value
func (c *MetricsCounter) Add(v float64) {
	c.val.Add(v)
}

// Inc increase the value
func (c *MetricsCounter) Inc() {
	c.val.Add(1)
}

// Val returns val
func (c *MetricsCounter) Val() float64 {
	return c.val.Load()
}
