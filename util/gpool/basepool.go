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

package gpool

// BasePool is base class of pool
type BasePool struct {
	statistic *Statistic
}

// SetStatistic is to set Statistic
func (p *BasePool) SetStatistic(statistic *Statistic) {
	p.statistic = statistic
}

// GetStatistic is to get Statistic
func (p *BasePool) GetStatistic() *Statistic {
	return p.statistic
}

// MaxInFlight is to get max in flight.
func (p *BasePool) MaxInFlight() int64 {
	return p.statistic.MaxInFlight()
}

// GetQueueSize is to get queue size.
func (p *BasePool) GetQueueSize() int64 {
	return p.statistic.GetQueueSize()
}

// MaxPASS is to get max pass.
func (p *BasePool) MaxPASS() uint64 {
	return p.statistic.MaxPASS()
}

// MinRT is to get min rt.
func (p *BasePool) MinRT() uint64 {
	return p.statistic.MinRT()
}

// InFlight is to get in flight.
func (p *BasePool) InFlight() int64 {
	return p.statistic.InFlight()
}

// LongRTT is to get long rtt.
func (p *BasePool) LongRTT() float64 {
	return p.statistic.LongRTT()
}

// ShortRTT is to get short rtt.
func (p *BasePool) ShortRTT() uint64 {
	return p.statistic.ShortRTT()
}
