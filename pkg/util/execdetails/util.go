// Copyright 2025 PingCAP, Inc.
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

package execdetails

import (
	"cmp"
	"context"
	"math"
	"slices"
	"time"

	"github.com/influxdata/tdigest"
	"github.com/tikv/client-go/v2/util"
)

// ContextWithInitializedExecDetails returns a context with initialized stmt execution, execution and resource usage details.
func ContextWithInitializedExecDetails(ctx context.Context) context.Context {
	ctx = context.WithValue(ctx, StmtExecDetailKey, &StmtExecDetails{})
	ctx = context.WithValue(ctx, util.ExecDetailsKey, &util.ExecDetails{})
	ctx = context.WithValue(ctx, util.RUDetailsCtxKey, util.NewRUDetails())
	return ctx
}

// GetExecDetailsFromContext gets stmt execution, execution and resource usage details from context.
func GetExecDetailsFromContext(ctx context.Context) (stmtDetail StmtExecDetails, tikvExecDetail util.ExecDetails, ruDetails *util.RUDetails) {
	stmtDetailRaw := ctx.Value(StmtExecDetailKey)
	if stmtDetailRaw != nil {
		stmtDetail = *(stmtDetailRaw.(*StmtExecDetails))
	}
	tikvExecDetailRaw := ctx.Value(util.ExecDetailsKey)
	if tikvExecDetailRaw != nil {
		tikvExecDetail = *(tikvExecDetailRaw.(*util.ExecDetails))
	}
	if ruDetailsVal := ctx.Value(util.RUDetailsCtxKey); ruDetailsVal != nil {
		ruDetails = ruDetailsVal.(*util.RUDetails)
	} else {
		ruDetails = util.NewRUDetails()
	}

	return
}

type canGetFloat64 interface {
	GetFloat64() float64
}

// Int64 is a wrapper of int64 to implement the canGetFloat64 interface.
type Int64 int64

// GetFloat64 implements the canGetFloat64 interface.
func (i Int64) GetFloat64() float64 { return float64(i) }

// Duration is a wrapper of time.Duration to implement the canGetFloat64 interface.
type Duration time.Duration

// GetFloat64 implements the canGetFloat64 interface.
func (d Duration) GetFloat64() float64 { return float64(d) }

// DurationWithAddr is a wrapper of time.Duration and string to implement the canGetFloat64 interface.
type DurationWithAddr struct {
	D    time.Duration
	Addr string
}

// GetFloat64 implements the canGetFloat64 interface.
func (d DurationWithAddr) GetFloat64() float64 { return float64(d.D) }

// Percentile is a struct to calculate the percentile of a series of values.
type Percentile[valueType canGetFloat64] struct {
	values   []valueType
	size     int
	isSorted bool

	minVal valueType
	maxVal valueType
	sumVal float64
	dt     *tdigest.TDigest
}

// Add adds a value to calculate the percentile.
func (p *Percentile[valueType]) Add(value valueType) {
	p.isSorted = false
	p.sumVal += value.GetFloat64()
	p.size++
	if p.dt == nil && len(p.values) == 0 {
		p.minVal = value
		p.maxVal = value
	} else {
		if value.GetFloat64() < p.minVal.GetFloat64() {
			p.minVal = value
		}
		if value.GetFloat64() > p.maxVal.GetFloat64() {
			p.maxVal = value
		}
	}
	if p.dt == nil {
		p.values = append(p.values, value)
		if len(p.values) >= MaxDetailsNumsForOneQuery {
			p.dt = tdigest.New()
			for _, v := range p.values {
				p.dt.Add(v.GetFloat64(), 1)
			}
			p.values = nil
		}
		return
	}
	p.dt.Add(value.GetFloat64(), 1)
}

// GetPercentile returns the percentile `f` of the values.
func (p *Percentile[valueType]) GetPercentile(f float64) float64 {
	if p.dt == nil {
		if !p.isSorted {
			p.isSorted = true
			slices.SortFunc(p.values, func(i, j valueType) int {
				return cmp.Compare(i.GetFloat64(), j.GetFloat64())
			})
		}
		return p.values[int(float64(len(p.values))*f)].GetFloat64()
	}
	return p.dt.Quantile(f)
}

// GetMax returns the max value.
func (p *Percentile[valueType]) GetMax() valueType {
	return p.maxVal
}

// GetMin returns the min value.
func (p *Percentile[valueType]) GetMin() valueType {
	return p.minVal
}

// MergePercentile merges two Percentile.
func (p *Percentile[valueType]) MergePercentile(p2 *Percentile[valueType]) {
	p.isSorted = false
	if p2.dt == nil {
		for _, v := range p2.values {
			p.Add(v)
		}
		return
	}
	p.sumVal += p2.sumVal
	p.size += p2.size
	if p.dt == nil {
		p.dt = tdigest.New()
		for _, v := range p.values {
			p.dt.Add(v.GetFloat64(), 1)
		}
		p.values = nil
	}
	p.dt.AddCentroidList(p2.dt.Centroids())
}

// Size returns the size of the values.
func (p *Percentile[valueType]) Size() int {
	return p.size
}

// Sum returns the sum of the values.
func (p *Percentile[valueType]) Sum() float64 {
	return p.sumVal
}

// FormatDuration uses to format duration, this function will prune precision before format duration.
// Pruning precision is for human readability. The prune rule is:
//  1. if the duration was less than 1us, return the original string.
//  2. readable value >=10, keep 1 decimal, otherwise, keep 2 decimal. such as:
//     9.412345ms  -> 9.41ms
//     10.412345ms -> 10.4ms
//     5.999s      -> 6s
//     100.45µs    -> 100.5µs
func FormatDuration(d time.Duration) string {
	if d <= time.Microsecond {
		return d.String()
	}
	unit := getUnit(d)
	if unit == time.Nanosecond {
		return d.String()
	}
	integer := (d / unit) * unit //nolint:durationcheck
	decimal := float64(d%unit) / float64(unit)
	if d < 10*unit {
		decimal = math.Round(decimal*100) / 100
	} else {
		decimal = math.Round(decimal*10) / 10
	}
	d = integer + time.Duration(decimal*float64(unit))
	return d.String()
}

func getUnit(d time.Duration) time.Duration {
	if d >= time.Second {
		return time.Second
	} else if d >= time.Millisecond {
		return time.Millisecond
	} else if d >= time.Microsecond {
		return time.Microsecond
	}
	return time.Nanosecond
}
