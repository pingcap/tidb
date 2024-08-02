// Copyright 2024 PingCAP, Inc.
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

package costusage

import (
	"fmt"
	"strconv"

	"github.com/pingcap/tidb/pkg/planner/util/optimizetrace"
)

// optimizetrace and costusage is isolated from `util` because `core/base` depended on them
// for interface definition. Ideally, the dependency chain should be:
//
// `base` <- `util`/`util.coreusage` <- `core`
//    ^ +---------------^                  |
//    +------------------------------------+
//
// since `base` depended on optimizetrace and costusage for definition, we should separate
// them out of `util`/`util.coreusage` to avoid import cycle.
//
// util.optimizetrace/util.costusage  <- `base` <- `util`/`util.coreusage` <- `core`
//   				^			            ^                                    ||
//   				|			            +------------------------------------+|
//                  +-------------------------------------------------------------+

const (
	// CostFlagRecalculate indicates the optimizer to ignore cached cost and recalculate it again.
	CostFlagRecalculate uint64 = 1 << iota

	// CostFlagUseTrueCardinality indicates the optimizer to use true cardinality to calculate the cost.
	CostFlagUseTrueCardinality

	// CostFlagTrace indicates whether to trace the cost calculation.
	CostFlagTrace
)

func init() {
	optimizetrace.CostFlagTrace = CostFlagTrace
}

// CostVer2 is a structure  of cost basic of version2
type CostVer2 struct {
	cost  float64
	trace *CostTrace
}

// GetCost returns the cost value of the costVer2
func (c *CostVer2) GetCost() float64 {
	return c.cost
}

// GetTrace returns the trace of current costVer2
func (c *CostVer2) GetTrace() *CostTrace {
	return c.trace
}

// CostTrace record the basic factor and formula in cost est.
type CostTrace struct {
	factorCosts map[string]float64 // map[factorName]cost, used to calibrate the cost model
	formula     string             // It used to trace the cost calculation.
}

// GetFormula return the formula of current costTrace.
func (c *CostTrace) GetFormula() string {
	return c.formula
}

// GetFactorCosts return the factors of current costTrace.
func (c *CostTrace) GetFactorCosts() map[string]float64 {
	return c.factorCosts
}

// NewZeroCostVer2 return a new zero costVer2.
func NewZeroCostVer2(trace bool) (ret CostVer2) {
	if trace {
		ret.trace = &CostTrace{make(map[string]float64), ""}
	}
	return
}

func hasCostFlag(costFlag, flag uint64) bool {
	return (costFlag & flag) > 0
}

// TraceCost indicates whether to trace cost.
func TraceCost(option *optimizetrace.PlanCostOption) bool {
	if option != nil && hasCostFlag(option.CostFlag, CostFlagTrace) {
		return true
	}
	return false
}

// NewCostVer2 is the constructor of CostVer2.
func NewCostVer2(option *optimizetrace.PlanCostOption, factor CostVer2Factor, cost float64,
	lazyFormula func() string) (ret CostVer2) {
	ret.cost = cost
	if TraceCost(option) {
		ret.trace = &CostTrace{make(map[string]float64), ""}
		ret.trace.factorCosts[factor.Name] = cost
		ret.trace.formula = lazyFormula()
	}
	return ret
}

// CostVer2Factor is a record of internal cost factor.
type CostVer2Factor struct {
	Name  string
	Value float64
}

// String return the current CostVer2Factor's format string.
func (f CostVer2Factor) String() string {
	return fmt.Sprintf("%s(%v)", f.Name, f.Value)
}

// SumCostVer2 sum the cost up of all the passed args.
func SumCostVer2(costs ...CostVer2) (ret CostVer2) {
	if len(costs) == 0 {
		return
	}
	for _, c := range costs {
		ret.cost += c.cost
		if c.trace != nil {
			if ret.trace == nil { // init
				ret.trace = &CostTrace{make(map[string]float64), ""}
			}
			for factor, factorCost := range c.trace.factorCosts {
				ret.trace.factorCosts[factor] += factorCost
			}
			if ret.trace.formula != "" {
				ret.trace.formula += " + "
			}
			ret.trace.formula += "(" + c.trace.formula + ")"
		}
	}
	return ret
}

// DivCostVer2 is div utility func of CostVer2.
func DivCostVer2(cost CostVer2, denominator float64) (ret CostVer2) {
	ret.cost = cost.cost / denominator
	if cost.trace != nil {
		ret.trace = &CostTrace{make(map[string]float64), ""}
		for f, c := range cost.trace.factorCosts {
			ret.trace.factorCosts[f] = c / denominator
		}
		ret.trace.formula = "(" + cost.trace.formula + ")/" + strconv.FormatFloat(denominator, 'f', 2, 64)
	}
	return ret
}

// MulCostVer2 is mul utility func of CostVer2.
func MulCostVer2(cost CostVer2, scale float64) (ret CostVer2) {
	ret.cost = cost.cost * scale
	if cost.trace != nil {
		ret.trace = &CostTrace{make(map[string]float64), ""}
		for f, c := range cost.trace.factorCosts {
			ret.trace.factorCosts[f] = c * scale
		}
		ret.trace.formula = "(" + cost.trace.formula + ")*" + strconv.FormatFloat(scale, 'f', 2, 64)
	}
	return ret
}

// ZeroCostVer2 is a pre-defined zero CostVer2.
var ZeroCostVer2 = NewZeroCostVer2(false)
