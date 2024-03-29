package coreusage

import (
	"fmt"
	"strconv"
)

const (
	// CostFlagRecalculate indicates the optimizer to ignore cached cost and recalculate it again.
	CostFlagRecalculate uint64 = 1 << iota

	// CostFlagUseTrueCardinality indicates the optimizer to use true cardinality to calculate the cost.
	CostFlagUseTrueCardinality

	// CostFlagTrace indicates whether to trace the cost calculation.
	CostFlagTrace
)

type CostVer2 struct {
	cost  float64
	trace *CostTrace
}

func (c *CostVer2) GetCost() float64 {
	return c.cost
}

func (c *CostVer2) GetTrace() *CostTrace {
	return c.trace
}

type CostTrace struct {
	factorCosts map[string]float64 // map[factorName]cost, used to calibrate the cost model
	formula     string             // It used to trace the cost calculation.
}

func (c *CostTrace) GetFormula() string {
	return c.formula
}

func (c *CostTrace) GetFactorCosts() map[string]float64 {
	return c.factorCosts
}

func NewZeroCostVer2(trace bool) (ret CostVer2) {
	if trace {
		ret.trace = &CostTrace{make(map[string]float64), ""}
	}
	return
}

func hasCostFlag(costFlag, flag uint64) bool {
	return (costFlag & flag) > 0
}

func TraceCost(option *PlanCostOption) bool {
	if option != nil && hasCostFlag(option.CostFlag, CostFlagTrace) {
		return true
	}
	return false
}

func NewCostVer2(option *PlanCostOption, factor CostVer2Factor, cost float64, lazyFormula func() string) (ret CostVer2) {
	ret.cost = cost
	if TraceCost(option) {
		ret.trace = &CostTrace{make(map[string]float64), ""}
		ret.trace.factorCosts[factor.Name] = cost
		ret.trace.formula = lazyFormula()
	}
	return ret
}

type CostVer2Factor struct {
	Name  string
	Value float64
}

func (f CostVer2Factor) String() string {
	return fmt.Sprintf("%s(%v)", f.Name, f.Value)
}

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

var ZeroCostVer2 = NewZeroCostVer2(false)
