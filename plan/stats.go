// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"fmt"
	"math"

	"github.com/pingcap/tidb/expression"
	log "github.com/sirupsen/logrus"
)

// statsInfo stores the basic information of statistics for the plan's output. It is used for cost estimation.
type statsInfo struct {
	count       float64
	cardinality []float64
}

func (s *statsInfo) String() string {
	return fmt.Sprintf("count %v, cardinality %v", s.count, s.cardinality)
}

func (s *statsInfo) Count() int64 {
	return int64(s.count)
}

// scale receives a selectivity and multiplies it with count and cardinality.
func (s *statsInfo) scale(factor float64) *statsInfo {
	profile := &statsInfo{
		count:       s.count * factor,
		cardinality: make([]float64, len(s.cardinality)),
	}
	for i := range profile.cardinality {
		profile.cardinality[i] = s.cardinality[i] * factor
	}
	return profile
}

// We try to scale statsInfo to an expectCnt which must be smaller than the derived cnt.
func (s *statsInfo) scaleByExpectCnt(expectCnt float64) *statsInfo {
	if expectCnt > s.count {
		return s
	}
	if s.count > 1.0 { // if s.count is too small, it will cause overflow
		return s.scale(expectCnt / s.count)
	}
	return s
}

func (p *basePhysicalPlan) StatsInfo() *statsInfo {
	return p.stats
}

func (p *baseLogicalPlan) deriveStats() *statsInfo {
	if len(p.children) == 0 {
		profile := &statsInfo{
			count:       float64(1),
			cardinality: make([]float64, p.self.Schema().Len()),
		}
		for i := range profile.cardinality {
			profile.cardinality[i] = float64(1)
		}
		p.stats = profile
		return profile
	}
	p.stats = p.children[0].deriveStats()
	return p.stats
}

func (ds *DataSource) getStatsByFilter(conds expression.CNFExprs) *statsInfo {
	profile := &statsInfo{
		count:       float64(ds.statisticTable.Count),
		cardinality: make([]float64, len(ds.Columns)),
	}
	for i, col := range ds.Columns {
		hist, ok := ds.statisticTable.Columns[col.ID]
		if ok && hist.Count > 0 {
			factor := float64(ds.statisticTable.Count) / float64(hist.Count)
			profile.cardinality[i] = float64(hist.NDV) * factor
		} else {
			profile.cardinality[i] = profile.count * distinctFactor
		}
	}
	selectivity, err := ds.statisticTable.Selectivity(ds.ctx, conds)
	if err != nil {
		log.Warnf("An error happened: %v, we have to use the default selectivity", err.Error())
		selectivity = selectionFactor
	}
	return profile.scale(selectivity)
}

func (ds *DataSource) deriveStats() *statsInfo {
	// PushDownNot here can convert query 'not (a != 1)' to 'a = 1'.
	for i, expr := range ds.pushedDownConds {
		ds.pushedDownConds[i] = expression.PushDownNot(expr, false, nil)
	}
	ds.stats = ds.getStatsByFilter(ds.pushedDownConds)
	return ds.stats
}

func (p *LogicalSelection) deriveStats() *statsInfo {
	childProfile := p.children[0].deriveStats()
	p.stats = childProfile.scale(selectionFactor)
	return p.stats
}

func (p *LogicalUnionAll) deriveStats() *statsInfo {
	p.stats = &statsInfo{
		cardinality: make([]float64, p.Schema().Len()),
	}
	for _, child := range p.children {
		childProfile := child.deriveStats()
		p.stats.count += childProfile.count
		for i := range p.stats.cardinality {
			p.stats.cardinality[i] += childProfile.cardinality[i]
		}
	}
	return p.stats
}

func (p *LogicalLimit) deriveStats() *statsInfo {
	childProfile := p.children[0].deriveStats()
	p.stats = &statsInfo{
		count:       float64(p.Count),
		cardinality: make([]float64, len(childProfile.cardinality)),
	}
	if p.stats.count > childProfile.count {
		p.stats.count = childProfile.count
	}
	for i := range p.stats.cardinality {
		p.stats.cardinality[i] = childProfile.cardinality[i]
		if p.stats.cardinality[i] > p.stats.count {
			p.stats.cardinality[i] = p.stats.count
		}
	}
	return p.stats
}

func (lt *LogicalTopN) deriveStats() *statsInfo {
	childProfile := lt.children[0].deriveStats()
	lt.stats = &statsInfo{
		count:       float64(lt.Count),
		cardinality: make([]float64, len(childProfile.cardinality)),
	}
	if lt.stats.count > childProfile.count {
		lt.stats.count = childProfile.count
	}
	for i := range lt.stats.cardinality {
		lt.stats.cardinality[i] = childProfile.cardinality[i]
		if lt.stats.cardinality[i] > lt.stats.count {
			lt.stats.cardinality[i] = lt.stats.count
		}
	}
	return lt.stats
}

// getCardinality will return the cardinality of a couple of columns. We simply return the max one, because we cannot know
// the cardinality for multi-dimension attributes properly. This is a simple and naive scheme of cardinality estimation.
func getCardinality(cols []*expression.Column, schema *expression.Schema, profile *statsInfo) float64 {
	indices := schema.ColumnsIndices(cols)
	if indices == nil {
		log.Errorf("Cannot find column %s indices from schema %s", cols, schema)
		return 0
	}
	var cardinality = 1.0
	for _, idx := range indices {
		if cardinality < profile.cardinality[idx] {
			// It is a very elementary estimation.
			cardinality = profile.cardinality[idx]
		}
	}
	return cardinality
}

func (p *LogicalProjection) deriveStats() *statsInfo {
	childProfile := p.children[0].deriveStats()
	p.stats = &statsInfo{
		count:       childProfile.count,
		cardinality: make([]float64, len(p.Exprs)),
	}
	for i, expr := range p.Exprs {
		cols := expression.ExtractColumns(expr)
		p.stats.cardinality[i] = getCardinality(cols, p.children[0].Schema(), childProfile)
	}
	return p.stats
}

func (la *LogicalAggregation) deriveStats() *statsInfo {
	childProfile := la.children[0].deriveStats()
	gbyCols := make([]*expression.Column, 0, len(la.GroupByItems))
	for _, gbyExpr := range la.GroupByItems {
		cols := expression.ExtractColumns(gbyExpr)
		gbyCols = append(gbyCols, cols...)
	}
	cardinality := getCardinality(gbyCols, la.children[0].Schema(), childProfile)
	la.stats = &statsInfo{
		count:       cardinality,
		cardinality: make([]float64, la.schema.Len()),
	}
	// We cannot estimate the cardinality for every output, so we use a conservative strategy.
	for i := range la.stats.cardinality {
		la.stats.cardinality[i] = cardinality
	}
	la.inputCount = childProfile.count
	return la.stats
}

// deriveStats prepares statsInfo.
// If the type of join is SemiJoin, the selectivity of it will be same as selection's.
// If the type of join is LeftOuterSemiJoin, it will not add or remove any row. The last column is a boolean value, whose cardinality should be two.
// If the type of join is inner/outer join, the output of join(s, t) should be N(s) * N(t) / (V(s.key) * V(t.key)) * Min(s.key, t.key).
// N(s) stands for the number of rows in relation s. V(s.key) means the cardinality of join key in s.
// This is a quite simple strategy: We assume every bucket of relation which will participate join has the same number of rows, and apply cross join for
// every matched bucket.
func (p *LogicalJoin) deriveStats() *statsInfo {
	leftProfile := p.children[0].deriveStats()
	rightProfile := p.children[1].deriveStats()
	if p.JoinType == SemiJoin || p.JoinType == AntiSemiJoin {
		p.stats = &statsInfo{
			count:       leftProfile.count * selectionFactor,
			cardinality: make([]float64, len(leftProfile.cardinality)),
		}
		for i := range p.stats.cardinality {
			p.stats.cardinality[i] = leftProfile.cardinality[i] * selectionFactor
		}
		return p.stats
	}
	if p.JoinType == LeftOuterSemiJoin || p.JoinType == AntiLeftOuterSemiJoin {
		p.stats = &statsInfo{
			count:       leftProfile.count,
			cardinality: make([]float64, p.schema.Len()),
		}
		copy(p.stats.cardinality, leftProfile.cardinality)
		p.stats.cardinality[len(p.stats.cardinality)-1] = 2.0
		return p.stats
	}
	if 0 == len(p.EqualConditions) {
		p.stats = &statsInfo{
			count:       leftProfile.count * rightProfile.count,
			cardinality: append(leftProfile.cardinality, rightProfile.cardinality...),
		}
		return p.stats
	}
	leftKeys := make([]*expression.Column, 0, len(p.EqualConditions))
	rightKeys := make([]*expression.Column, 0, len(p.EqualConditions))
	for _, eqCond := range p.EqualConditions {
		leftKeys = append(leftKeys, eqCond.GetArgs()[0].(*expression.Column))
		rightKeys = append(rightKeys, eqCond.GetArgs()[1].(*expression.Column))
	}
	leftKeyCardinality := getCardinality(leftKeys, p.children[0].Schema(), leftProfile)
	rightKeyCardinality := getCardinality(rightKeys, p.children[1].Schema(), rightProfile)
	count := leftProfile.count * rightProfile.count / math.Max(leftKeyCardinality, rightKeyCardinality)
	if p.JoinType == LeftOuterJoin {
		count = math.Max(count, leftProfile.count)
	} else if p.JoinType == RightOuterJoin {
		count = math.Max(count, rightProfile.count)
	}
	cardinality := make([]float64, 0, p.schema.Len())
	cardinality = append(cardinality, leftProfile.cardinality...)
	cardinality = append(cardinality, rightProfile.cardinality...)
	for i := range cardinality {
		cardinality[i] = math.Min(cardinality[i], count)
	}
	p.stats = &statsInfo{
		count:       count,
		cardinality: cardinality,
	}
	return p.stats
}

func (la *LogicalApply) deriveStats() *statsInfo {
	leftProfile := la.children[0].deriveStats()
	_ = la.children[1].deriveStats()
	la.stats = &statsInfo{
		count:       leftProfile.count,
		cardinality: make([]float64, la.schema.Len()),
	}
	copy(la.stats.cardinality, leftProfile.cardinality)
	if la.JoinType == LeftOuterSemiJoin || la.JoinType == AntiLeftOuterSemiJoin {
		la.stats.cardinality[len(la.stats.cardinality)-1] = 2.0
	} else {
		for i := la.children[0].Schema().Len(); i < la.schema.Len(); i++ {
			la.stats.cardinality[i] = leftProfile.count
		}
	}
	return la.stats
}

// Exists and MaxOneRow produce at most one row, so we set the count of stats one.
func getSingletonStats(len int) *statsInfo {
	ret := &statsInfo{
		count:       1.0,
		cardinality: make([]float64, len),
	}
	for i := 0; i < len; i++ {
		ret.cardinality[i] = 1
	}
	return ret
}

func (p *LogicalExists) deriveStats() *statsInfo {
	p.children[0].deriveStats()
	p.stats = getSingletonStats(1)
	return p.stats
}

func (p *LogicalMaxOneRow) deriveStats() *statsInfo {
	p.children[0].deriveStats()
	p.stats = getSingletonStats(p.Schema().Len())
	return p.stats
}
