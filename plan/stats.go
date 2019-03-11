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

	"github.com/juju/errors"
	"github.com/pingcap/tidb/expression"
	log "github.com/sirupsen/logrus"
)

// StatsInfo stores the basic information of statistics for the plan's output. It is used for cost estimation.
type StatsInfo struct {
	count       float64
	cardinality []float64
}

// String returns the string representation of the StatsInfo.
func (s *StatsInfo) String() string {
	return fmt.Sprintf("count %v, cardinality %v", s.count, s.cardinality)
}

// Count returns the count of the StatsInfo.
func (s *StatsInfo) Count() int64 {
	return int64(s.count)
}

// scale receives a selectivity and multiplies it with count and cardinality.
func (s *StatsInfo) scale(factor float64) *StatsInfo {
	profile := &StatsInfo{
		count:       s.count * factor,
		cardinality: make([]float64, len(s.cardinality)),
	}
	for i := range profile.cardinality {
		profile.cardinality[i] = s.cardinality[i] * factor
	}
	return profile
}

// We try to scale StatsInfo to an expectCnt which must be smaller than the derived cnt.
func (s *StatsInfo) scaleByExpectCnt(expectCnt float64) *StatsInfo {
	if expectCnt > s.count {
		return s
	}
	if s.count > 1.0 { // if s.count is too small, it will cause overflow
		return s.scale(expectCnt / s.count)
	}
	return s
}

func (p *basePlan) StatsInfo() *StatsInfo {
	return p.stats
}

func (p *LogicalTableDual) deriveStats() (*StatsInfo, error) {
	profile := &StatsInfo{
		count:       float64(p.RowCount),
		cardinality: make([]float64, p.Schema().Len()),
	}
	for i := range profile.cardinality {
		profile.cardinality[i] = float64(p.RowCount)
	}
	p.stats = profile
	return p.stats, nil
}

func (p *baseLogicalPlan) deriveStats() (*StatsInfo, error) {
	if len(p.children) > 1 {
		panic("LogicalPlans with more than one child should implement their own deriveStats().")
	}

	if len(p.children) == 1 {
		var err error
		p.stats, err = p.children[0].deriveStats()
		return p.stats, errors.Trace(err)
	}

	profile := &StatsInfo{
		count:       float64(1),
		cardinality: make([]float64, p.self.Schema().Len()),
	}
	for i := range profile.cardinality {
		profile.cardinality[i] = float64(1)
	}
	p.stats = profile
	return profile, nil
}

func (ds *DataSource) getStatsByFilter(conds expression.CNFExprs) *StatsInfo {
	profile := &StatsInfo{
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
	ds.stats = profile
	selectivity, err := ds.statisticTable.Selectivity(ds.ctx, conds)
	if err != nil {
		log.Warnf("An error happened: %v, we have to use the default selectivity", err.Error())
		selectivity = selectionFactor
	}
	return profile.scale(selectivity)
}

func (ds *DataSource) deriveStats() (*StatsInfo, error) {
	// PushDownNot here can convert query 'not (a != 1)' to 'a = 1'.
	for i, expr := range ds.pushedDownConds {
		ds.pushedDownConds[i] = expression.PushDownNot(nil, expr, false)
	}
	ds.statsAfterSelect = ds.getStatsByFilter(ds.pushedDownConds)
	for _, path := range ds.possibleAccessPaths {
		if path.isTablePath {
			noIntervalRanges, err := ds.deriveTablePathStats(path)
			if err != nil {
				return nil, errors.Trace(err)
			}
			// If there's only point range. Just remove other possible paths.
			if noIntervalRanges {
				ds.possibleAccessPaths[0] = path
				ds.possibleAccessPaths = ds.possibleAccessPaths[:1]
				break
			}
			continue
		}
		noIntervalRanges, err := ds.deriveIndexPathStats(path)
		if err != nil {
			return nil, errors.Trace(err)
		}
		// If there's only point range and this index is unique key. Just remove other possible paths.
		if noIntervalRanges && path.index.Unique {
			ds.possibleAccessPaths[0] = path
			ds.possibleAccessPaths = ds.possibleAccessPaths[:1]
			break
		}
	}
	return ds.statsAfterSelect, nil
}

func (p *LogicalSelection) deriveStats() (*StatsInfo, error) {
	childProfile, err := p.children[0].deriveStats()
	if err != nil {
		return nil, errors.Trace(err)
	}
	p.stats = childProfile.scale(selectionFactor)
	return p.stats, nil
}

func (p *LogicalUnionAll) deriveStats() (*StatsInfo, error) {
	p.stats = &StatsInfo{
		cardinality: make([]float64, p.Schema().Len()),
	}
	for _, child := range p.children {
		childProfile, err := child.deriveStats()
		if err != nil {
			return nil, errors.Trace(err)
		}
		p.stats.count += childProfile.count
		for i := range p.stats.cardinality {
			p.stats.cardinality[i] += childProfile.cardinality[i]
		}
	}
	return p.stats, nil
}

func (p *LogicalLimit) deriveStats() (*StatsInfo, error) {
	childProfile, err := p.children[0].deriveStats()
	if err != nil {
		return nil, errors.Trace(err)
	}
	p.stats = &StatsInfo{
		count:       math.Min(float64(p.Count), childProfile.count),
		cardinality: make([]float64, len(childProfile.cardinality)),
	}
	for i := range p.stats.cardinality {
		p.stats.cardinality[i] = math.Min(childProfile.cardinality[i], p.stats.count)
	}
	return p.stats, nil
}

func (lt *LogicalTopN) deriveStats() (*StatsInfo, error) {
	childProfile, err := lt.children[0].deriveStats()
	if err != nil {
		return nil, errors.Trace(err)
	}
	lt.stats = &StatsInfo{
		count:       math.Min(float64(lt.Count), childProfile.count),
		cardinality: make([]float64, len(childProfile.cardinality)),
	}
	for i := range lt.stats.cardinality {
		lt.stats.cardinality[i] = math.Min(childProfile.cardinality[i], lt.stats.count)
	}
	return lt.stats, nil
}

// getCardinality will return the cardinality of a couple of columns. We simply return the max one, because we cannot know
// the cardinality for multi-dimension attributes properly. This is a simple and naive scheme of cardinality estimation.
func getCardinality(cols []*expression.Column, schema *expression.Schema, profile *StatsInfo) float64 {
	indices := schema.ColumnsIndices(cols)
	if indices == nil {
		log.Errorf("Cannot find column %s indices from schema %s", cols, schema)
		return 0
	}
	var cardinality = 1.0
	for _, idx := range indices {
		// It is a very elementary estimation.
		cardinality = math.Max(cardinality, profile.cardinality[idx])
	}
	return cardinality
}

func (p *LogicalProjection) deriveStats() (*StatsInfo, error) {
	childProfile, err := p.children[0].deriveStats()
	if err != nil {
		return nil, errors.Trace(err)
	}
	p.stats = &StatsInfo{
		count:       childProfile.count,
		cardinality: make([]float64, len(p.Exprs)),
	}
	for i, expr := range p.Exprs {
		cols := expression.ExtractColumns(expr)
		p.stats.cardinality[i] = getCardinality(cols, p.children[0].Schema(), childProfile)
	}
	return p.stats, nil
}

func (la *LogicalAggregation) deriveStats() (*StatsInfo, error) {
	childProfile, err := la.children[0].deriveStats()
	if err != nil {
		return nil, errors.Trace(err)
	}
	gbyCols := make([]*expression.Column, 0, len(la.GroupByItems))
	for _, gbyExpr := range la.GroupByItems {
		cols := expression.ExtractColumns(gbyExpr)
		gbyCols = append(gbyCols, cols...)
	}
	cardinality := getCardinality(gbyCols, la.children[0].Schema(), childProfile)
	la.stats = &StatsInfo{
		count:       cardinality,
		cardinality: make([]float64, la.schema.Len()),
	}
	// We cannot estimate the cardinality for every output, so we use a conservative strategy.
	for i := range la.stats.cardinality {
		la.stats.cardinality[i] = cardinality
	}
	la.inputCount = childProfile.count
	return la.stats, nil
}

// deriveStats prepares StatsInfo.
// If the type of join is SemiJoin, the selectivity of it will be same as selection's.
// If the type of join is LeftOuterSemiJoin, it will not add or remove any row. The last column is a boolean value, whose cardinality should be two.
// If the type of join is inner/outer join, the output of join(s, t) should be N(s) * N(t) / (V(s.key) * V(t.key)) * Min(s.key, t.key).
// N(s) stands for the number of rows in relation s. V(s.key) means the cardinality of join key in s.
// This is a quite simple strategy: We assume every bucket of relation which will participate join has the same number of rows, and apply cross join for
// every matched bucket.
func (p *LogicalJoin) deriveStats() (*StatsInfo, error) {
	leftProfile, err := p.children[0].deriveStats()
	if err != nil {
		return nil, errors.Trace(err)
	}
	rightProfile, err := p.children[1].deriveStats()
	if err != nil {
		return nil, errors.Trace(err)
	}
	if p.JoinType == SemiJoin || p.JoinType == AntiSemiJoin {
		p.stats = &StatsInfo{
			count:       leftProfile.count * selectionFactor,
			cardinality: make([]float64, len(leftProfile.cardinality)),
		}
		for i := range p.stats.cardinality {
			p.stats.cardinality[i] = leftProfile.cardinality[i] * selectionFactor
		}
		return p.stats, nil
	}
	if p.JoinType == LeftOuterSemiJoin || p.JoinType == AntiLeftOuterSemiJoin {
		p.stats = &StatsInfo{
			count:       leftProfile.count,
			cardinality: make([]float64, p.schema.Len()),
		}
		copy(p.stats.cardinality, leftProfile.cardinality)
		p.stats.cardinality[len(p.stats.cardinality)-1] = 2.0
		return p.stats, nil
	}
	if 0 == len(p.EqualConditions) {
		p.stats = &StatsInfo{
			count:       leftProfile.count * rightProfile.count,
			cardinality: append(leftProfile.cardinality, rightProfile.cardinality...),
		}
		return p.stats, nil
	}
	leftKeyCardinality := getCardinality(p.LeftJoinKeys, p.children[0].Schema(), leftProfile)
	rightKeyCardinality := getCardinality(p.RightJoinKeys, p.children[1].Schema(), rightProfile)
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
	p.stats = &StatsInfo{
		count:       count,
		cardinality: cardinality,
	}
	return p.stats, nil
}

func (la *LogicalApply) deriveStats() (*StatsInfo, error) {
	leftProfile, err := la.children[0].deriveStats()
	if err != nil {
		return nil, errors.Trace(err)
	}
	_, err = la.children[1].deriveStats()
	if err != nil {
		return nil, errors.Trace(err)
	}
	la.stats = &StatsInfo{
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
	return la.stats, nil
}

// Exists and MaxOneRow produce at most one row, so we set the count of stats one.
func getSingletonStats(len int) *StatsInfo {
	ret := &StatsInfo{
		count:       1.0,
		cardinality: make([]float64, len),
	}
	for i := 0; i < len; i++ {
		ret.cardinality[i] = 1
	}
	return ret
}

func (p *LogicalExists) deriveStats() (*StatsInfo, error) {
	_, err := p.children[0].deriveStats()
	if err != nil {
		return nil, errors.Trace(err)
	}
	p.stats = getSingletonStats(1)
	return p.stats, nil
}

func (p *LogicalMaxOneRow) deriveStats() (*StatsInfo, error) {
	_, err := p.children[0].deriveStats()
	if err != nil {
		return nil, errors.Trace(err)
	}
	p.stats = getSingletonStats(p.Schema().Len())
	return p.stats, nil
}
