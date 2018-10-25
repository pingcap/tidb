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

package core

import (
	"math"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/planner/property"
	log "github.com/sirupsen/logrus"
)

func (p *basePhysicalPlan) StatsCount() float64 {
	return p.stats.RowCount
}

func (p *LogicalTableDual) deriveStats() (*property.StatsInfo, error) {
	profile := &property.StatsInfo{
		RowCount:    float64(p.RowCount),
		Cardinality: make([]float64, p.Schema().Len()),
	}
	for i := range profile.Cardinality {
		profile.Cardinality[i] = float64(p.RowCount)
	}
	p.stats = profile
	return p.stats, nil
}

func (p *baseLogicalPlan) deriveStats() (*property.StatsInfo, error) {
	if len(p.children) > 1 {
		panic("LogicalPlans with more than one child should implement their own deriveStats().")
	}

	if len(p.children) == 1 {
		var err error
		p.stats, err = p.children[0].deriveStats()
		return p.stats, errors.Trace(err)
	}

	profile := &property.StatsInfo{
		RowCount:    float64(1),
		Cardinality: make([]float64, p.self.Schema().Len()),
	}
	for i := range profile.Cardinality {
		profile.Cardinality[i] = float64(1)
	}
	p.stats = profile
	return profile, nil
}

func (ds *DataSource) getStatsByFilter(conds expression.CNFExprs) *property.StatsInfo {
	profile := &property.StatsInfo{
		RowCount:       float64(ds.statisticTable.Count),
		Cardinality:    make([]float64, len(ds.Columns)),
		HistColl:       ds.statisticTable.GenerateHistCollFromColumnInfo(ds.Columns, ds.schema.Columns),
		UsePseudoStats: ds.statisticTable.Pseudo,
	}
	for i, col := range ds.Columns {
		hist, ok := ds.statisticTable.Columns[col.ID]
		if ok && hist.Count > 0 {
			factor := float64(ds.statisticTable.Count) / float64(hist.Count)
			profile.Cardinality[i] = float64(hist.NDV) * factor
		} else {
			profile.Cardinality[i] = profile.RowCount * distinctFactor
		}
	}
	ds.stats = profile
	selectivity, err := profile.HistColl.Selectivity(ds.ctx, conds)
	if err != nil {
		log.Warnf("An error happened: %v, we have to use the default selectivity", err.Error())
		selectivity = selectionFactor
	}
	return profile.Scale(selectivity)
}

func (ds *DataSource) deriveStats() (*property.StatsInfo, error) {
	// PushDownNot here can convert query 'not (a != 1)' to 'a = 1'.
	for i, expr := range ds.pushedDownConds {
		ds.pushedDownConds[i] = expression.PushDownNot(nil, expr, false)
	}
	ds.stats = ds.getStatsByFilter(ds.pushedDownConds)
	for _, path := range ds.possibleAccessPaths {
		if path.isTablePath {
			noIntervalRanges, err := ds.deriveTablePathStats(path)
			if err != nil {
				return nil, errors.Trace(err)
			}
			// If we have point or empty range, just remove other possible paths.
			if noIntervalRanges || len(path.ranges) == 0 {
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
		// If we have empty range, or point range on unique index, just remove other possible paths.
		if (noIntervalRanges && path.index.Unique) || len(path.ranges) == 0 {
			ds.possibleAccessPaths[0] = path
			ds.possibleAccessPaths = ds.possibleAccessPaths[:1]
			break
		}
	}
	return ds.stats, nil
}

func (p *LogicalSelection) deriveStats() (*property.StatsInfo, error) {
	childProfile, err := p.children[0].deriveStats()
	if err != nil {
		return nil, errors.Trace(err)
	}
	p.stats = childProfile.Scale(selectionFactor)
	return p.stats, nil
}

func (p *LogicalUnionAll) deriveStats() (*property.StatsInfo, error) {
	p.stats = &property.StatsInfo{
		Cardinality: make([]float64, p.Schema().Len()),
	}
	for _, child := range p.children {
		childProfile, err := child.deriveStats()
		if err != nil {
			return nil, errors.Trace(err)
		}
		p.stats.RowCount += childProfile.RowCount
		for i := range p.stats.Cardinality {
			p.stats.Cardinality[i] += childProfile.Cardinality[i]
		}
	}
	return p.stats, nil
}

func (p *LogicalLimit) deriveStats() (*property.StatsInfo, error) {
	childProfile, err := p.children[0].deriveStats()
	if err != nil {
		return nil, errors.Trace(err)
	}
	p.stats = &property.StatsInfo{
		RowCount:    math.Min(float64(p.Count), childProfile.RowCount),
		Cardinality: make([]float64, len(childProfile.Cardinality)),
	}
	for i := range p.stats.Cardinality {
		p.stats.Cardinality[i] = math.Min(childProfile.Cardinality[i], p.stats.RowCount)
	}
	return p.stats, nil
}

func (lt *LogicalTopN) deriveStats() (*property.StatsInfo, error) {
	childProfile, err := lt.children[0].deriveStats()
	if err != nil {
		return nil, errors.Trace(err)
	}
	lt.stats = &property.StatsInfo{
		RowCount:    math.Min(float64(lt.Count), childProfile.RowCount),
		Cardinality: make([]float64, len(childProfile.Cardinality)),
	}
	for i := range lt.stats.Cardinality {
		lt.stats.Cardinality[i] = math.Min(childProfile.Cardinality[i], lt.stats.RowCount)
	}
	return lt.stats, nil
}

// getCardinality will return the Cardinality of a couple of columns. We simply return the max one, because we cannot know
// the Cardinality for multi-dimension attributes properly. This is a simple and naive scheme of Cardinality estimation.
func getCardinality(cols []*expression.Column, schema *expression.Schema, profile *property.StatsInfo) float64 {
	indices := schema.ColumnsIndices(cols)
	if indices == nil {
		log.Errorf("Cannot find column %v indices from schema %s", cols, schema)
		return 0
	}
	var cardinality = 1.0
	for _, idx := range indices {
		// It is a very elementary estimation.
		cardinality = math.Max(cardinality, profile.Cardinality[idx])
	}
	return cardinality
}

func (p *LogicalProjection) deriveStats() (*property.StatsInfo, error) {
	childProfile, err := p.children[0].deriveStats()
	if err != nil {
		return nil, errors.Trace(err)
	}
	p.stats = &property.StatsInfo{
		RowCount:    childProfile.RowCount,
		Cardinality: make([]float64, len(p.Exprs)),
	}
	for i, expr := range p.Exprs {
		cols := expression.ExtractColumns(expr)
		p.stats.Cardinality[i] = getCardinality(cols, p.children[0].Schema(), childProfile)
	}
	return p.stats, nil
}

func (la *LogicalAggregation) deriveStats() (*property.StatsInfo, error) {
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
	la.stats = &property.StatsInfo{
		RowCount:    cardinality,
		Cardinality: make([]float64, la.schema.Len()),
	}
	// We cannot estimate the Cardinality for every output, so we use a conservative strategy.
	for i := range la.stats.Cardinality {
		la.stats.Cardinality[i] = cardinality
	}
	la.inputCount = childProfile.RowCount
	return la.stats, nil
}

// deriveStats prepares property.StatsInfo.
// If the type of join is SemiJoin, the selectivity of it will be same as selection's.
// If the type of join is LeftOuterSemiJoin, it will not add or remove any row. The last column is a boolean value, whose Cardinality should be two.
// If the type of join is inner/outer join, the output of join(s, t) should be N(s) * N(t) / (V(s.key) * V(t.key)) * Min(s.key, t.key).
// N(s) stands for the number of rows in relation s. V(s.key) means the Cardinality of join key in s.
// This is a quite simple strategy: We assume every bucket of relation which will participate join has the same number of rows, and apply cross join for
// every matched bucket.
func (p *LogicalJoin) deriveStats() (*property.StatsInfo, error) {
	leftProfile, err := p.children[0].deriveStats()
	if err != nil {
		return nil, errors.Trace(err)
	}
	rightProfile, err := p.children[1].deriveStats()
	if err != nil {
		return nil, errors.Trace(err)
	}
	if p.JoinType == SemiJoin || p.JoinType == AntiSemiJoin {
		p.stats = &property.StatsInfo{
			RowCount:    leftProfile.RowCount * selectionFactor,
			Cardinality: make([]float64, len(leftProfile.Cardinality)),
		}
		for i := range p.stats.Cardinality {
			p.stats.Cardinality[i] = leftProfile.Cardinality[i] * selectionFactor
		}
		return p.stats, nil
	}
	if p.JoinType == LeftOuterSemiJoin || p.JoinType == AntiLeftOuterSemiJoin {
		p.stats = &property.StatsInfo{
			RowCount:    leftProfile.RowCount,
			Cardinality: make([]float64, p.schema.Len()),
		}
		copy(p.stats.Cardinality, leftProfile.Cardinality)
		p.stats.Cardinality[len(p.stats.Cardinality)-1] = 2.0
		return p.stats, nil
	}
	if 0 == len(p.EqualConditions) {
		p.stats = &property.StatsInfo{
			RowCount:    leftProfile.RowCount * rightProfile.RowCount,
			Cardinality: append(leftProfile.Cardinality, rightProfile.Cardinality...),
		}
		return p.stats, nil
	}
	leftKeys := make([]*expression.Column, 0, len(p.EqualConditions))
	rightKeys := make([]*expression.Column, 0, len(p.EqualConditions))
	for _, eqCond := range p.EqualConditions {
		leftKeys = append(leftKeys, eqCond.GetArgs()[0].(*expression.Column))
		rightKeys = append(rightKeys, eqCond.GetArgs()[1].(*expression.Column))
	}
	leftKeyCardinality := getCardinality(leftKeys, p.children[0].Schema(), leftProfile)
	rightKeyCardinality := getCardinality(rightKeys, p.children[1].Schema(), rightProfile)
	count := leftProfile.RowCount * rightProfile.RowCount / math.Max(leftKeyCardinality, rightKeyCardinality)
	if p.JoinType == LeftOuterJoin {
		count = math.Max(count, leftProfile.RowCount)
	} else if p.JoinType == RightOuterJoin {
		count = math.Max(count, rightProfile.RowCount)
	}
	cardinality := make([]float64, 0, p.schema.Len())
	cardinality = append(cardinality, leftProfile.Cardinality...)
	cardinality = append(cardinality, rightProfile.Cardinality...)
	for i := range cardinality {
		cardinality[i] = math.Min(cardinality[i], count)
	}
	p.stats = &property.StatsInfo{
		RowCount:    count,
		Cardinality: cardinality,
	}
	return p.stats, nil
}

func (la *LogicalApply) deriveStats() (*property.StatsInfo, error) {
	leftProfile, err := la.children[0].deriveStats()
	if err != nil {
		return nil, errors.Trace(err)
	}
	_, err = la.children[1].deriveStats()
	if err != nil {
		return nil, errors.Trace(err)
	}
	la.stats = &property.StatsInfo{
		RowCount:    leftProfile.RowCount,
		Cardinality: make([]float64, la.schema.Len()),
	}
	copy(la.stats.Cardinality, leftProfile.Cardinality)
	if la.JoinType == LeftOuterSemiJoin || la.JoinType == AntiLeftOuterSemiJoin {
		la.stats.Cardinality[len(la.stats.Cardinality)-1] = 2.0
	} else {
		for i := la.children[0].Schema().Len(); i < la.schema.Len(); i++ {
			la.stats.Cardinality[i] = leftProfile.RowCount
		}
	}
	return la.stats, nil
}

// Exists and MaxOneRow produce at most one row, so we set the RowCount of stats one.
func getSingletonStats(len int) *property.StatsInfo {
	ret := &property.StatsInfo{
		RowCount:    1.0,
		Cardinality: make([]float64, len),
	}
	for i := 0; i < len; i++ {
		ret.Cardinality[i] = 1
	}
	return ret
}

func (p *LogicalMaxOneRow) deriveStats() (*property.StatsInfo, error) {
	_, err := p.children[0].deriveStats()
	if err != nil {
		return nil, errors.Trace(err)
	}
	p.stats = getSingletonStats(p.Schema().Len())
	return p.stats, nil
}
