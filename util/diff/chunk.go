// Copyright 2018 PingCAP, Inc.
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

package diff

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb-tools/pkg/dbutil"
	"github.com/pingcap/tidb-tools/pkg/utils"
	"github.com/pingcap/tidb/parser/model"
	"go.uber.org/zap"
)

var (
	equal = "="
	lt    = "<"
	lte   = "<="
	gt    = ">"
	gte   = ">="

	bucketMode = "bucketMode"
	normalMode = "normalMode"
)

// Bound represents a bound for a column
type Bound struct {
	Column string `json:"column"`
	Lower  string `json:"lower"`
	Upper  string `json:"upper"`

	HasLower bool `json:"has-lower"`
	HasUpper bool `json:"has-upper"`
}

// ChunkRange represents chunk range
type ChunkRange struct {
	ID     int      `json:"id"`
	Bounds []*Bound `json:"bounds"`

	Where string   `json:"where"`
	Args  []string `json:"args"`

	State string `json:"state"`

	columnOffset map[string]int
}

// NewChunkRange return a ChunkRange.
func NewChunkRange() *ChunkRange {
	return &ChunkRange{
		Bounds:       make([]*Bound, 0, 2),
		columnOffset: make(map[string]int),
	}
}

// String returns the string of ChunkRange, used for log.
func (c *ChunkRange) String() string {
	chunkBytes, err := json.Marshal(c)
	if err != nil {
		log.Warn("fail to encode chunk into string", zap.Error(err))
		return ""
	}

	return string(chunkBytes)
}

func (c *ChunkRange) toString(collation string) (string, []string) {
	if collation != "" {
		collation = fmt.Sprintf(" COLLATE '%s'", collation)
	}

	/* for example:
	there is a bucket in TiDB, and the lowerbound and upperbound are (v1, v3), (v2, v4), and the columns are `a` and `b`,
	this bucket's data range is (a > v1 or (a == v1 and b > v3)) and (a < v2 or (a == v2 and b <= v4))
	*/

	lowerCondition := make([]string, 0, 1)
	upperCondition := make([]string, 0, 1)
	lowerArgs := make([]string, 0, 1)
	upperArgs := make([]string, 0, 1)

	preConditionForLower := make([]string, 0, 1)
	preConditionForUpper := make([]string, 0, 1)
	preConditionArgsForLower := make([]string, 0, 1)
	preConditionArgsForUpper := make([]string, 0, 1)

	for i, bound := range c.Bounds {
		lowerSymbol := gt
		upperSymbol := lt
		if i == len(c.Bounds)-1 {
			upperSymbol = lte
		}

		if bound.HasLower {
			if len(preConditionForLower) > 0 {
				lowerCondition = append(lowerCondition, fmt.Sprintf("(%s AND %s%s %s ?)", strings.Join(preConditionForLower, " AND "), dbutil.ColumnName(bound.Column), collation, lowerSymbol))
				lowerArgs = append(append(lowerArgs, preConditionArgsForLower...), bound.Lower)
			} else {
				lowerCondition = append(lowerCondition, fmt.Sprintf("(%s%s %s ?)", dbutil.ColumnName(bound.Column), collation, lowerSymbol))
				lowerArgs = append(lowerArgs, bound.Lower)
			}
			preConditionForLower = append(preConditionForLower, fmt.Sprintf("%s = ?", dbutil.ColumnName(bound.Column)))
			preConditionArgsForLower = append(preConditionArgsForLower, bound.Lower)
		}

		if bound.HasUpper {
			if len(preConditionForUpper) > 0 {
				upperCondition = append(upperCondition, fmt.Sprintf("(%s AND %s%s %s ?)", strings.Join(preConditionForUpper, " AND "), dbutil.ColumnName(bound.Column), collation, upperSymbol))
				upperArgs = append(append(upperArgs, preConditionArgsForUpper...), bound.Upper)
			} else {
				upperCondition = append(upperCondition, fmt.Sprintf("(%s%s %s ?)", dbutil.ColumnName(bound.Column), collation, upperSymbol))
				upperArgs = append(upperArgs, bound.Upper)
			}
			preConditionForUpper = append(preConditionForUpper, fmt.Sprintf("%s = ?", dbutil.ColumnName(bound.Column)))
			preConditionArgsForUpper = append(preConditionArgsForUpper, bound.Upper)
		}
	}

	if len(upperCondition) == 0 && len(lowerCondition) == 0 {
		return "TRUE", nil
	}

	if len(upperCondition) == 0 {
		return strings.Join(lowerCondition, " OR "), lowerArgs
	}

	if len(lowerCondition) == 0 {
		return strings.Join(upperCondition, " OR "), upperArgs
	}

	return fmt.Sprintf("(%s) AND (%s)", strings.Join(lowerCondition, " OR "), strings.Join(upperCondition, " OR ")), append(lowerArgs, upperArgs...)
}

func (c *ChunkRange) addBound(bound *Bound) {
	c.Bounds = append(c.Bounds, bound)
	c.columnOffset[bound.Column] = len(c.Bounds) - 1
}

func (c *ChunkRange) updateColumnOffset() {
	c.columnOffset = make(map[string]int)
	for i, bound := range c.Bounds {
		c.columnOffset[bound.Column] = i
	}
}

func (c *ChunkRange) update(column, lower, upper string, updateLower, updateUpper bool) {
	if offset, ok := c.columnOffset[column]; ok {
		// update the bound
		if updateLower {
			c.Bounds[offset].Lower = lower
			c.Bounds[offset].HasLower = true
		}
		if updateUpper {
			c.Bounds[offset].Upper = upper
			c.Bounds[offset].HasUpper = true
		}

		return
	}

	// add a new bound
	c.addBound(&Bound{
		Column:   column,
		Lower:    lower,
		Upper:    upper,
		HasLower: updateLower,
		HasUpper: updateUpper,
	})
}

func (c *ChunkRange) copy() *ChunkRange {
	newChunk := NewChunkRange()
	for _, bound := range c.Bounds {
		newChunk.addBound(&Bound{
			Column:   bound.Column,
			Lower:    bound.Lower,
			Upper:    bound.Upper,
			HasLower: bound.HasLower,
			HasUpper: bound.HasUpper,
		})
	}

	return newChunk
}

func (c *ChunkRange) copyAndUpdate(column, lower, upper string, updateLower, updateUpper bool) *ChunkRange {
	newChunk := c.copy()
	newChunk.update(column, lower, upper, updateLower, updateUpper)
	return newChunk
}

type spliter interface {
	// split splits a table's data to several chunks.
	split(table *TableInstance, columns []*model.ColumnInfo, chunkSize int, limits string, collation string) ([]*ChunkRange, error)
}

type randomSpliter struct {
	table     *TableInstance
	chunkSize int
	limits    string
	collation string
}

func (s *randomSpliter) split(table *TableInstance, columns []*model.ColumnInfo, chunkSize int, limits string, collation string) ([]*ChunkRange, error) {
	s.table = table
	s.chunkSize = chunkSize
	s.limits = limits
	s.collation = collation

	// get the chunk count by data count and chunk size
	cnt, err := dbutil.GetRowCount(context.Background(), table.Conn, table.Schema, table.Table, limits, nil)
	if err != nil {
		return nil, errors.Trace(err)
	}

	chunkCnt := (int(cnt) + chunkSize - 1) / chunkSize
	log.Info("split range by random", zap.Int64("row count", cnt), zap.Int("split chunk num", chunkCnt))
	chunks, err := splitRangeByRandom(table.Conn, NewChunkRange(), chunkCnt, table.Schema, table.Table, columns, s.limits, s.collation)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return chunks, nil
}

// splitRangeByRandom splits a chunk to multiple chunks by random
func splitRangeByRandom(db *sql.DB, chunk *ChunkRange, count int, schema string, table string, columns []*model.ColumnInfo, limits, collation string) (chunks []*ChunkRange, err error) {
	if count <= 1 {
		chunks = append(chunks, chunk)
		return chunks, nil
	}

	chunkLimits, args := chunk.toString(collation)
	limitRange := fmt.Sprintf("(%s) AND %s", chunkLimits, limits)

	randomValues := make([][]string, len(columns))
	for i, column := range columns {
		randomValues[i], err = dbutil.GetRandomValues(context.Background(), db, schema, table, column.Name.O, count-1, limitRange, utils.StringsToInterfaces(args), collation)
		if err != nil {
			return nil, errors.Trace(err)
		}

		log.Debug("get split values by random", zap.Stringer("chunk", chunk), zap.String("column", column.Name.O), zap.Int("random values num", len(randomValues[i])))
	}

	for i := 0; i <= minLenInSlices(randomValues); i++ {
		newChunk := chunk.copy()

		for j, column := range columns {
			if i == 0 {
				if len(randomValues[j]) == 0 {
					break
				}
				newChunk.update(column.Name.O, "", randomValues[j][i], false, true)
			} else if i == len(randomValues[j]) {
				newChunk.update(column.Name.O, randomValues[j][i-1], "", true, false)
			} else {
				newChunk.update(column.Name.O, randomValues[j][i-1], randomValues[j][i], true, true)
			}
		}
		chunks = append(chunks, newChunk)
	}

	log.Debug("split range by random", zap.Stringer("origin chunk", chunk), zap.Int("split num", len(chunks)))

	return chunks, nil
}

type bucketSpliter struct {
	table     *TableInstance
	chunkSize int
	limits    string
	collation string
	buckets   map[string][]dbutil.Bucket
}

func (s *bucketSpliter) split(table *TableInstance, columns []*model.ColumnInfo, chunkSize int, limits string, collation string) ([]*ChunkRange, error) {
	s.table = table
	s.chunkSize = chunkSize
	s.limits = limits
	s.collation = collation

	buckets, err := dbutil.GetBucketsInfo(context.Background(), s.table.Conn, s.table.Schema, s.table.Table, s.table.info)
	if err != nil {
		return nil, errors.Trace(err)
	}
	s.buckets = buckets

	return s.getChunksByBuckets()
}

func (s *bucketSpliter) getChunksByBuckets() (chunks []*ChunkRange, err error) {
	indices := dbutil.FindAllIndex(s.table.info)
	for _, index := range indices {
		if index == nil {
			continue
		}
		buckets, ok := s.buckets[index.Name.O]
		if !ok {
			return nil, errors.NotFoundf("index %s in buckets info", index.Name.O)
		}
		log.Debug("buckets for index", zap.String("index", index.Name.O), zap.Reflect("buckets", buckets))

		var (
			lowerValues, upperValues []string
			latestCount              int64
		)

		indexColumns := getColumnsFromIndex(index, s.table.info)

		for i := 0; i <= len(buckets); i++ {
			if i != len(buckets) {
				upperValues, err = dbutil.AnalyzeValuesFromBuckets(buckets[i].UpperBound, indexColumns)
				if err != nil {
					return nil, errors.Trace(err)
				}
			}

			chunk := NewChunkRange()
			for j, column := range indexColumns {
				if i == 0 {
					chunk.update(column.Name.O, "", upperValues[j], false, true)
				} else if i == len(buckets) {
					if len(lowerValues) > 0 {
						chunk.update(column.Name.O, lowerValues[j], "", true, false)
					}
				} else {
					var lowerValue, upperValue string
					if len(lowerValues) > 0 {
						lowerValue = lowerValues[j]
					}
					if len(upperValues) > 0 {
						upperValue = upperValues[j]
					}
					chunk.update(column.Name.O, lowerValue, upperValue, len(lowerValues) > 0, len(upperValues) > 0)
				}
			}

			if i == len(buckets) {
				chunks = append(chunks, chunk)
				break
			}

			count := (buckets[i].Count - latestCount) / int64(s.chunkSize)
			if count == 0 {
				continue
			} else if count >= 2 {
				splitChunks, err := splitRangeByRandom(s.table.Conn, chunk, int(count), s.table.Schema, s.table.Table, indexColumns, s.limits, s.collation)
				if err != nil {
					return nil, errors.Trace(err)
				}
				chunks = append(chunks, splitChunks...)
			} else {
				chunks = append(chunks, chunk)
			}

			latestCount = buckets[i].Count
			lowerValues = upperValues
		}

		if len(chunks) != 0 {
			break
		}
	}

	return chunks, nil
}

func getChunksForTable(table *TableInstance, columns []*model.ColumnInfo, chunkSize int, limits string, collation string, useTiDBStatsInfo bool) ([]*ChunkRange, error) {
	if useTiDBStatsInfo {
		s := bucketSpliter{}
		chunks, err := s.split(table, columns, chunkSize, limits, collation)
		if err == nil && len(chunks) > 0 {
			return chunks, nil
		}

		log.Warn("use tidb bucket information to get chunks failed, will split chunk by random again", zap.Int("get chunk", len(chunks)), zap.Error(err))
	}

	// get chunks from tidb bucket information failed, use random.
	s := randomSpliter{}
	chunks, err := s.split(table, columns, chunkSize, limits, collation)
	return chunks, err
}

// getSplitFields returns fields to split chunks, order by pk, uk, index, columns.
func getSplitFields(table *model.TableInfo, splitFields []string) ([]*model.ColumnInfo, error) {
	cols := make([]*model.ColumnInfo, 0, len(table.Columns))
	colsMap := make(map[string]*model.ColumnInfo)

	splitCols := make([]*model.ColumnInfo, 0, 2)
	for _, splitField := range splitFields {
		col := dbutil.FindColumnByName(table.Columns, splitField)
		if col == nil {
			return nil, errors.NotFoundf("column %s in table %s", splitField, table.Name)

		}
		splitCols = append(splitCols, col)
	}

	if len(splitCols) != 0 {
		return splitCols, nil
	}

	for _, col := range table.Columns {
		colsMap[col.Name.O] = col
	}
	indices := dbutil.FindAllIndex(table)
	if len(indices) != 0 {
		for _, col := range indices[0].Columns {
			cols = append(cols, colsMap[col.Name.O])
		}
		return cols, nil
	}

	return []*model.ColumnInfo{table.Columns[0]}, nil
}

// SplitChunks splits the table to some chunks.
func SplitChunks(ctx context.Context, table *TableInstance, splitFields, limits string, chunkSize int, collation string, useTiDBStatsInfo bool, cpDB *sql.DB) (chunks []*ChunkRange, err error) {
	var splitFieldArr []string
	if len(splitFields) != 0 {
		splitFieldArr = strings.Split(splitFields, ",")
	}

	for i := range splitFieldArr {
		splitFieldArr[i] = strings.TrimSpace(splitFieldArr[i])
	}

	fields, err := getSplitFields(table.info, splitFieldArr)
	if err != nil {
		return nil, errors.Trace(err)
	}

	chunks, err = getChunksForTable(table, fields, chunkSize, limits, collation, useTiDBStatsInfo)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if chunks == nil {
		return nil, nil
	}

	for i, chunk := range chunks {
		conditions, args := chunk.toString(collation)
		chunk.ID = i
		chunk.Where = fmt.Sprintf("((%s) AND %s)", conditions, limits)
		chunk.Args = args
		chunk.State = notCheckedState
	}

	ctx1, cancel1 := context.WithTimeout(ctx, time.Duration(len(chunks))*dbutil.DefaultTimeout)
	defer cancel1()

	err = initChunks(ctx1, cpDB, table.InstanceID, table.Schema, table.Table, chunks)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return chunks, nil
}
