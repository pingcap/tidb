// Copyright 2022 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cache

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/ttl/session"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/mathutil"
	"github.com/tikv/client-go/v2/tikv"
)

func getTableKeyColumns(tbl *model.TableInfo) ([]*model.ColumnInfo, []*types.FieldType, error) {
	if tbl.PKIsHandle {
		for i, col := range tbl.Columns {
			if mysql.HasPriKeyFlag(col.GetFlag()) {
				return []*model.ColumnInfo{tbl.Columns[i]}, []*types.FieldType{&tbl.Columns[i].FieldType}, nil
			}
		}
		return nil, nil, errors.Errorf("Cannot find primary key for table: %s", tbl.Name)
	}

	if tbl.IsCommonHandle {
		idxInfo := tables.FindPrimaryIndex(tbl)
		columns := make([]*model.ColumnInfo, len(idxInfo.Columns))
		fieldTypes := make([]*types.FieldType, len(idxInfo.Columns))
		for i, idxCol := range idxInfo.Columns {
			columns[i] = tbl.Columns[idxCol.Offset]
			fieldTypes[i] = &tbl.Columns[idxCol.Offset].FieldType
		}
		return columns, fieldTypes, nil
	}

	extraHandleColInfo := model.NewExtraHandleColInfo()
	return []*model.ColumnInfo{extraHandleColInfo}, []*types.FieldType{&extraHandleColInfo.FieldType}, nil
}

// ScanRange is the range to scan. The range is: [Start, End)
type ScanRange struct {
	Start []types.Datum
	End   []types.Datum
}

func newDatumRange(start types.Datum, end types.Datum) (r ScanRange) {
	if !start.IsNull() {
		r.Start = []types.Datum{start}
	}
	if !end.IsNull() {
		r.End = []types.Datum{end}
	}
	return r
}

func newInt64Range(start int64, end int64) (r ScanRange) {
	if start != math.MinInt64 {
		r.Start = []types.Datum{types.NewIntDatum(start)}
	}
	if end != math.MaxInt64 {
		r.End = []types.Datum{types.NewIntDatum(end)}
	}
	return
}

func newUint64Range(start uint64, end uint64) (r ScanRange) {
	if start != 0 {
		r.Start = []types.Datum{types.NewUintDatum(start)}
	}
	if end != math.MaxUint64 {
		r.End = []types.Datum{types.NewUintDatum(end)}
	}
	return
}

// PhysicalTable is used to provide some information for a physical table in TTL job
type PhysicalTable struct {
	// ID is the physical ID of the table
	ID int64
	// Schema is the database name of the table
	Schema model.CIStr
	*model.TableInfo
	// Partition is the partition name
	Partition model.CIStr
	// PartitionDef is the partition definition
	PartitionDef *model.PartitionDefinition
	// KeyColumns is the cluster index key columns for the table
	KeyColumns []*model.ColumnInfo
	// KeyColumnTypes is the types of the key columns
	KeyColumnTypes []*types.FieldType
	// TimeColum is the time column used for TTL
	TimeColumn *model.ColumnInfo
}

// NewPhysicalTable create a new PhysicalTable
func NewPhysicalTable(schema model.CIStr, tbl *model.TableInfo, partition model.CIStr) (*PhysicalTable, error) {
	if tbl.State != model.StatePublic {
		return nil, errors.Errorf("table '%s.%s' is not a public table", schema, tbl.Name)
	}

	ttlInfo := tbl.TTLInfo
	if ttlInfo == nil {
		return nil, errors.Errorf("table '%s.%s' is not a ttl table", schema, tbl.Name)
	}

	timeColumn := tbl.FindPublicColumnByName(ttlInfo.ColumnName.L)
	if timeColumn == nil {
		return nil, errors.Errorf("time column '%s' is not public in ttl table '%s.%s'", ttlInfo.ColumnName, schema, tbl.Name)
	}

	keyColumns, keyColumTypes, err := getTableKeyColumns(tbl)
	if err != nil {
		return nil, err
	}

	var physicalID int64
	var partitionDef *model.PartitionDefinition
	if tbl.Partition == nil {
		if partition.L != "" {
			return nil, errors.Errorf("table '%s.%s' is not a partitioned table", schema, tbl.Name)
		}
		physicalID = tbl.ID
	} else {
		if partition.L == "" {
			return nil, errors.Errorf("partition name is required, table '%s.%s' is a partitioned table", schema, tbl.Name)
		}

		for i := range tbl.Partition.Definitions {
			def := &tbl.Partition.Definitions[i]
			if def.Name.L == partition.L {
				partitionDef = def
			}
		}

		if partitionDef == nil {
			return nil, errors.Errorf("partition '%s' is not found in ttl table '%s.%s'", partition.O, schema, tbl.Name)
		}

		physicalID = partitionDef.ID
	}

	return &PhysicalTable{
		ID:             physicalID,
		Schema:         schema,
		TableInfo:      tbl,
		Partition:      partition,
		PartitionDef:   partitionDef,
		KeyColumns:     keyColumns,
		KeyColumnTypes: keyColumTypes,
		TimeColumn:     timeColumn,
	}, nil
}

// ValidateKey validates a key
func (t *PhysicalTable) ValidateKey(key []types.Datum) error {
	if len(t.KeyColumns) != len(key) {
		return errors.Errorf("invalid key length: %d, expected %d", len(key), len(t.KeyColumns))
	}
	return nil
}

// EvalExpireTime returns the expired time
func (t *PhysicalTable) EvalExpireTime(ctx context.Context, se session.Session, now time.Time) (expire time.Time, err error) {
	tz := se.GetSessionVars().TimeZone

	expireExpr := t.TTLInfo.IntervalExprStr
	unit := ast.TimeUnitType(t.TTLInfo.IntervalTimeUnit)

	var rows []chunk.Row
	rows, err = se.ExecuteSQL(
		ctx,
		// FROM_UNIXTIME does not support negative value, so we use `FROM_UNIXTIME(0) + INTERVAL <current_ts>` to present current time
		fmt.Sprintf("SELECT FROM_UNIXTIME(0) + INTERVAL %d SECOND - INTERVAL %s %s", now.Unix(), expireExpr, unit.String()),
	)

	if err != nil {
		return
	}

	tm := rows[0].GetTime(0)
	return tm.CoreTime().GoTime(tz)
}

func (t *PhysicalTable) SplitScanRanges(ctx context.Context, store kv.Storage, fullRange ScanRange, maxSplit int) ([]ScanRange, error) {
	if len(t.KeyColumns) != 1 || maxSplit <= 1 {
		return []ScanRange{fullRange}, nil
	}

	tikvStore, ok := store.(tikv.Storage)
	if !ok {
		return []ScanRange{fullRange}, nil
	}

	ft := t.KeyColumns[0].FieldType
	switch ft.GetType() {
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeLong, mysql.TypeLonglong, mysql.TypeInt24:
		return t.splitIntRanges(ctx, tikvStore, fullRange, maxSplit)
	case mysql.TypeString, mysql.TypeVarString, mysql.TypeVarchar, mysql.TypeBit:
		if mysql.HasBinaryFlag(ft.GetFlag()) {
			return t.splitBinaryRanges(ctx, tikvStore, fullRange, maxSplit)
		}
	}
	return []ScanRange{fullRange}, nil
}

func (t *PhysicalTable) splitIntRanges(ctx context.Context, store tikv.Storage, fullRange ScanRange, maxSplit int) ([]ScanRange, error) {
	startHandle := kv.IntHandle(math.MinInt64)
	endHandle := kv.IntHandle(math.MaxInt64)
	if len(fullRange.Start) > 0 {
		startHandle = kv.IntHandle(fullRange.Start[0].GetInt64())
	}
	if len(fullRange.End) > 0 {
		endHandle = kv.IntHandle(fullRange.End[0].GetInt64())
	}

	startKey := tablecodec.EncodeRowKeyWithHandle(t.ID, startHandle)
	endKey := tablecodec.EncodeRowKeyWithHandle(t.ID, endHandle)
	keyRanges, err := t.splitRawKeyRanges(ctx, store, startKey, endKey, maxSplit)
	if err != nil {
		return nil, err
	}

	ft := t.KeyColumnTypes[0]
	scanRanges := make([]ScanRange, 0, len(keyRanges)+1)
	for _, keyRange := range keyRanges {
		rStart, err := tablecodec.DecodeRowKey(keyRange.StartKey)
		if err != nil {
			return nil, err
		}

		rEnd, err := tablecodec.DecodeRowKey(keyRange.EndKey)
		if err != nil {
			return nil, err
		}

		startVal, endVal := rStart.IntValue(), rEnd.IntValue()
		if mysql.HasUnsignedFlag(ft.GetFlag()) {
			scanRanges = append(scanRanges, newInt64Range(startVal, endVal))
		} else if startVal < 0 && endVal > 0 {
			scanRanges = append(scanRanges,
				newUint64Range(0, uint64(endVal)),
				newUint64Range(uint64(startVal), math.MaxUint64),
			)
		} else {
			scanRanges = append(scanRanges, newUint64Range(uint64(startVal), uint64(endVal)))
		}
	}
	return scanRanges, nil
}

func (t *PhysicalTable) splitBinaryRanges(ctx context.Context, store tikv.Storage, fullRange ScanRange, maxSplit int) ([]ScanRange, error) {
	recordPrefix := tablecodec.GenTableRecordPrefix(t.ID)
	startKey := recordPrefix
	if len(fullRange.Start) > 0 {
		startKey = tablecodec.EncodeRowKey(t.ID, fullRange.Start[0].GetBytes())
	}

	endKey := startKey.PrefixNext()
	if len(fullRange.End) > 0 {
		endKey = tablecodec.EncodeRowKey(t.ID, fullRange.End[0].GetBytes())
	}

	keyRanges, err := t.splitRawKeyRanges(ctx, store, startKey, endKey, maxSplit)
	if err != nil {
		return nil, err
	}

	ft := t.KeyColumnTypes[0]
	scanRanges := make([]ScanRange, 0, len(keyRanges))
	chk := chunk.New(t.KeyColumnTypes, 2, 2)
	for _, keyRange := range keyRanges {
		chk.Reset()
		if keyRange.StartKey.Cmp(recordPrefix) > 0 {
			chk.AppendBytes(0, keyRange.StartKey[len(recordPrefix):])
		} else {
			chk.AppendNull(0)
		}

		if keyRange.EndKey.HasPrefix(recordPrefix) {
			chk.AppendBytes(0, keyRange.EndKey[len(recordPrefix):])
		} else {
			chk.AppendNull(0)
		}

		scanRanges = append(scanRanges, newDatumRange(
			chk.GetRow(0).GetDatum(0, ft),
			chk.GetRow(1).GetDatum(0, ft),
		))
	}
	return scanRanges, nil
}

func (t *PhysicalTable) splitRawKeyRanges(ctx context.Context, store tikv.Storage, startKey, endKey kv.Key, maxSplit int) ([]kv.KeyRange, error) {
	bo := tikv.NewBackofferWithVars(ctx, 20000, nil)
	regions, err := store.GetRegionCache().LoadRegionsInKeyRange(bo, startKey, endKey)
	if err != nil {
		return nil, err
	}

	regionsPerRange := len(regions) / maxSplit
	oversizeCnt := len(regions) % maxSplit
	ranges := make([]kv.KeyRange, 0, mathutil.Min(len(regions), maxSplit))
	for len(regions) > 0 {
		rangeStartKey := kv.Key(regions[0].StartKey())
		if rangeStartKey.Cmp(startKey) < 0 {
			rangeStartKey = startKey
		}

		endRegionIdx := regionsPerRange
		if oversizeCnt > 0 {
			endRegionIdx++
		}

		rangeEndKey := kv.Key(regions[endRegionIdx].EndKey())
		if rangeEndKey.Cmp(endKey) > 0 {
			rangeEndKey = endKey
		}

		ranges = append(ranges, kv.KeyRange{StartKey: rangeStartKey, EndKey: rangeEndKey})
		oversizeCnt--
		regions = regions[endRegionIdx+1:]
	}
	return ranges, nil
}
