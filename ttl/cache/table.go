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

package cache

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/ttl/session"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/codec"
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

func newFullRange() ScanRange {
	return ScanRange{}
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

func nullDatum() types.Datum {
	d := types.Datum{}
	d.SetNull()
	return d
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

// ValidateKeyPrefix validates a key prefix
func (t *PhysicalTable) ValidateKeyPrefix(key []types.Datum) error {
	if len(key) > len(t.KeyColumns) {
		return errors.Errorf("invalid key length: %d, expected %d", len(key), len(t.KeyColumns))
	}
	return nil
}

// EvalExpireTime returns the expired time
func (t *PhysicalTable) EvalExpireTime(ctx context.Context, se session.Session, now time.Time) (expire time.Time, err error) {
	tz := se.GetSessionVars().Location()

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

// SplitScanRanges split ranges for TTL scan
func (t *PhysicalTable) SplitScanRanges(ctx context.Context, store kv.Storage, splitCnt int) ([]ScanRange, error) {
	if len(t.KeyColumns) < 1 || splitCnt <= 1 {
		return []ScanRange{newFullRange()}, nil
	}

	tikvStore, ok := store.(tikv.Storage)
	if !ok {
		return []ScanRange{newFullRange()}, nil
	}

	ft := t.KeyColumns[0].FieldType
	switch ft.GetType() {
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeLong, mysql.TypeLonglong, mysql.TypeInt24:
		return t.splitIntRanges(ctx, tikvStore, splitCnt)
	case mysql.TypeBit:
		return t.splitBinaryRanges(ctx, tikvStore, splitCnt)
	case mysql.TypeString, mysql.TypeVarString, mysql.TypeVarchar:
		if mysql.HasBinaryFlag(ft.GetFlag()) {
			return t.splitBinaryRanges(ctx, tikvStore, splitCnt)
		}
	}
	return []ScanRange{newFullRange()}, nil
}

func unsignedEdge(d types.Datum) types.Datum {
	if d.IsNull() {
		return types.NewUintDatum(uint64(math.MaxInt64 + 1))
	}
	if d.GetInt64() == 0 {
		return nullDatum()
	}
	return types.NewUintDatum(uint64(d.GetInt64()))
}

func (t *PhysicalTable) splitIntRanges(ctx context.Context, store tikv.Storage, splitCnt int) ([]ScanRange, error) {
	recordPrefix := tablecodec.GenTableRecordPrefix(t.ID)
	startKey, endKey := tablecodec.GetTableHandleKeyRange(t.ID)
	keyRanges, err := t.splitRawKeyRanges(ctx, store, startKey, endKey, splitCnt)
	if err != nil {
		return nil, err
	}

	if len(keyRanges) <= 1 {
		return []ScanRange{newFullRange()}, nil
	}

	ft := t.KeyColumnTypes[0]
	unsigned := mysql.HasUnsignedFlag(ft.GetFlag())
	scanRanges := make([]ScanRange, 0, len(keyRanges)+1)
	curScanStart := nullDatum()
	for i, keyRange := range keyRanges {
		if i != 0 && curScanStart.IsNull() {
			break
		}

		curScanEnd := nullDatum()
		if i < len(keyRanges)-1 {
			if val := GetNextIntHandle(keyRange.EndKey, recordPrefix); val != nil {
				curScanEnd = types.NewIntDatum(val.IntValue())
			}
		}

		if !curScanStart.IsNull() && !curScanEnd.IsNull() && curScanStart.GetInt64() >= curScanEnd.GetInt64() {
			continue
		}

		if !unsigned {
			// primary key is signed or range
			scanRanges = append(scanRanges, newDatumRange(curScanStart, curScanEnd))
		} else if !curScanStart.IsNull() && curScanStart.GetInt64() >= 0 {
			// primary key is unsigned and range is in the right half side
			scanRanges = append(scanRanges, newDatumRange(unsignedEdge(curScanStart), unsignedEdge(curScanEnd)))
		} else if !curScanEnd.IsNull() && curScanEnd.GetInt64() <= 0 {
			// primary key is unsigned and range is in the left half side
			scanRanges = append(scanRanges, newDatumRange(unsignedEdge(curScanStart), unsignedEdge(curScanEnd)))
		} else {
			// primary key is unsigned and the start > math.MaxInt64 && end < math.MaxInt64
			// we must split it to two ranges
			scanRanges = append(scanRanges,
				newDatumRange(unsignedEdge(curScanStart), nullDatum()),
				newDatumRange(nullDatum(), unsignedEdge(curScanEnd)),
			)
		}
		curScanStart = curScanEnd
	}
	return scanRanges, nil
}

func (t *PhysicalTable) splitBinaryRanges(ctx context.Context, store tikv.Storage, splitCnt int) ([]ScanRange, error) {
	recordPrefix := tablecodec.GenTableRecordPrefix(t.ID)
	startKey, endKey := recordPrefix, recordPrefix.PrefixNext()
	keyRanges, err := t.splitRawKeyRanges(ctx, store, startKey, endKey, splitCnt)
	if err != nil {
		return nil, err
	}

	if len(keyRanges) <= 1 {
		return []ScanRange{newFullRange()}, nil
	}

	scanRanges := make([]ScanRange, 0, len(keyRanges))
	curScanStart := nullDatum()
	for i, keyRange := range keyRanges {
		if i != 0 && curScanStart.IsNull() {
			break
		}

		curScanEnd := nullDatum()
		if i != len(keyRanges)-1 {
			curScanEnd = GetNextBytesHandleDatum(keyRange.EndKey, recordPrefix)
		}

		if !curScanStart.IsNull() && !curScanEnd.IsNull() && kv.Key(curScanStart.GetBytes()).Cmp(curScanEnd.GetBytes()) >= 0 {
			continue
		}

		scanRanges = append(scanRanges, newDatumRange(curScanStart, curScanEnd))
		curScanStart = curScanEnd
	}
	return scanRanges, nil
}

func (t *PhysicalTable) splitRawKeyRanges(ctx context.Context, store tikv.Storage, startKey, endKey kv.Key, splitCnt int) ([]kv.KeyRange, error) {
	regionCache := store.GetRegionCache()
	regionIDs, err := regionCache.ListRegionIDsInKeyRange(tikv.NewBackofferWithVars(ctx, 20000, nil), startKey, endKey)
	if err != nil {
		return nil, err
	}

	regionsPerRange := len(regionIDs) / splitCnt
	oversizeCnt := len(regionIDs) % splitCnt
	ranges := make([]kv.KeyRange, 0, mathutil.Min(len(regionIDs), splitCnt))
	for len(regionIDs) > 0 {
		startRegion, err := regionCache.LocateRegionByID(tikv.NewBackofferWithVars(ctx, 20000, nil), regionIDs[0])
		if err != nil {
			return nil, err
		}

		endRegionIdx := regionsPerRange - 1
		if oversizeCnt > 0 {
			endRegionIdx++
		}

		endRegion, err := regionCache.LocateRegionByID(tikv.NewBackofferWithVars(ctx, 20000, nil), regionIDs[endRegionIdx])
		if err != nil {
			return nil, err
		}

		rangeStartKey := kv.Key(startRegion.StartKey)
		if rangeStartKey.Cmp(startKey) < 0 {
			rangeStartKey = startKey
		}

		rangeEndKey := kv.Key(endRegion.EndKey)
		if rangeEndKey.Cmp(endKey) > 0 {
			rangeEndKey = endKey
		}

		ranges = append(ranges, kv.KeyRange{StartKey: rangeStartKey, EndKey: rangeEndKey})
		oversizeCnt--
		regionIDs = regionIDs[endRegionIdx+1:]
	}
	return ranges, nil
}

var emptyBytesHandleKey kv.Key

func init() {
	key, err := codec.EncodeKey(nil, nil, types.NewBytesDatum(nil))
	terror.MustNil(err)
	emptyBytesHandleKey = key
}

// GetNextIntHandle is used for int handle tables. It returns the min handle whose encoded key is or after argument `key`
// If it cannot find a valid value, a null datum will be returned.
func GetNextIntHandle(key kv.Key, recordPrefix []byte) kv.Handle {
	if key.Cmp(recordPrefix) > 0 && !key.HasPrefix(recordPrefix) {
		return nil
	}

	if key.Cmp(recordPrefix) <= 0 {
		return kv.IntHandle(math.MinInt64)
	}

	suffix := key[len(recordPrefix):]
	encodedVal := suffix
	if len(suffix) < 8 {
		encodedVal = make([]byte, 8)
		copy(encodedVal, suffix)
	}

	findNext := false
	if len(suffix) > 8 {
		findNext = true
		encodedVal = encodedVal[:8]
	}

	u := codec.DecodeCmpUintToInt(binary.BigEndian.Uint64(encodedVal))
	if !findNext {
		return kv.IntHandle(u)
	}

	if u == math.MaxInt64 {
		return nil
	}

	return kv.IntHandle(u + 1)
}

// GetNextBytesHandleDatum is used for a table with one binary or string column common handle.
// It returns the minValue whose encoded key is or after argument `key`
// If it cannot find a valid value, a null datum will be returned.
func GetNextBytesHandleDatum(key kv.Key, recordPrefix []byte) (d types.Datum) {
	if key.Cmp(recordPrefix) > 0 && !key.HasPrefix(recordPrefix) {
		d.SetNull()
		return d
	}

	if key.Cmp(recordPrefix) <= 0 {
		d.SetBytes([]byte{})
		return d
	}

	encodedVal := key[len(recordPrefix):]
	if encodedVal[0] < emptyBytesHandleKey[0] {
		d.SetBytes([]byte{})
		return d
	}

	if encodedVal[0] > emptyBytesHandleKey[0] {
		d.SetNull()
		return d
	}

	if remain, v, err := codec.DecodeOne(encodedVal); err == nil {
		if len(remain) > 0 {
			v.SetBytes(kv.Key(v.GetBytes()).Next())
		}
		return v
	}

	encodedVal = encodedVal[1:]
	brokenGroupEndIdx := len(encodedVal) - 1
	brokenGroupEmptyBytes := len(encodedVal) % 9
	for i := 7; i+1 < len(encodedVal); i += 9 {
		if emptyBytes := 255 - int(encodedVal[i+1]); emptyBytes != 0 || i+1 == len(encodedVal)-1 {
			brokenGroupEndIdx = i
			brokenGroupEmptyBytes = emptyBytes
			break
		}
	}

	for i := 0; i < brokenGroupEmptyBytes; i++ {
		if encodedVal[brokenGroupEndIdx] > 0 {
			break
		}
		brokenGroupEndIdx--
	}

	if brokenGroupEndIdx < 0 {
		d.SetBytes(nil)
		return d
	}

	val := make([]byte, 0, len(encodedVal))
	for i := 0; i <= brokenGroupEndIdx; i++ {
		if i%9 == 8 {
			continue
		}
		val = append(val, encodedVal[i])
	}
	d.SetBytes(val)
	return d
}
