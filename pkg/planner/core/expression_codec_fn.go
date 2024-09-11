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

package core

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/infoschema"
	infoschemactx "github.com/pingcap/tidb/pkg/infoschema/context"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
)

// tidbCodecFuncHelper contains some utililty functions for
//   - tidb_decode_key(hex_string)
//   - tidb_encode_record_key(database_name, table_name, handle/pk columns...)
//   - tidb_encode_index_key(database_name, table_name, index_name, index columns..., handle/pk columns...)
//
// define an individual struct instead of a bunch of un-exported functions
// to avoid polluting the global scope of current package.
type tidbCodecFuncHelper struct{}

func (h tidbCodecFuncHelper) encodeHandleFromRow(
	ctx expression.EvalContext,
	isVer infoschemactx.MetaOnlyInfoSchema,
	args []expression.Expression,
	row chunk.Row,
) ([]byte, bool, error) {
	dbName, isNull, err := args[0].EvalString(ctx, row)
	if err != nil || isNull {
		return nil, isNull, err
	}
	tblName, isNull, err := args[1].EvalString(ctx, row)
	if err != nil || isNull {
		return nil, isNull, err
	}
	is := isVer.(infoschema.InfoSchema)
	tbl, _, err := h.findCommonOrPartitionedTable(ctx, is, dbName, tblName)
	if err != nil {
		return nil, false, err
	}
	recordID, err := h.buildHandle(ctx, tbl.Meta(), args[2:], row)
	if err != nil {
		return nil, false, err
	}
	key := tablecodec.EncodeRecordKey(tbl.RecordPrefix(), recordID)
	return key, false, nil
}

func (h tidbCodecFuncHelper) findCommonOrPartitionedTable(
	ctx expression.EvalContext,
	is infoschema.InfoSchema,
	dbName string,
	tblName string,
) (table.Table, int64, error) {
	tblName, partName := h.extractTablePartition(tblName)
	tbl, err := is.TableByName(context.Background(), pmodel.NewCIStr(dbName), pmodel.NewCIStr(tblName))
	if err != nil {
		return nil, 0, err
	}
	if !ctx.RequestVerification(dbName, tblName, "", mysql.AllPrivMask) {
		// The arguments will be filled by caller.
		return nil, 0, plannererrors.ErrSpecificAccessDenied
	}
	if len(partName) > 0 {
		if part := tbl.GetPartitionedTable(); part != nil {
			pid, err := tables.FindPartitionByName(tbl.Meta(), partName)
			if err != nil {
				return nil, 0, errors.Trace(err)
			}
			tbl = part.GetPartition(pid)
			return tbl, pid, nil
		}
		return nil, 0, errors.New("not a partitioned table")
	}
	return tbl, tbl.Meta().ID, nil
}

func (tidbCodecFuncHelper) extractTablePartition(str string) (table, partition string) {
	start := strings.IndexByte(str, '(')
	if start == -1 {
		return str, ""
	}
	end := strings.IndexByte(str, ')')
	if end == -1 {
		return str, ""
	}
	return str[:start], str[start+1 : end]
}

func (tidbCodecFuncHelper) buildHandle(
	ctx expression.EvalContext,
	tblInfo *model.TableInfo,
	pkArgs []expression.Expression,
	row chunk.Row,
) (kv.Handle, error) {
	var recordID kv.Handle
	if !tblInfo.IsCommonHandle {
		h, _, err := pkArgs[0].EvalInt(ctx, row)
		if err != nil {
			return nil, err
		}
		recordID = kv.IntHandle(h)
	} else {
		pkIdx := tables.FindPrimaryIndex(tblInfo)
		if len(pkIdx.Columns) != len(pkArgs) {
			return nil, errors.Errorf("pk column count mismatch, expected %d, got %d", len(pkIdx.Columns), pkArgs)
		}
		pkDts := make([]types.Datum, 0, len(pkIdx.Columns))
		for i, idxCol := range pkIdx.Columns {
			dt, err := pkArgs[i].Eval(ctx, row)
			if err != nil {
				return nil, err
			}
			ft := tblInfo.Columns[idxCol.Offset].FieldType
			pkDt, err := dt.ConvertTo(ctx.TypeCtx(), &ft)
			if err != nil {
				return nil, err
			}
			pkDts = append(pkDts, pkDt)
		}
		tablecodec.TruncateIndexValues(tblInfo, pkIdx, pkDts)
		var handleBytes []byte
		handleBytes, err := codec.EncodeKey(ctx.Location(), nil, pkDts...)
		ec := ctx.ErrCtx()
		err = ec.HandleError(err)
		if err != nil {
			return nil, err
		}
		recordID, err = kv.NewCommonHandle(handleBytes)
		if err != nil {
			return nil, err
		}
	}
	return recordID, nil
}

func (h tidbCodecFuncHelper) encodeIndexKeyFromRow(
	ctx expression.EvalContext,
	isVer infoschemactx.MetaOnlyInfoSchema,
	args []expression.Expression,
	row chunk.Row,
) ([]byte, bool, error) {
	dbName, isNull, err := args[0].EvalString(ctx, row)
	if err != nil || isNull {
		return nil, isNull, err
	}
	tblName, isNull, err := args[1].EvalString(ctx, row)
	if err != nil || isNull {
		return nil, isNull, err
	}
	idxName, isNull, err := args[2].EvalString(ctx, row)
	if err != nil || isNull {
		return nil, isNull, err
	}
	is := isVer.(infoschema.InfoSchema)
	tbl, physicalID, err := h.findCommonOrPartitionedTable(ctx, is, dbName, tblName)
	if err != nil {
		return nil, false, err
	}
	tblInfo := tbl.Meta()
	idxInfo := tblInfo.FindIndexByName(strings.ToLower(idxName))
	if idxInfo == nil {
		return nil, false, errors.New("index not found")
	}

	pkLen := 1
	var pkIdx *model.IndexInfo
	if tblInfo.IsCommonHandle {
		pkIdx = tables.FindPrimaryIndex(tblInfo)
		pkLen = len(pkIdx.Columns)
	}

	if len(idxInfo.Columns)+pkLen != len(args)-3 {
		return nil, false, errors.Errorf(
			"column count mismatch, expected %d (index length + pk/rowid length), got %d",
			len(idxInfo.Columns)+pkLen, len(args)-3)
	}

	handle, err := h.buildHandle(ctx, tblInfo, args[3+len(idxInfo.Columns):], row)
	if err != nil {
		return nil, false, err
	}

	idxDts := make([]types.Datum, 0, len(idxInfo.Columns))
	for i, idxCol := range idxInfo.Columns {
		dt, err := args[i+3].Eval(ctx, row)
		if err != nil {
			return nil, false, err
		}
		ft := tblInfo.Columns[idxCol.Offset].FieldType
		idxDt, err := dt.ConvertTo(ctx.TypeCtx(), &ft)
		if err != nil {
			return nil, false, err
		}
		idxDts = append(idxDts, idxDt)
	}
	tablecodec.TruncateIndexValues(tblInfo, idxInfo, idxDts)
	// Use physicalID instead of tblInfo.ID here to handle the partition case.
	idx := tables.NewIndex(physicalID, tblInfo, idxInfo)

	idxKey, _, err := idx.GenIndexKey(ctx.ErrCtx(), ctx.Location(), idxDts, handle, nil)
	return idxKey, false, err
}

func (h tidbCodecFuncHelper) decodeKeyFromString(
	tc types.Context, isVer infoschemactx.MetaOnlyInfoSchema, s string) string {
	key, err := hex.DecodeString(s)
	if err != nil {
		tc.AppendWarning(errors.NewNoStackErrorf("invalid key: %X", key))
		return s
	}
	// Auto decode byte if needed.
	_, bs, err := codec.DecodeBytes(key, nil)
	if err == nil {
		key = bs
	}
	tableID := tablecodec.DecodeTableID(key)
	if tableID <= 0 {
		tc.AppendWarning(errors.NewNoStackErrorf("invalid key: %X", key))
		return s
	}

	is, ok := isVer.(infoschema.InfoSchema)
	if !ok {
		tc.AppendWarning(errors.NewNoStackErrorf("infoschema not found when decoding key: %X", key))
		return s
	}
	tbl, _ := infoschema.FindTableByTblOrPartID(is, tableID)
	loc := tc.Location()
	if tablecodec.IsRecordKey(key) {
		ret, err := h.decodeRecordKey(key, tableID, tbl, loc)
		if err != nil {
			tc.AppendWarning(err)
			return s
		}
		return ret
	} else if tablecodec.IsIndexKey(key) {
		ret, err := h.decodeIndexKey(key, tableID, tbl, loc)
		if err != nil {
			tc.AppendWarning(err)
			return s
		}
		return ret
	} else if tablecodec.IsTableKey(key) {
		ret, err := h.decodeTableKey(key, tableID, tbl)
		if err != nil {
			tc.AppendWarning(err)
			return s
		}
		return ret
	}
	tc.AppendWarning(errors.NewNoStackErrorf("invalid key: %X", key))
	return s
}

func (h tidbCodecFuncHelper) decodeRecordKey(
	key []byte, tableID int64, tbl table.Table, loc *time.Location) (string, error) {
	_, handle, err := tablecodec.DecodeRecordKey(key)
	if err != nil {
		return "", errors.Trace(err)
	}
	if handle.IsInt() {
		ret := make(map[string]any)
		if tbl != nil && tbl.Meta().Partition != nil {
			ret["partition_id"] = tableID
			tableID = tbl.Meta().ID
		}
		ret["table_id"] = strconv.FormatInt(tableID, 10)
		// When the clustered index is enabled, we should show the PK name.
		if tbl != nil && tbl.Meta().HasClusteredIndex() {
			ret[tbl.Meta().GetPkName().String()] = handle.IntValue()
		} else {
			ret["_tidb_rowid"] = handle.IntValue()
		}
		retStr, err := json.Marshal(ret)
		if err != nil {
			return "", errors.Trace(err)
		}
		return string(retStr), nil
	}
	if tbl != nil {
		tblInfo := tbl.Meta()
		idxInfo := tables.FindPrimaryIndex(tblInfo)
		if idxInfo == nil {
			return "", errors.Trace(errors.Errorf("primary key not found when decoding record key: %X", key))
		}
		cols := make(map[int64]*types.FieldType, len(tblInfo.Columns))
		for _, col := range tblInfo.Columns {
			cols[col.ID] = &(col.FieldType)
		}
		handleColIDs := make([]int64, 0, len(idxInfo.Columns))
		for _, col := range idxInfo.Columns {
			handleColIDs = append(handleColIDs, tblInfo.Columns[col.Offset].ID)
		}

		if len(handleColIDs) != handle.NumCols() {
			return "", errors.Trace(errors.Errorf("primary key length not match handle columns number in key"))
		}
		datumMap, err := tablecodec.DecodeHandleToDatumMap(handle, handleColIDs, cols, loc, nil)
		if err != nil {
			return "", errors.Trace(err)
		}
		ret := make(map[string]any)
		if tbl.Meta().Partition != nil {
			ret["partition_id"] = tableID
			tableID = tbl.Meta().ID
		}
		ret["table_id"] = tableID
		handleRet := make(map[string]any)
		for colID := range datumMap {
			dt := datumMap[colID]
			dtStr, err := h.datumToJSONObject(&dt)
			if err != nil {
				return "", errors.Trace(err)
			}
			found := false
			for _, colInfo := range tblInfo.Columns {
				if colInfo.ID == colID {
					found = true
					handleRet[colInfo.Name.L] = dtStr
					break
				}
			}
			if !found {
				return "", errors.Trace(errors.Errorf("column not found when decoding record key: %X", key))
			}
		}
		ret["handle"] = handleRet
		retStr, err := json.Marshal(ret)
		if err != nil {
			return "", errors.Trace(err)
		}
		return string(retStr), nil
	}
	ret := make(map[string]any)
	ret["table_id"] = tableID
	ret["handle"] = handle.String()
	retStr, err := json.Marshal(ret)
	if err != nil {
		return "", errors.Trace(err)
	}
	return string(retStr), nil
}

func (h tidbCodecFuncHelper) decodeIndexKey(
	key []byte, tableID int64, tbl table.Table, loc *time.Location) (string, error) {
	if tbl != nil {
		_, indexID, _, err := tablecodec.DecodeKeyHead(key)
		if err != nil {
			return "", errors.Trace(errors.Errorf("invalid record/index key: %X", key))
		}
		tblInfo := tbl.Meta()
		var targetIndex *model.IndexInfo
		for _, idx := range tblInfo.Indices {
			if idx.ID == indexID {
				targetIndex = idx
				break
			}
		}
		if targetIndex == nil {
			return "", errors.Trace(errors.Errorf("index not found when decoding index key: %X", key))
		}
		colInfos := tables.BuildRowcodecColInfoForIndexColumns(targetIndex, tblInfo)
		tps := tables.BuildFieldTypesForIndexColumns(targetIndex, tblInfo)
		values, err := tablecodec.DecodeIndexKV(key, []byte{0}, len(colInfos), tablecodec.HandleNotNeeded, colInfos)
		if err != nil {
			return "", errors.Trace(err)
		}
		ds := make([]types.Datum, 0, len(colInfos))
		for i := 0; i < len(colInfos); i++ {
			d, err := tablecodec.DecodeColumnValue(values[i], tps[i], loc)
			if err != nil {
				return "", errors.Trace(err)
			}
			ds = append(ds, d)
		}
		ret := make(map[string]any)
		if tbl.Meta().Partition != nil {
			ret["partition_id"] = tableID
			tableID = tbl.Meta().ID
		}
		ret["table_id"] = tableID
		ret["index_id"] = indexID
		idxValMap := make(map[string]any, len(targetIndex.Columns))
		for i := 0; i < len(targetIndex.Columns); i++ {
			dtStr, err := h.datumToJSONObject(&ds[i])
			if err != nil {
				return "", errors.Trace(err)
			}
			idxValMap[targetIndex.Columns[i].Name.L] = dtStr
		}
		ret["index_vals"] = idxValMap
		retStr, err := json.Marshal(ret)
		if err != nil {
			return "", errors.Trace(err)
		}
		return string(retStr), nil
	}
	_, indexID, indexValues, err := tablecodec.DecodeIndexKey(key)
	if err != nil {
		return "", errors.Trace(errors.Errorf("invalid index key: %X", key))
	}
	ret := make(map[string]any)
	ret["table_id"] = tableID
	ret["index_id"] = indexID
	ret["index_vals"] = strings.Join(indexValues, ", ")
	retStr, err := json.Marshal(ret)
	if err != nil {
		return "", errors.Trace(err)
	}
	return string(retStr), nil
}

func (tidbCodecFuncHelper) decodeTableKey(_ []byte, tableID int64, tbl table.Table) (string, error) {
	ret := map[string]int64{}
	if tbl != nil && tbl.Meta().GetPartitionInfo() != nil {
		ret["partition_id"] = tableID
		tableID = tbl.Meta().ID
	}
	ret["table_id"] = tableID
	retStr, err := json.Marshal(ret)
	if err != nil {
		return "", errors.Trace(err)
	}
	return string(retStr), nil
}

func (tidbCodecFuncHelper) datumToJSONObject(d *types.Datum) (any, error) {
	if d.IsNull() {
		return nil, nil
	}
	return d.ToString()
}
