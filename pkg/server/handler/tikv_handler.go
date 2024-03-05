// Copyright 2023 PingCAP, Inc.
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

package handler

import (
	"context"
	"encoding/hex"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	derr "github.com/pingcap/tidb/pkg/store/driver/error"
	"github.com/pingcap/tidb/pkg/store/helper"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/tikv/client-go/v2/tikv"
)

// TikvHandlerTool is a tool to handle TiKV data.
type TikvHandlerTool struct {
	helper.Helper
}

// NewTikvHandlerTool creates a new TikvHandlerTool.
func NewTikvHandlerTool(store helper.Storage) *TikvHandlerTool {
	return &TikvHandlerTool{Helper: *helper.NewHelper(store)}
}

type mvccKV struct {
	Key      string                        `json:"key"`
	RegionID uint64                        `json:"region_id"`
	Value    *kvrpcpb.MvccGetByKeyResponse `json:"value"`
}

// GetRegionIDByKey gets the region id by the key.
func (t *TikvHandlerTool) GetRegionIDByKey(encodedKey []byte) (uint64, error) {
	keyLocation, err := t.RegionCache.LocateKey(tikv.NewBackofferWithVars(context.Background(), 500, nil), encodedKey)
	if err != nil {
		return 0, derr.ToTiDBErr(err)
	}
	return keyLocation.Region.GetID(), nil
}

// GetHandle gets the handle of the record.
func (t *TikvHandlerTool) GetHandle(tb table.PhysicalTable, params map[string]string, values url.Values) (kv.Handle, error) {
	var handle kv.Handle
	if intHandleStr, ok := params[Handle]; ok {
		if tb.Meta().IsCommonHandle {
			return nil, errors.BadRequestf("For clustered index tables, please use query strings to specify the column values.")
		}
		intHandle, err := strconv.ParseInt(intHandleStr, 0, 64)
		if err != nil {
			return nil, errors.Trace(err)
		}
		handle = kv.IntHandle(intHandle)
	} else {
		tblInfo := tb.Meta()
		pkIdx := tables.FindPrimaryIndex(tblInfo)
		if pkIdx == nil || !tblInfo.IsCommonHandle {
			return nil, errors.BadRequestf("Clustered common handle not found.")
		}
		cols := tblInfo.Cols()
		pkCols := make([]*model.ColumnInfo, 0, len(pkIdx.Columns))
		for _, idxCol := range pkIdx.Columns {
			pkCols = append(pkCols, cols[idxCol.Offset])
		}
		sc := stmtctx.NewStmtCtx()
		sc.SetTimeZone(time.UTC)
		pkDts, err := t.formValue2DatumRow(sc, values, pkCols)
		if err != nil {
			return nil, errors.Trace(err)
		}
		tablecodec.TruncateIndexValues(tblInfo, pkIdx, pkDts)
		var handleBytes []byte
		handleBytes, err = codec.EncodeKey(sc.TimeZone(), nil, pkDts...)
		err = sc.HandleError(err)
		if err != nil {
			return nil, errors.Trace(err)
		}
		handle, err = kv.NewCommonHandle(handleBytes)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	return handle, nil
}

// GetMvccByIdxValue gets the mvcc by the index value.
func (t *TikvHandlerTool) GetMvccByIdxValue(idx table.Index, values url.Values, idxCols []*model.ColumnInfo, handle kv.Handle) ([]*helper.MvccKV, error) {
	// HTTP request is not a database session, set timezone to UTC directly here.
	// See https://github.com/pingcap/tidb/blob/master/docs/tidb_http_api.md for more details.
	sc := stmtctx.NewStmtCtxWithTimeZone(time.UTC)
	idxRow, err := t.formValue2DatumRow(sc, values, idxCols)
	if err != nil {
		return nil, errors.Trace(err)
	}
	encodedKey, _, err := idx.GenIndexKey(sc.ErrCtx(), sc.TimeZone(), idxRow, handle, nil)
	if err != nil {
		return nil, errors.Trace(err)
	}
	data, err := t.GetMvccByEncodedKey(encodedKey)
	if err != nil {
		return nil, err
	}
	regionID, err := t.GetRegionIDByKey(encodedKey)
	if err != nil {
		return nil, err
	}
	idxData := &helper.MvccKV{Key: strings.ToUpper(hex.EncodeToString(encodedKey)), RegionID: regionID, Value: data}
	tablecodec.IndexKey2TempIndexKey(encodedKey)
	data, err = t.GetMvccByEncodedKey(encodedKey)
	if err != nil {
		return nil, err
	}
	regionID, err = t.GetRegionIDByKey(encodedKey)
	if err != nil {
		return nil, err
	}
	tempIdxData := &helper.MvccKV{Key: strings.ToUpper(hex.EncodeToString(encodedKey)), RegionID: regionID, Value: data}
	return append([]*helper.MvccKV{}, idxData, tempIdxData), err
}

// formValue2DatumRow converts URL query string to a Datum Row.
func (*TikvHandlerTool) formValue2DatumRow(sc *stmtctx.StatementContext, values url.Values, idxCols []*model.ColumnInfo) ([]types.Datum, error) {
	data := make([]types.Datum, len(idxCols))
	for i, col := range idxCols {
		colName := col.Name.String()
		vals, ok := values[colName]
		if !ok {
			return nil, errors.BadRequestf("Missing value for index column %s.", colName)
		}

		switch len(vals) {
		case 0:
			data[i].SetNull()
		case 1:
			bDatum := types.NewStringDatum(vals[0])
			cDatum, err := bDatum.ConvertTo(sc.TypeCtx(), &col.FieldType)
			if err != nil {
				return nil, errors.Trace(err)
			}
			data[i] = cDatum
		default:
			return nil, errors.BadRequestf("Invalid query form for column '%s', it's values are %v."+
				" Column value should be unique for one index record.", colName, vals)
		}
	}
	return data, nil
}

// GetTableID gets the table ID by the database name and table name.
func (t *TikvHandlerTool) GetTableID(dbName, tableName string) (int64, error) {
	tbl, err := t.GetTable(dbName, tableName)
	if err != nil {
		return 0, errors.Trace(err)
	}
	return tbl.GetPhysicalID(), nil
}

// GetTable gets the table by the database name and table name.
func (t *TikvHandlerTool) GetTable(dbName, tableName string) (table.PhysicalTable, error) {
	schema, err := t.Schema()
	if err != nil {
		return nil, errors.Trace(err)
	}
	tableName, partitionName := ExtractTableAndPartitionName(tableName)
	tableVal, err := schema.TableByName(model.NewCIStr(dbName), model.NewCIStr(tableName))
	if err != nil {
		return nil, errors.Trace(err)
	}
	return t.GetPartition(tableVal, partitionName)
}

// GetPartition gets the partition by the table and partition name.
func (*TikvHandlerTool) GetPartition(tableVal table.Table, partitionName string) (table.PhysicalTable, error) {
	if pt, ok := tableVal.(table.PartitionedTable); ok {
		if partitionName == "" {
			return tableVal.(table.PhysicalTable), errors.New("work on partitioned table, please specify the table name like this: table(partition)")
		}
		tblInfo := pt.Meta()
		pid, err := tables.FindPartitionByName(tblInfo, partitionName)
		if err != nil {
			return nil, errors.Trace(err)
		}
		return pt.GetPartition(pid), nil
	}
	if partitionName != "" {
		return nil, fmt.Errorf("%s is not a partitionted table", tableVal.Meta().Name)
	}
	return tableVal.(table.PhysicalTable), nil
}

// Schema gets the schema.
func (t *TikvHandlerTool) Schema() (infoschema.InfoSchema, error) {
	dom, err := session.GetDomain(t.Store)
	if err != nil {
		return nil, err
	}
	return dom.InfoSchema(), nil
}

// HandleMvccGetByHex handles the request of getting mvcc by hex encoded key.
func (t *TikvHandlerTool) HandleMvccGetByHex(params map[string]string) (*mvccKV, error) {
	encodedKey, err := hex.DecodeString(params[HexKey])
	if err != nil {
		return nil, errors.Trace(err)
	}
	data, err := t.GetMvccByEncodedKey(encodedKey)
	if err != nil {
		return nil, errors.Trace(err)
	}
	regionID, err := t.GetRegionIDByKey(encodedKey)
	if err != nil {
		return nil, err
	}
	return &mvccKV{Key: strings.ToUpper(params[HexKey]), Value: data, RegionID: regionID}, nil
}

// RegionMeta contains a region's peer detail
type RegionMeta struct {
	ID          uint64              `json:"region_id"`
	Leader      *metapb.Peer        `json:"leader"`
	Peers       []*metapb.Peer      `json:"peers"`
	RegionEpoch *metapb.RegionEpoch `json:"region_epoch"`
}

// GetRegionsMeta gets regions meta by regionIDs
func (t *TikvHandlerTool) GetRegionsMeta(regionIDs []uint64) ([]RegionMeta, error) {
	regions := make([]RegionMeta, len(regionIDs))
	for i, regionID := range regionIDs {
		region, err := t.RegionCache.PDClient().GetRegionByID(context.TODO(), regionID)
		if err != nil {
			return nil, errors.Trace(err)
		}

		failpoint.Inject("errGetRegionByIDEmpty", func(val failpoint.Value) {
			if val.(bool) {
				region.Meta = nil
			}
		})

		if region.Meta == nil {
			return nil, errors.Errorf("region not found for regionID %q", regionID)
		}
		regions[i] = RegionMeta{
			ID:          regionID,
			Leader:      region.Leader,
			Peers:       region.Meta.Peers,
			RegionEpoch: region.Meta.RegionEpoch,
		}
	}
	return regions, nil
}
