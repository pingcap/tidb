// Copyright 2020 PingCAP, Inc.
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

package session

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"reflect"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/rowcodec"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/txnkv/transaction"
	"go.uber.org/zap"
)

const amendableType = nonMemAmendType | memBufAmendType
const nonMemAmendType = (1 << model.ActionAddColumn) | (1 << model.ActionDropColumn) | (1 << model.ActionDropIndex)
const memBufAmendType = uint64(1<<model.ActionAddIndex) | (1 << model.ActionModifyColumn)

// Amend operation types.
const (
	AmendNone int = iota

	// For add index.
	AmendNeedAddDelete
	AmendNeedAddDeleteAndInsert
	AmendNeedAddInsert
)

// ConstOpAddIndex is the possible ddl state changes, and related amend action types.
var ConstOpAddIndex = map[model.SchemaState]map[model.SchemaState]int{
	model.StateNone: {
		model.StateDeleteOnly:          AmendNeedAddDelete,
		model.StateWriteOnly:           AmendNeedAddDeleteAndInsert,
		model.StateWriteReorganization: AmendNeedAddDeleteAndInsert,
		model.StatePublic:              AmendNeedAddDeleteAndInsert,
	},
	model.StateDeleteOnly: {
		model.StateWriteOnly:           AmendNeedAddInsert,
		model.StateWriteReorganization: AmendNeedAddInsert,
		model.StatePublic:              AmendNeedAddInsert,
	},
	model.StateWriteOnly: {
		model.StateWriteReorganization: AmendNone,
		model.StatePublic:              AmendNone,
	},
	model.StateWriteReorganization: {
		model.StatePublic: AmendNone,
	},
}

type schemaAndDecoder struct {
	schema  *expression.Schema
	decoder *rowcodec.ChunkDecoder
}

// amendCollector collects all amend operations.
type amendCollector struct {
	tblAmendOpMap map[int64][]amendOp
}

func newAmendCollector() *amendCollector {
	res := &amendCollector{
		tblAmendOpMap: make(map[int64][]amendOp),
	}
	return res
}

func findIndexByID(tbl table.Table, ID int64) table.Index {
	for _, indexInfo := range tbl.Indices() {
		if indexInfo.Meta().ID == ID {
			return indexInfo
		}
	}
	return nil
}

func findColByID(tbl table.Table, colID int64) *table.Column {
	for _, colInfo := range tbl.Cols() {
		if colInfo.ID == colID {
			return colInfo
		}
	}
	return nil
}

func addIndexNeedRemoveOp(amendOp int) bool {
	if amendOp == AmendNeedAddDelete || amendOp == AmendNeedAddDeleteAndInsert {
		return true
	}
	return false
}

func addIndexNeedAddOp(amendOp int) bool {
	if amendOp == AmendNeedAddDeleteAndInsert || amendOp == AmendNeedAddInsert {
		return true
	}
	return false
}

func (a *amendCollector) keyHasAmendOp(key []byte) bool {
	tblID := tablecodec.DecodeTableID(key)
	ops := a.tblAmendOpMap[tblID]
	return len(ops) > 0
}

func needCollectIndexOps(actionType uint64) bool {
	return actionType&(1<<model.ActionAddIndex) != 0
}

func needCollectModifyColOps(actionType uint64) bool {
	return actionType&(1<<model.ActionModifyColumn) != 0
}

func fieldTypeDeepEquals(ft1 *types.FieldType, ft2 *types.FieldType) bool {
	if ft1.GetType() == ft2.GetType() &&
		ft1.GetFlag() == ft2.GetFlag() &&
		ft1.GetFlen() == ft2.GetFlen() &&
		ft1.GetDecimal() == ft2.GetDecimal() &&
		ft1.GetCharset() == ft2.GetCharset() &&
		ft1.GetCollate() == ft2.GetCollate() &&
		len(ft1.GetElems()) == len(ft2.GetElems()) {
		for i, elem := range ft1.GetElems() {
			if elem != ft2.GetElem(i) {
				return false
			}
		}
		return true
	}
	return false
}

// colChangeAmendable checks whether the column change is amendable, now only increasing column field
// length is allowed for committing concurrent pessimistic transactions.
func colChangeAmendable(colAtStart *model.ColumnInfo, colAtCommit *model.ColumnInfo) error {
	// Modifying a stored generated column is not allowed by DDL, the generated related fields are not considered.
	if !fieldTypeDeepEquals(&colAtStart.FieldType, &colAtCommit.FieldType) {
		if colAtStart.FieldType.GetFlag() != colAtCommit.FieldType.GetFlag() {
			return errors.Trace(errors.Errorf("flag is not matched for column=%v, from=%v to=%v",
				colAtCommit.Name.String(), colAtStart.FieldType.GetFlag(), colAtCommit.FieldType.GetFlag()))
		}
		if colAtStart.GetCharset() != colAtCommit.GetCharset() || colAtStart.GetCollate() != colAtCommit.GetCollate() {
			return errors.Trace(errors.Errorf("charset or collate is not matched for column=%v", colAtCommit.Name.String()))
		}
		_, err := types.CheckModifyTypeCompatible(&colAtStart.FieldType, &colAtCommit.FieldType)
		if err != nil {
			return errors.Trace(err)
		}
	}
	// TODO Default value change is not supported.
	if !reflect.DeepEqual(colAtStart.DefaultValue, colAtCommit.DefaultValue) {
		return errors.Trace(errors.Errorf("default value is not matched for column=%v, from=%v to=%v",
			colAtCommit.Name.String(), colAtStart.DefaultValue, colAtCommit.DefaultValue))
	}
	if !bytes.Equal(colAtStart.DefaultValueBit, colAtCommit.DefaultValueBit) {
		return errors.Trace(errors.Errorf("default value bits is not matched for column=%v, from=%v to=%v",
			colAtCommit.Name.String(), colAtStart.DefaultValueBit, colAtCommit.DefaultValueBit))
	}
	if colAtStart.Version != colAtCommit.Version {
		return errors.Trace(errors.Errorf("column version is not matched for column=%v, from=%v to=%v",
			colAtCommit.Name.String(), colAtStart.Version, colAtCommit.Version))
	}
	return nil
}

// collectModifyColAmendOps is used to check if there is only column size increasing change.Other column type changes
// such as column change from nullable to not null or column type change are not supported by now.
func (a *amendCollector) collectModifyColAmendOps(tblAtStart, tblAtCommit table.Table) ([]amendOp, error) {
	for _, colAtCommit := range tblAtCommit.WritableCols() {
		colAtStart := findColByID(tblAtStart, colAtCommit.ID)
		// It can't find colAtCommit's ID from tblAtStart's public columns when "modify/change column" needs reorg data.
		if colAtStart != nil {
			err := colChangeAmendable(colAtStart.ColumnInfo, colAtCommit.ColumnInfo)
			if err != nil {
				return nil, err
			}
		} else {
			// If the column could not be found in the original schema, it could not be decided if this column
			// is newly added or modified from an original column.Report error to solve the issue
			// https://github.com/pingcap/tidb/issues/21470. This change will make amend fail for adding column
			// and modifying columns at the same time.
			// In addition, amended operations are not currently supported and it goes to this logic when "modify/change column" needs reorg data.
			return nil, errors.Errorf("column=%v id=%v is not found for table=%v checking column modify",
				colAtCommit.Name, colAtCommit.ID, tblAtCommit.Meta().Name.String())
		}
	}
	return nil, nil
}

func (a *amendCollector) collectIndexAmendOps(sctx sessionctx.Context, tblAtStart, tblAtCommit table.Table) ([]amendOp, error) {
	res := make([]amendOp, 0, 4)
	// Check index having state change, collect index column info.
	for _, idxInfoAtCommit := range tblAtCommit.Indices() {
		idxInfoAtStart := findIndexByID(tblAtStart, idxInfoAtCommit.Meta().ID)
		// Try to find index state change.
		var amendOpType int
		if idxInfoAtStart == nil {
			amendOpType = ConstOpAddIndex[model.StateNone][idxInfoAtCommit.Meta().State]
		} else if idxInfoAtCommit.Meta().State > idxInfoAtStart.Meta().State {
			amendOpType = ConstOpAddIndex[idxInfoAtStart.Meta().State][idxInfoAtCommit.Meta().State]
		}
		if amendOpType != AmendNone {
			opInfo := &amendOperationAddIndexInfo{}
			opInfo.AmendOpType = amendOpType
			opInfo.tblInfoAtStart = tblAtStart
			opInfo.tblInfoAtCommit = tblAtCommit
			opInfo.indexInfoAtStart = idxInfoAtStart
			opInfo.indexInfoAtCommit = idxInfoAtCommit
			for _, idxCol := range idxInfoAtCommit.Meta().Columns {
				colID := tblAtCommit.Meta().Columns[idxCol.Offset].ID
				oldColInfo := findColByID(tblAtStart, colID)
				// TODO: now index column MUST be found in old table columns, generated column is not supported.
				if oldColInfo == nil || oldColInfo.IsGenerated() || oldColInfo.Hidden {
					return nil, errors.Trace(errors.Errorf("amend index column=%v id=%v is not found or generated in table=%v",
						idxCol.Name, colID, tblAtCommit.Meta().Name.String()))
				}
				opInfo.relatedOldIdxCols = append(opInfo.relatedOldIdxCols, oldColInfo)
			}
			opInfo.schemaAndDecoder = newSchemaAndDecoder(sctx, tblAtStart.Meta())
			fieldTypes := make([]*types.FieldType, 0, len(tblAtStart.Meta().Columns))
			for _, col := range tblAtStart.Meta().Columns {
				fieldTypes = append(fieldTypes, &col.FieldType)
			}
			opInfo.chk = chunk.NewChunkWithCapacity(fieldTypes, 4)
			addNewIndexOp := &amendOperationAddIndex{
				info:                 opInfo,
				insertedNewIndexKeys: make(map[string]struct{}),
				deletedOldIndexKeys:  make(map[string]struct{}),
			}
			res = append(res, addNewIndexOp)
		}
	}
	return res, nil
}

// collectTblAmendOps collects amend operations for each table using the schema diff between startTS and commitTS.
func (a *amendCollector) collectTblAmendOps(sctx sessionctx.Context, phyTblID int64,
	tblInfoAtStart, tblInfoAtCommit table.Table, actionType uint64) error {
	if _, ok := a.tblAmendOpMap[phyTblID]; !ok {
		a.tblAmendOpMap[phyTblID] = make([]amendOp, 0, 4)
	}
	if needCollectModifyColOps(actionType) {
		_, err := a.collectModifyColAmendOps(tblInfoAtStart, tblInfoAtCommit)
		if err != nil {
			return err
		}
	}
	if needCollectIndexOps(actionType) {
		// TODO: currently only "add index" is considered.
		ops, err := a.collectIndexAmendOps(sctx, tblInfoAtStart, tblInfoAtCommit)
		if err != nil {
			return err
		}
		a.tblAmendOpMap[phyTblID] = append(a.tblAmendOpMap[phyTblID], ops...)
	}
	return nil
}

// mayGenDelIndexRowKeyOp returns if the row key op could generate Op_Del index key mutations.
func mayGenDelIndexRowKeyOp(keyOp kvrpcpb.Op) bool {
	return keyOp == kvrpcpb.Op_Del || keyOp == kvrpcpb.Op_Put
}

// mayGenPutIndexRowKeyOp returns if the row key op could generate Op_Put/Op_Insert index key mutations.
func mayGenPutIndexRowKeyOp(keyOp kvrpcpb.Op) bool {
	return keyOp == kvrpcpb.Op_Put || keyOp == kvrpcpb.Op_Insert
}

// amendOp is an amend operation for a specific schema change, new mutations will be generated using input ones.
type amendOp interface {
	genMutations(ctx context.Context, sctx sessionctx.Context, commitMutations transaction.CommitterMutations, kvMap *rowKvMap,
		resultMutations *transaction.PlainMutations) error
}

// amendOperationAddIndex represents one amend operation related to a specific add index change.
type amendOperationAddIndexInfo struct {
	AmendOpType       int
	tblInfoAtStart    table.Table
	tblInfoAtCommit   table.Table
	indexInfoAtStart  table.Index
	indexInfoAtCommit table.Index
	relatedOldIdxCols []*table.Column

	schemaAndDecoder *schemaAndDecoder
	chk              *chunk.Chunk
}

// amendOperationAddIndex represents the add operation will be performed on new key values for add index amend.
type amendOperationAddIndex struct {
	info *amendOperationAddIndexInfo

	// insertedNewIndexKeys is used to check duplicates for new index generated by unique key.
	insertedNewIndexKeys map[string]struct{}
	// deletedOldIndexKeys is used to check duplicates for deleted old index keys.
	deletedOldIndexKeys map[string]struct{}
}

func (a *amendOperationAddIndexInfo) String() string {
	var colStr string
	colStr += "["
	for _, colInfo := range a.relatedOldIdxCols {
		colStr += fmt.Sprintf(" %s ", colInfo.Name)
	}
	colStr += "]"
	res := fmt.Sprintf("AmenedOpType=%d phyTblID=%d idxID=%d columns=%v", a.AmendOpType, a.indexInfoAtCommit.Meta().ID,
		a.indexInfoAtCommit.Meta().ID, colStr)
	return res
}

func (a *amendOperationAddIndex) genMutations(ctx context.Context, sctx sessionctx.Context, commitMutations transaction.CommitterMutations,
	kvMap *rowKvMap, resAddMutations *transaction.PlainMutations) error {
	// There should be no duplicate keys in deletedOldIndexKeys and insertedNewIndexKeys.
	deletedMutations := transaction.NewPlainMutations(32)
	insertedMutations := transaction.NewPlainMutations(32)
	for i, key := range commitMutations.GetKeys() {
		if tablecodec.IsIndexKey(key) || tablecodec.DecodeTableID(key) != a.info.tblInfoAtCommit.Meta().ID {
			continue
		}
		var newIdxMutation *transaction.PlainMutation
		var oldIdxMutation *transaction.PlainMutation
		var err error
		keyOp := commitMutations.GetOp(i)
		if addIndexNeedRemoveOp(a.info.AmendOpType) {
			if mayGenDelIndexRowKeyOp(keyOp) {
				oldIdxMutation, err = a.genOldIdxKey(ctx, sctx, key, kvMap.oldRowKvMap)
				if err != nil {
					return err
				}
			}
		}
		if addIndexNeedAddOp(a.info.AmendOpType) {
			if mayGenPutIndexRowKeyOp(keyOp) {
				newIdxMutation, err = a.genNewIdxKey(ctx, sctx, key, kvMap.newRowKvMap)
				if err != nil {
					return err
				}
			}
		}
		skipMerge := false
		if a.info.AmendOpType == AmendNeedAddDeleteAndInsert {
			// If the old index key is the same with new index key, then the index related row value
			// is not changed in this row, we don't need to add or remove index keys for this row.
			if oldIdxMutation != nil && newIdxMutation != nil {
				if bytes.Equal(oldIdxMutation.Key, newIdxMutation.Key) {
					skipMerge = true
				}
			}
		}
		if !skipMerge {
			if oldIdxMutation != nil {
				deletedMutations.AppendMutation(*oldIdxMutation)
			}
			if newIdxMutation != nil {
				insertedMutations.AppendMutation(*newIdxMutation)
			}
		}
	}
	// For unique index, there may be conflicts on the same unique index key from different rows.Consider a update statement,
	// "Op_Del" on row_key = 3, row_val = 4, the "Op_Del"     unique_key_4 -> nil will be generated.
	// "Op_Put" on row_key = 0, row_val = 4, the "Op_Insert"  unique_key_4 -> 0   will be generated.
	// The "Op_Insert" should cover the "Op_Del" otherwise the new put row value will not have a correspond index value.
	if a.info.indexInfoAtCommit.Meta().Unique {
		for i := 0; i < len(deletedMutations.GetKeys()); i++ {
			key := deletedMutations.GetKeys()[i]
			if _, ok := a.insertedNewIndexKeys[string(key)]; !ok {
				resAddMutations.Push(deletedMutations.GetOps()[i], key, deletedMutations.GetValues()[i], deletedMutations.IsPessimisticLock(i),
					deletedMutations.IsAssertExists(i), deletedMutations.IsAssertNotExist(i))
			}
		}
		for i := 0; i < len(insertedMutations.GetKeys()); i++ {
			key := insertedMutations.GetKeys()[i]
			destKeyOp := kvrpcpb.Op_Insert
			if _, ok := a.deletedOldIndexKeys[string(key)]; ok {
				destKeyOp = kvrpcpb.Op_Put
			}
			resAddMutations.Push(destKeyOp, key, insertedMutations.GetValues()[i], insertedMutations.IsPessimisticLock(i),
				insertedMutations.IsAssertExists(i), insertedMutations.IsAssertNotExist(i))
		}
	} else {
		resAddMutations.MergeMutations(deletedMutations)
		resAddMutations.MergeMutations(insertedMutations)
	}
	return nil
}

func getCommonHandleDatum(tbl table.Table, row chunk.Row) []types.Datum {
	if !tbl.Meta().IsCommonHandle {
		return nil
	}
	datumBuf := make([]types.Datum, 0, 4)
	for _, col := range tbl.Cols() {
		if mysql.HasPriKeyFlag(col.GetFlag()) {
			datumBuf = append(datumBuf, row.GetDatum(col.Offset, &col.FieldType))
		}
	}
	return datumBuf
}

func (a *amendOperationAddIndexInfo) genIndexKeyValue(ctx context.Context, sctx sessionctx.Context, kvMap map[string][]byte,
	key []byte, kvHandle kv.Handle, keyOnly bool) ([]byte, []byte, error) {
	chk := a.chk
	chk.Reset()
	val, ok := kvMap[string(key)]
	if !ok {
		// The Op_Put may not exist in old value kv map.
		if keyOnly {
			return nil, nil, nil
		}
		return nil, nil, errors.Errorf("key=%v is not found in new row kv map", kv.Key(key).String())
	}
	err := executor.DecodeRowValToChunk(sctx, a.schemaAndDecoder.schema, a.tblInfoAtStart.Meta(), kvHandle, val, chk, a.schemaAndDecoder.decoder)
	if err != nil {
		logutil.Logger(ctx).Warn("amend decode value to chunk failed", zap.Error(err))
		return nil, nil, errors.Trace(err)
	}
	idxVals := make([]types.Datum, 0, len(a.indexInfoAtCommit.Meta().Columns))
	for _, oldCol := range a.relatedOldIdxCols {
		idxVals = append(idxVals, chk.GetRow(0).GetDatum(oldCol.Offset, &oldCol.FieldType))
	}

	rsData := tables.TryGetHandleRestoredDataWrapper(a.tblInfoAtCommit, getCommonHandleDatum(a.tblInfoAtCommit, chk.GetRow(0)), nil, a.indexInfoAtCommit.Meta())

	// Generate index key buf.
	newIdxKey, distinct, err := tablecodec.GenIndexKey(sctx.GetSessionVars().StmtCtx,
		a.tblInfoAtCommit.Meta(), a.indexInfoAtCommit.Meta(), a.tblInfoAtCommit.Meta().ID, idxVals, kvHandle, nil)
	if err != nil {
		logutil.Logger(ctx).Warn("amend generate index key failed", zap.Error(err))
		return nil, nil, errors.Trace(err)
	}
	if keyOnly {
		return newIdxKey, []byte{}, nil
	}

	// Generate index value buf.
	needRsData := tables.NeedRestoredData(a.indexInfoAtCommit.Meta().Columns, a.tblInfoAtCommit.Meta().Columns)
	newIdxVal, err := tablecodec.GenIndexValuePortal(sctx.GetSessionVars().StmtCtx, a.tblInfoAtCommit.Meta(), a.indexInfoAtCommit.Meta(), needRsData, distinct, false, idxVals, kvHandle, 0, rsData)
	if err != nil {
		logutil.Logger(ctx).Warn("amend generate index values failed", zap.Error(err))
		return nil, nil, errors.Trace(err)
	}
	return newIdxKey, newIdxVal, nil
}

func (a *amendOperationAddIndex) genNewIdxKey(ctx context.Context, sctx sessionctx.Context, key []byte,
	kvMap map[string][]byte) (*transaction.PlainMutation, error) {
	kvHandle, err := tablecodec.DecodeRowKey(key)
	if err != nil {
		logutil.Logger(ctx).Error("decode key error", zap.String("key", hex.EncodeToString(key)), zap.Error(err))
		return nil, errors.Trace(err)
	}

	newIdxKey, newIdxValue, err := a.info.genIndexKeyValue(ctx, sctx, kvMap, key, kvHandle, false)
	if err != nil {
		return nil, errors.Trace(err)
	}
	newIndexOp := kvrpcpb.Op_Put
	isPessimisticLock := false
	if _, ok := a.insertedNewIndexKeys[string(newIdxKey)]; ok {
		return nil, errors.Trace(errors.Errorf("amend process key same key=%v found for index=%v in table=%v",
			newIdxKey, a.info.indexInfoAtCommit.Meta().Name, a.info.tblInfoAtCommit.Meta().Name))
	}
	if a.info.indexInfoAtCommit.Meta().Unique {
		newIndexOp = kvrpcpb.Op_Insert
		isPessimisticLock = true
	}
	a.insertedNewIndexKeys[string(newIdxKey)] = struct{}{}
	var flags transaction.CommitterMutationFlags
	if isPessimisticLock {
		flags |= transaction.MutationFlagIsPessimisticLock
	}
	newMutation := &transaction.PlainMutation{KeyOp: newIndexOp, Key: newIdxKey, Value: newIdxValue, Flags: flags}
	return newMutation, nil
}

func (a *amendOperationAddIndex) genOldIdxKey(ctx context.Context, sctx sessionctx.Context, key []byte,
	oldValKvMap map[string][]byte) (*transaction.PlainMutation, error) {
	kvHandle, err := tablecodec.DecodeRowKey(key)
	if err != nil {
		logutil.Logger(ctx).Error("decode key error", zap.String("key", hex.EncodeToString(key)), zap.Error(err))
		return nil, errors.Trace(err)
	}
	// Generated delete index key value.
	newIdxKey, emptyVal, err := a.info.genIndexKeyValue(ctx, sctx, oldValKvMap, key, kvHandle, true)
	if err != nil {
		return nil, errors.Trace(err)
	}
	// For Op_Put the key may not exist in old key value map.
	if len(newIdxKey) > 0 {
		isPessimisticLock := false
		if _, ok := a.deletedOldIndexKeys[string(newIdxKey)]; ok {
			return nil, errors.Trace(errors.Errorf("amend process key same key=%v found for index=%v in table=%v",
				newIdxKey, a.info.indexInfoAtCommit.Meta().Name, a.info.tblInfoAtCommit.Meta().Name))
		}
		if a.info.indexInfoAtCommit.Meta().Unique {
			isPessimisticLock = true
		}
		a.deletedOldIndexKeys[string(newIdxKey)] = struct{}{}
		var flags transaction.CommitterMutationFlags
		if isPessimisticLock {
			flags |= transaction.MutationFlagIsPessimisticLock
		}
		return &transaction.PlainMutation{KeyOp: kvrpcpb.Op_Del, Key: newIdxKey, Value: emptyVal, Flags: flags}, nil
	}
	return nil, nil
}

// SchemaAmender is used to amend pessimistic transactions for schema change.
type SchemaAmender struct {
	sess *session
}

// NewSchemaAmenderForTikvTxn creates a schema amender for tikvTxn type.
func NewSchemaAmenderForTikvTxn(sess *session) *SchemaAmender {
	amender := &SchemaAmender{sess: sess}
	return amender
}

func (s *SchemaAmender) getAmendableKeys(commitMutations transaction.CommitterMutations, info *amendCollector) ([]kv.Key, []kv.Key) {
	addKeys := make([]kv.Key, 0, len(commitMutations.GetKeys()))
	removeKeys := make([]kv.Key, 0, len(commitMutations.GetKeys()))
	for i, byteKey := range commitMutations.GetKeys() {
		if tablecodec.IsIndexKey(byteKey) || !info.keyHasAmendOp(byteKey) {
			continue
		}
		keyOp := commitMutations.GetOp(i)
		switch keyOp {
		case kvrpcpb.Op_Put:
			addKeys = append(addKeys, byteKey)
			removeKeys = append(removeKeys, byteKey)
		case kvrpcpb.Op_Insert:
			addKeys = append(addKeys, byteKey)
		case kvrpcpb.Op_Del:
			removeKeys = append(removeKeys, byteKey)
		}
	}
	return addKeys, removeKeys
}

type rowKvMap struct {
	oldRowKvMap map[string][]byte
	newRowKvMap map[string][]byte
}

func (s *SchemaAmender) prepareKvMap(ctx context.Context, commitMutations transaction.CommitterMutations, info *amendCollector) (*rowKvMap, error) {
	// Get keys need to be considered for the amend operation, currently only row keys.
	addKeys, removeKeys := s.getAmendableKeys(commitMutations, info)

	// BatchGet the new key values, the Op_Put and Op_Insert type keys in memory buffer.
	txn, err := s.sess.Txn(true)
	if err != nil {
		return nil, errors.Trace(err)
	}
	newValKvMap, err := txn.BatchGet(ctx, addKeys)
	if err != nil {
		logutil.Logger(ctx).Warn("amend failed to batch get kv new keys", zap.Error(err))
		return nil, errors.Trace(err)
	}
	if len(newValKvMap) != len(addKeys) {
		logutil.Logger(ctx).Error("amend failed to batch get results invalid",
			zap.Int("addKeys len", len(addKeys)), zap.Int("newValKvMap", len(newValKvMap)))
		return nil, errors.Errorf("add keys has %v values but result kvMap has %v", len(addKeys), len(newValKvMap))
	}
	// BatchGet the old key values, the Op_Del and Op_Put types keys in storage using forUpdateTS, the Op_put type is for
	// row update using the same row key, it may not exist.
	snapshot := s.sess.GetStore().GetSnapshot(kv.Version{Ver: s.sess.sessionVars.TxnCtx.GetForUpdateTS()})
	oldValKvMap, err := snapshot.BatchGet(ctx, removeKeys)
	if err != nil {
		logutil.Logger(ctx).Warn("amend failed to batch get kv old keys", zap.Error(err))
		return nil, errors.Trace(err)
	}

	res := &rowKvMap{
		oldRowKvMap: oldValKvMap,
		newRowKvMap: newValKvMap,
	}
	return res, nil
}

func (s *SchemaAmender) checkDupKeys(ctx context.Context, mutations transaction.CommitterMutations) error {
	// Check if there are duplicate key entries.
	checkMap := make(map[string]kvrpcpb.Op)
	for i := 0; i < mutations.Len(); i++ {
		key := mutations.GetKey(i)
		keyOp := mutations.GetOp(i)
		keyVal := mutations.GetValue(i)
		if foundOp, ok := checkMap[string(key)]; ok {
			logutil.Logger(ctx).Error("duplicate key found in amend result mutations",
				zap.Stringer("key", kv.Key(key)),
				zap.Stringer("foundKeyOp", foundOp),
				zap.Stringer("thisKeyOp", keyOp),
				zap.Stringer("thisKeyValue", kv.Key(keyVal)))
			return errors.Trace(errors.Errorf("duplicate key=%s is found in mutations", kv.Key(key).String()))
		}
		checkMap[string(key)] = keyOp
	}
	return nil
}

// genAllAmendMutations generates CommitterMutations for all tables and related amend operations.
func (s *SchemaAmender) genAllAmendMutations(ctx context.Context, commitMutations transaction.CommitterMutations,
	info *amendCollector) (*transaction.PlainMutations, error) {
	rowKvMap, err := s.prepareKvMap(ctx, commitMutations, info)
	if err != nil {
		return nil, err
	}
	// Do generate add/remove mutations processing each key.
	resultNewMutations := transaction.NewPlainMutations(32)
	for _, amendOps := range info.tblAmendOpMap {
		for _, curOp := range amendOps {
			err := curOp.genMutations(ctx, s.sess, commitMutations, rowKvMap, &resultNewMutations)
			if err != nil {
				return nil, err
			}
		}
	}
	err = s.checkDupKeys(ctx, &resultNewMutations)
	if err != nil {
		return nil, err
	}
	return &resultNewMutations, nil
}

// AmendTxn does check and generate amend mutations based on input infoSchema and mutations, mutations need to prewrite
// are returned, the input commitMutations will not be changed.
func (s *SchemaAmender) AmendTxn(ctx context.Context, startInfoSchema tikv.SchemaVer, change *transaction.RelatedSchemaChange,
	commitMutations transaction.CommitterMutations) (transaction.CommitterMutations, error) {
	// Get info schema meta
	infoSchemaAtStart := startInfoSchema.(infoschema.InfoSchema)
	infoSchemaAtCheck := change.LatestInfoSchema.(infoschema.InfoSchema)

	// Collect amend operations for each table by physical table ID.
	var needAmendMem bool
	amendCollector := newAmendCollector()
	for i, tblID := range change.PhyTblIDS {
		actionType := change.ActionTypes[i]
		// Check amendable flags, return if not supported flags exist.
		if actionType&(^amendableType) != 0 {
			logutil.Logger(ctx).Info("amend action type not supported for txn", zap.Int64("tblID", tblID), zap.Uint64("actionType", actionType))
			return nil, errors.Trace(table.ErrUnsupportedOp)
		}
		// Partition table is not supported now.
		tblInfoAtStart, ok := infoSchemaAtStart.TableByID(tblID)
		if !ok {
			return nil, errors.Trace(errors.Errorf("tableID=%d is not found in infoSchema", tblID))
		}
		if tblInfoAtStart.Meta().Partition != nil {
			logutil.Logger(ctx).Info("Amend for partition table is not supported",
				zap.String("tableName", tblInfoAtStart.Meta().Name.String()), zap.Int64("tableID", tblID))
			return nil, errors.Trace(table.ErrUnsupportedOp)
		}
		tblInfoAtCommit, ok := infoSchemaAtCheck.TableByID(tblID)
		if !ok {
			return nil, errors.Trace(errors.Errorf("tableID=%d is not found in infoSchema", tblID))
		}
		if actionType&(memBufAmendType) != 0 {
			needAmendMem = true
			err := amendCollector.collectTblAmendOps(s.sess, tblID, tblInfoAtStart, tblInfoAtCommit, actionType)
			if err != nil {
				return nil, err
			}
		}
	}
	// After amend operations collect, generate related new mutations based on input commitMutations
	if needAmendMem {
		return s.genAllAmendMutations(ctx, commitMutations, amendCollector)
	}
	return nil, nil
}

func newSchemaAndDecoder(ctx sessionctx.Context, tbl *model.TableInfo) *schemaAndDecoder {
	schema := expression.NewSchema(make([]*expression.Column, 0, len(tbl.Columns))...)
	for _, col := range tbl.Columns {
		colExpr := &expression.Column{
			RetType: &col.FieldType,
			ID:      col.ID,
		}
		if col.IsGenerated() && !col.GeneratedStored {
			// This will not be used since generated column is rejected in collectIndexAmendOps.
			colExpr.VirtualExpr = &expression.Constant{}
		}
		schema.Append(colExpr)
	}
	return &schemaAndDecoder{schema, executor.NewRowDecoder(ctx, schema, tbl)}
}
