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
	pb "github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/rowcodec"
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
	if ft1.Tp == ft2.Tp &&
		ft1.Flag == ft2.Flag &&
		ft1.Flen == ft2.Flen &&
		ft1.Decimal == ft2.Decimal &&
		ft1.Charset == ft2.Charset &&
		ft1.Collate == ft2.Collate &&
		len(ft1.Elems) == len(ft2.Elems) {
		for i, elem := range ft1.Elems {
			if elem != ft2.Elems[i] {
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
		if colAtStart.FieldType.Flag != colAtCommit.FieldType.Flag {
			return errors.Trace(errors.Errorf("flag is not matched for column=%v, from=%v to=%v",
				colAtCommit.Name.String(), colAtStart.FieldType.Flag, colAtCommit.FieldType.Flag))
		}
		if colAtStart.Charset != colAtCommit.Charset || colAtStart.Collate != colAtCommit.Collate {
			return errors.Trace(errors.Errorf("charset or collate is not matched for column=%v", colAtCommit.Name.String()))
		}
		_, err := ddl.CheckModifyTypeCompatible(&colAtStart.FieldType, &colAtCommit.FieldType)
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

// collectModifyColAmendOps is used to check if there is column change from nullable to not null by now.
// TODO allow column change from nullable to not null, and generate keys check operation.
func (a *amendCollector) collectModifyColAmendOps(tblAtStart, tblAtCommit table.Table) ([]amendOp, error) {
	for _, colAtCommit := range tblAtCommit.Cols() {
		colAtStart := findColByID(tblAtStart, colAtCommit.ID)
		if colAtStart != nil {
			err := colChangeAmendable(colAtStart.ColumnInfo, colAtCommit.ColumnInfo)
			if err != nil {
				return nil, err
			}
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
			// TODO unique index amend is not supported by now.
			if idxInfoAtCommit.Meta().Unique {
				return nil, errors.Trace(errors.Errorf("amend unique index=%v for table=%v is not supported now",
					idxInfoAtCommit.Meta().Name, tblAtCommit.Meta().Name))
			}
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
			if addIndexNeedRemoveOp(amendOpType) {
				removeIndexOp := &amendOperationDeleteOldIndex{
					info: opInfo,
				}
				res = append(res, removeIndexOp)
			}
			if addIndexNeedAddOp(amendOpType) {
				addNewIndexOp := &amendOperationAddNewIndex{
					info: opInfo,
				}
				res = append(res, addNewIndexOp)
			}
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

func isDeleteOp(keyOp pb.Op) bool {
	return keyOp == pb.Op_Del || keyOp == pb.Op_Put
}

func isInsertOp(keyOp pb.Op) bool {
	return keyOp == pb.Op_Put || keyOp == pb.Op_Insert
}

// amendOp is an amend operation for a specific schema change, new mutations will be generated using input ones.
type amendOp interface {
	genMutations(ctx context.Context, sctx sessionctx.Context, commitMutations tikv.CommitterMutations, kvMap *rowKvMap,
		resultMutations *tikv.CommitterMutations) error
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

// amendOperationDeleteOldIndex represents the remove operation will be performed on old key values for add index amend.
type amendOperationDeleteOldIndex struct {
	info *amendOperationAddIndexInfo
}

// amendOperationAddNewIndex represents the add operation will be performed on new key values for add index amend.
type amendOperationAddNewIndex struct {
	info *amendOperationAddIndexInfo
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

func (a *amendOperationDeleteOldIndex) genMutations(ctx context.Context, sctx sessionctx.Context,
	commitMutations tikv.CommitterMutations, kvMap *rowKvMap, resAddMutations *tikv.CommitterMutations) error {
	for i, key := range commitMutations.GetKeys() {
		keyOp := commitMutations.GetOps()[i]
		if tablecodec.IsIndexKey(key) || tablecodec.DecodeTableID(key) != a.info.tblInfoAtCommit.Meta().ID {
			continue
		}
		if !isDeleteOp(keyOp) {
			continue
		}
		err := a.processRowKey(ctx, sctx, key, kvMap.oldRowKvMap, resAddMutations)
		if err != nil {
			return err
		}
	}
	return nil
}

func (a *amendOperationAddNewIndex) genMutations(ctx context.Context, sctx sessionctx.Context, commitMutations tikv.CommitterMutations,
	kvMap *rowKvMap, resAddMutations *tikv.CommitterMutations) error {
	for i, key := range commitMutations.GetKeys() {
		keyOp := commitMutations.GetOps()[i]
		if tablecodec.IsIndexKey(key) || tablecodec.DecodeTableID(key) != a.info.tblInfoAtCommit.Meta().ID {
			continue
		}
		if !isInsertOp(keyOp) {
			continue
		}
		err := a.processRowKey(ctx, sctx, key, kvMap.newRowKvMap, resAddMutations)
		if err != nil {
			return err
		}
	}
	return nil
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
	containsNonBinaryString := tables.ContainsNonBinaryString(a.indexInfoAtCommit.Meta().Columns, a.tblInfoAtCommit.Meta().Columns)
	newIdxVal, err := tablecodec.GenIndexValue(sctx.GetSessionVars().StmtCtx, a.tblInfoAtCommit.Meta(),
		a.indexInfoAtCommit.Meta(), containsNonBinaryString, distinct, false, idxVals, kvHandle)
	if err != nil {
		logutil.Logger(ctx).Warn("amend generate index values failed", zap.Error(err))
		return nil, nil, errors.Trace(err)
	}
	return newIdxKey, newIdxVal, nil
}

func (a *amendOperationAddNewIndex) processRowKey(ctx context.Context, sctx sessionctx.Context, key []byte,
	kvMap map[string][]byte, resAddMutations *tikv.CommitterMutations) error {
	kvHandle, err := tablecodec.DecodeRowKey(key)
	if err != nil {
		logutil.Logger(ctx).Error("decode key error", zap.String("key", hex.EncodeToString(key)), zap.Error(err))
		return errors.Trace(err)
	}

	newIdxKey, newIdxValue, err := a.info.genIndexKeyValue(ctx, sctx, kvMap, key, kvHandle, false)
	if err != nil {
		return errors.Trace(err)
	}
	resAddMutations.Push(pb.Op_Put, newIdxKey, newIdxValue, false)
	return nil
}

func (a *amendOperationDeleteOldIndex) processRowKey(ctx context.Context, sctx sessionctx.Context, key []byte,
	oldValKvMap map[string][]byte, resAddMutations *tikv.CommitterMutations) error {
	kvHandle, err := tablecodec.DecodeRowKey(key)
	if err != nil {
		logutil.Logger(ctx).Error("decode key error", zap.String("key", hex.EncodeToString(key)), zap.Error(err))
		return errors.Trace(err)
	}
	// Generated delete index key value.
	newIdxKey, emptyVal, err := a.info.genIndexKeyValue(ctx, sctx, oldValKvMap, key, kvHandle, true)
	if err != nil {
		return errors.Trace(err)
	}
	// For Op_Put the key may not exist in old key value map.
	if len(newIdxKey) > 0 {
		resAddMutations.Push(pb.Op_Del, newIdxKey, emptyVal, false)
	}
	return nil
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

func (s *SchemaAmender) getAmendableKeys(commitMutations tikv.CommitterMutations, info *amendCollector) ([]kv.Key, []kv.Key) {
	addKeys := make([]kv.Key, 0, len(commitMutations.GetKeys()))
	removeKeys := make([]kv.Key, 0, len(commitMutations.GetKeys()))
	for i, byteKey := range commitMutations.GetKeys() {
		if tablecodec.IsIndexKey(byteKey) || !info.keyHasAmendOp(byteKey) {
			continue
		}
		keyOp := commitMutations.GetOps()[i]
		if pb.Op_Put == keyOp {
			addKeys = append(addKeys, byteKey)
			removeKeys = append(removeKeys, byteKey)
		} else if pb.Op_Insert == keyOp {
			addKeys = append(addKeys, byteKey)
		} else if pb.Op_Del == keyOp {
			removeKeys = append(removeKeys, byteKey)
		} // else Do nothing.
	}
	return addKeys, removeKeys
}

type rowKvMap struct {
	oldRowKvMap map[string][]byte
	newRowKvMap map[string][]byte
}

func (s *SchemaAmender) prepareKvMap(ctx context.Context, commitMutations tikv.CommitterMutations, info *amendCollector) (*rowKvMap, error) {
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
	snapshot, err := s.sess.GetStore().GetSnapshot(kv.Version{Ver: s.sess.sessionVars.TxnCtx.GetForUpdateTS()})
	if err != nil {
		logutil.Logger(ctx).Warn("amend failed to get snapshot using forUpdateTS", zap.Error(err))
		return nil, errors.Trace(err)
	}
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

// genAllAmendMutations generates CommitterMutations for all tables and related amend operations.
func (s *SchemaAmender) genAllAmendMutations(ctx context.Context, commitMutations tikv.CommitterMutations,
	info *amendCollector) (*tikv.CommitterMutations, error) {
	rowKvMap, err := s.prepareKvMap(ctx, commitMutations, info)
	if err != nil {
		return nil, err
	}
	// Do generate add/remove mutations processing each key.
	resultNewMutations := tikv.NewCommiterMutations(32)
	for _, amendOps := range info.tblAmendOpMap {
		for _, curOp := range amendOps {
			err := curOp.genMutations(ctx, s.sess, commitMutations, rowKvMap, &resultNewMutations)
			if err != nil {
				return nil, err
			}
		}
	}
	return &resultNewMutations, nil
}

// AmendTxn does check and generate amend mutations based on input infoSchema and mutations, mutations need to prewrite
// are returned, the input commitMutations will not be changed.
func (s *SchemaAmender) AmendTxn(ctx context.Context, startInfoSchema tikv.SchemaVer, change *tikv.RelatedSchemaChange,
	commitMutations tikv.CommitterMutations) (*tikv.CommitterMutations, error) {
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
