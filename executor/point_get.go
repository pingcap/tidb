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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package executor

import (
	"context"
	"fmt"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tidb/ddl/placement"
	"github.com/pingcap/tidb/distsql"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/dbterror"
	"github.com/pingcap/tidb/util/execdetails"
	"github.com/pingcap/tidb/util/logutil/consistency"
	"github.com/pingcap/tidb/util/rowcodec"
	"github.com/tikv/client-go/v2/txnkv/txnsnapshot"
)

func (b *executorBuilder) buildPointGet(p *plannercore.PointGetPlan) Executor {
	if err := b.validCanReadTemporaryOrCacheTable(p.TblInfo); err != nil {
		b.err = err
		return nil
	}

	startTS, err := b.getSnapshotTS()
	if err != nil {
		b.err = err
		return nil
	}
	e := &PointGetExecutor{
		baseExecutor:     newBaseExecutor(b.ctx, p.Schema(), p.ID()),
		readReplicaScope: b.readReplicaScope,
		isStaleness:      b.isStaleness,
	}

	if p.TblInfo.TableCacheStatusType == model.TableCacheStatusEnable {
		e.cacheTable = b.getCacheTable(p.TblInfo, startTS)
	}
	e.base().initCap = 1
	e.base().maxChunkSize = 1
	e.Init(p, startTS)
	if e.lock {
		b.hasLock = true
	}
	return e
}

// PointGetExecutor executes point select query.
type PointGetExecutor struct {
	baseExecutor

	tblInfo          *model.TableInfo
	handle           kv.Handle
	idxInfo          *model.IndexInfo
	partInfo         *model.PartitionDefinition
	idxKey           kv.Key
	handleVal        []byte
	idxVals          []types.Datum
	startTS          uint64
	readReplicaScope string
	isStaleness      bool
	txn              kv.Transaction
	snapshot         kv.Snapshot
	done             bool
	lock             bool
	lockWaitTime     int64
	rowDecoder       *rowcodec.ChunkDecoder

	columns []*model.ColumnInfo
	// virtualColumnIndex records all the indices of virtual columns and sort them in definition
	// to make sure we can compute the virtual column in right order.
	virtualColumnIndex []int

	// virtualColumnRetFieldTypes records the RetFieldTypes of virtual columns.
	virtualColumnRetFieldTypes []*types.FieldType

	stats      *runtimeStatsWithSnapshot
	cacheTable kv.MemBuffer
}

// Init set fields needed for PointGetExecutor reuse, this does NOT change baseExecutor field
func (e *PointGetExecutor) Init(p *plannercore.PointGetPlan, startTs uint64) {
	decoder := NewRowDecoder(e.ctx, p.Schema(), p.TblInfo)
	e.tblInfo = p.TblInfo
	e.handle = p.Handle
	e.idxInfo = p.IndexInfo
	e.idxVals = p.IndexValues
	e.startTS = startTs
	e.done = false
	if e.tblInfo.TempTableType == model.TempTableNone {
		e.lock = p.Lock
		e.lockWaitTime = p.LockWaitTime
	} else {
		// Temporary table should not do any lock operations
		e.lock = false
		e.lockWaitTime = 0
	}
	e.rowDecoder = decoder
	e.partInfo = p.PartitionInfo
	e.columns = p.Columns
	e.buildVirtualColumnInfo()
}

// buildVirtualColumnInfo saves virtual column indices and sort them in definition order
func (e *PointGetExecutor) buildVirtualColumnInfo() {
	e.virtualColumnIndex = buildVirtualColumnIndex(e.Schema(), e.columns)
	if len(e.virtualColumnIndex) > 0 {
		e.virtualColumnRetFieldTypes = make([]*types.FieldType, len(e.virtualColumnIndex))
		for i, idx := range e.virtualColumnIndex {
			e.virtualColumnRetFieldTypes[i] = e.schema.Columns[idx].RetType
		}
	}
}

// Open implements the Executor interface.
func (e *PointGetExecutor) Open(context.Context) error {
	txnCtx := e.ctx.GetSessionVars().TxnCtx
	snapshotTS := e.startTS
	if e.lock {
		snapshotTS = txnCtx.GetForUpdateTS()
	}
	var err error
	e.txn, err = e.ctx.Txn(false)
	if err != nil {
		return err
	}
	if e.txn.Valid() && txnCtx.StartTS == txnCtx.GetForUpdateTS() && txnCtx.StartTS == snapshotTS {
		e.snapshot = e.txn.GetSnapshot()
	} else {
		e.snapshot = e.ctx.GetSnapshotWithTS(snapshotTS)
	}
	if e.ctx.GetSessionVars().StmtCtx.RCCheckTS {
		e.snapshot.SetOption(kv.IsolationLevel, kv.RCCheckTS)
	}
	if e.cacheTable != nil {
		e.snapshot = cacheTableSnapshot{e.snapshot, e.cacheTable}
	}
	if err := e.verifyTxnScope(); err != nil {
		return err
	}
	if e.runtimeStats != nil {
		snapshotStats := &txnsnapshot.SnapshotRuntimeStats{}
		e.stats = &runtimeStatsWithSnapshot{
			SnapshotRuntimeStats: snapshotStats,
		}
		e.snapshot.SetOption(kv.CollectRuntimeStats, snapshotStats)
		e.ctx.GetSessionVars().StmtCtx.RuntimeStatsColl.RegisterStats(e.id, e.stats)
	}
	readReplicaType := e.ctx.GetSessionVars().GetReplicaRead()
	if readReplicaType.IsFollowerRead() && !e.ctx.GetSessionVars().StmtCtx.RCCheckTS {
		e.snapshot.SetOption(kv.ReplicaRead, readReplicaType)
	}
	e.snapshot.SetOption(kv.TaskID, e.ctx.GetSessionVars().StmtCtx.TaskID)
	e.snapshot.SetOption(kv.ReadReplicaScope, e.readReplicaScope)
	e.snapshot.SetOption(kv.IsStalenessReadOnly, e.isStaleness)
	if readReplicaType.IsClosestRead() && e.readReplicaScope != kv.GlobalTxnScope {
		e.snapshot.SetOption(kv.MatchStoreLabels, []*metapb.StoreLabel{
			{
				Key:   placement.DCLabelKey,
				Value: e.readReplicaScope,
			},
		})
	}
	failpoint.Inject("assertPointReplicaOption", func(val failpoint.Value) {
		assertScope := val.(string)
		if readReplicaType.IsClosestRead() && assertScope != e.readReplicaScope {
			panic("point get replica option fail")
		}
	})
	setResourceGroupTaggerForTxn(e.ctx.GetSessionVars().StmtCtx, e.snapshot)
	setRPCInterceptorOfExecCounterForTxn(e.ctx.GetSessionVars(), e.snapshot)
	return nil
}

// Close implements the Executor interface.
func (e *PointGetExecutor) Close() error {
	if e.runtimeStats != nil && e.snapshot != nil {
		e.snapshot.SetOption(kv.CollectRuntimeStats, nil)
	}
	if e.idxInfo != nil && e.tblInfo != nil {
		actRows := int64(0)
		if e.runtimeStats != nil {
			actRows = e.runtimeStats.GetActRows()
		}
		e.ctx.StoreIndexUsage(e.tblInfo.ID, e.idxInfo.ID, actRows)
	}
	e.done = false
	return nil
}

// Next implements the Executor interface.
func (e *PointGetExecutor) Next(ctx context.Context, req *chunk.Chunk) error {
	req.Reset()
	if e.done {
		return nil
	}
	e.done = true

	var tblID int64
	var err error
	if e.partInfo != nil {
		tblID = e.partInfo.ID
	} else {
		tblID = e.tblInfo.ID
	}
	if e.lock {
		e.updateDeltaForTableID(tblID)
	}
	if e.idxInfo != nil {
		if isCommonHandleRead(e.tblInfo, e.idxInfo) {
			handleBytes, err := EncodeUniqueIndexValuesForKey(e.ctx, e.tblInfo, e.idxInfo, e.idxVals)
			if err != nil {
				if kv.ErrNotExist.Equal(err) {
					return nil
				}
				return err
			}
			e.handle, err = kv.NewCommonHandle(handleBytes)
			if err != nil {
				return err
			}
		} else {
			e.idxKey, err = EncodeUniqueIndexKey(e.ctx, e.tblInfo, e.idxInfo, e.idxVals, tblID)
			if err != nil && !kv.ErrNotExist.Equal(err) {
				return err
			}

			e.handleVal, err = e.get(ctx, e.idxKey)
			if err != nil {
				if !kv.ErrNotExist.Equal(err) {
					return err
				}
			}

			// try lock the index key if isolation level is not read consistency
			// also lock key if read consistency read a value
			if !e.ctx.GetSessionVars().IsPessimisticReadConsistency() || len(e.handleVal) > 0 {
				err = e.lockKeyIfNeeded(ctx, e.idxKey)
				if err != nil {
					return err
				}
				// Change the unique index LOCK into PUT record.
				if e.lock && len(e.handleVal) > 0 {
					if !e.txn.Valid() {
						return kv.ErrInvalidTxn
					}
					memBuffer := e.txn.GetMemBuffer()
					err = memBuffer.Set(e.idxKey, e.handleVal)
					if err != nil {
						return err
					}
				}
			}
			if len(e.handleVal) == 0 {
				return nil
			}

			var iv kv.Handle
			iv, err = tablecodec.DecodeHandleInUniqueIndexValue(e.handleVal, e.tblInfo.IsCommonHandle)
			if err != nil {
				return err
			}
			e.handle = iv

			// The injection is used to simulate following scenario:
			// 1. Session A create a point get query but pause before second time `GET` kv from backend
			// 2. Session B create an UPDATE query to update the record that will be obtained in step 1
			// 3. Then point get retrieve data from backend after step 2 finished
			// 4. Check the result
			failpoint.InjectContext(ctx, "pointGetRepeatableReadTest-step1", func() {
				if ch, ok := ctx.Value("pointGetRepeatableReadTest").(chan struct{}); ok {
					// Make `UPDATE` continue
					close(ch)
				}
				// Wait `UPDATE` finished
				failpoint.InjectContext(ctx, "pointGetRepeatableReadTest-step2", nil)
			})
		}
	}

	key := tablecodec.EncodeRowKeyWithHandle(tblID, e.handle)
	val, err := e.getAndLock(ctx, key)
	if err != nil {
		return err
	}
	if len(val) == 0 {
		if e.idxInfo != nil && !isCommonHandleRead(e.tblInfo, e.idxInfo) &&
			!e.ctx.GetSessionVars().StmtCtx.WeakConsistency {
			return (&consistency.Reporter{
				HandleEncode: func(handle kv.Handle) kv.Key {
					return key
				},
				IndexEncode: func(idxRow *consistency.RecordData) kv.Key {
					return e.idxKey
				},
				Tbl:  e.tblInfo,
				Idx:  e.idxInfo,
				Sctx: e.ctx,
			}).ReportLookupInconsistent(ctx,
				1, 0,
				[]kv.Handle{e.handle},
				[]kv.Handle{e.handle},
				[]consistency.RecordData{{}},
			)
		}
		return nil
	}
	err = DecodeRowValToChunk(e.base().ctx, e.schema, e.tblInfo, e.handle, val, req, e.rowDecoder)
	if err != nil {
		return err
	}

	err = FillVirtualColumnValue(e.virtualColumnRetFieldTypes, e.virtualColumnIndex,
		e.schema, e.columns, e.ctx, req)
	if err != nil {
		return err
	}
	return nil
}

func (e *PointGetExecutor) getAndLock(ctx context.Context, key kv.Key) (val []byte, err error) {
	if e.ctx.GetSessionVars().IsPessimisticReadConsistency() {
		// Only Lock the exist keys in RC isolation.
		val, err = e.get(ctx, key)
		if err != nil {
			if !kv.ErrNotExist.Equal(err) {
				return nil, err
			}
			return nil, nil
		}
		err = e.lockKeyIfNeeded(ctx, key)
		if err != nil {
			return nil, err
		}
		return val, nil
	}
	// Lock the key before get in RR isolation, then get will get the value from the cache.
	err = e.lockKeyIfNeeded(ctx, key)
	if err != nil {
		return nil, err
	}
	val, err = e.get(ctx, key)
	if err != nil {
		if !kv.ErrNotExist.Equal(err) {
			return nil, err
		}
		return nil, nil
	}
	return val, nil
}

func (e *PointGetExecutor) lockKeyIfNeeded(ctx context.Context, key []byte) error {
	if len(key) == 0 {
		return nil
	}
	if e.lock {
		seVars := e.ctx.GetSessionVars()
		lockCtx := newLockCtx(seVars, e.lockWaitTime, 1)
		lockCtx.InitReturnValues(1)
		err := doLockKeys(ctx, e.ctx, lockCtx, key)
		if err != nil {
			return err
		}
		lockCtx.IterateValuesNotLocked(func(k, v []byte) {
			seVars.TxnCtx.SetPessimisticLockCache(kv.Key(k), v)
		})
		if len(e.handleVal) > 0 {
			seVars.TxnCtx.SetPessimisticLockCache(e.idxKey, e.handleVal)
		}
	}
	return nil
}

// get will first try to get from txn buffer, then check the pessimistic lock cache,
// then the store. Kv.ErrNotExist will be returned if key is not found
func (e *PointGetExecutor) get(ctx context.Context, key kv.Key) ([]byte, error) {
	if len(key) == 0 {
		return nil, kv.ErrNotExist
	}

	var (
		val []byte
		err error
	)

	if e.txn.Valid() && !e.txn.IsReadOnly() {
		// We cannot use txn.Get directly here because the snapshot in txn and the snapshot of e.snapshot may be
		// different for pessimistic transaction.
		val, err = e.txn.GetMemBuffer().Get(ctx, key)
		if err == nil {
			return val, err
		}
		if !kv.IsErrNotFound(err) {
			return nil, err
		}
		// key does not exist in mem buffer, check the lock cache
		if e.lock {
			var ok bool
			val, ok = e.ctx.GetSessionVars().TxnCtx.GetKeyInPessimisticLockCache(key)
			if ok {
				return val, nil
			}
		}
		// fallthrough to snapshot get.
	}

	lock := e.tblInfo.Lock
	if lock != nil && (lock.Tp == model.TableLockRead || lock.Tp == model.TableLockReadOnly) {
		if e.ctx.GetSessionVars().EnablePointGetCache {
			cacheDB := e.ctx.GetStore().GetMemCache()
			val, err = cacheDB.UnionGet(ctx, e.tblInfo.ID, e.snapshot, key)
			if err != nil {
				return nil, err
			}
			return val, nil
		}
	}
	// if not read lock or table was unlock then snapshot get
	return e.snapshot.Get(ctx, key)
}

func (e *PointGetExecutor) verifyTxnScope() error {
	// Stale Read uses the calculated TSO for the read,
	// so there is no need to check the TxnScope here.
	if e.isStaleness {
		return nil
	}
	txnScope := e.readReplicaScope
	if txnScope == "" || txnScope == kv.GlobalTxnScope {
		return nil
	}
	var tblID int64
	var tblName string
	var partName string
	is := e.ctx.GetInfoSchema().(infoschema.InfoSchema)
	if e.partInfo != nil {
		tblID = e.partInfo.ID
		tblInfo, _, partInfo := is.FindTableByPartitionID(tblID)
		tblName = tblInfo.Meta().Name.String()
		partName = partInfo.Name.String()
	} else {
		tblID = e.tblInfo.ID
		tblInfo, _ := is.TableByID(tblID)
		tblName = tblInfo.Meta().Name.String()
	}
	valid := distsql.VerifyTxnScope(txnScope, tblID, is)
	if valid {
		return nil
	}
	if len(partName) > 0 {
		return dbterror.ErrInvalidPlacementPolicyCheck.GenWithStackByArgs(
			fmt.Sprintf("table %v's partition %v can not be read by %v txn_scope", tblName, partName, txnScope))
	}
	return dbterror.ErrInvalidPlacementPolicyCheck.GenWithStackByArgs(
		fmt.Sprintf("table %v can not be read by %v txn_scope", tblName, txnScope))
}

// EncodeUniqueIndexKey encodes a unique index key.
func EncodeUniqueIndexKey(ctx sessionctx.Context, tblInfo *model.TableInfo, idxInfo *model.IndexInfo, idxVals []types.Datum, tID int64) (_ []byte, err error) {
	encodedIdxVals, err := EncodeUniqueIndexValuesForKey(ctx, tblInfo, idxInfo, idxVals)
	if err != nil {
		return nil, err
	}
	return tablecodec.EncodeIndexSeekKey(tID, idxInfo.ID, encodedIdxVals), nil
}

// EncodeUniqueIndexValuesForKey encodes unique index values for a key.
func EncodeUniqueIndexValuesForKey(ctx sessionctx.Context, tblInfo *model.TableInfo, idxInfo *model.IndexInfo, idxVals []types.Datum) (_ []byte, err error) {
	sc := ctx.GetSessionVars().StmtCtx
	for i := range idxVals {
		colInfo := tblInfo.Columns[idxInfo.Columns[i].Offset]
		// table.CastValue will append 0x0 if the string value's length is smaller than the BINARY column's length.
		// So we don't use CastValue for string value for now.
		// TODO: merge two if branch.
		if colInfo.Tp == mysql.TypeString || colInfo.Tp == mysql.TypeVarString || colInfo.Tp == mysql.TypeVarchar {
			var str string
			str, err = idxVals[i].ToString()
			idxVals[i].SetString(str, colInfo.FieldType.Collate)
		} else if colInfo.Tp == mysql.TypeEnum && (idxVals[i].Kind() == types.KindString || idxVals[i].Kind() == types.KindBytes || idxVals[i].Kind() == types.KindBinaryLiteral) {
			var str string
			var e types.Enum
			str, err = idxVals[i].ToString()
			if err != nil {
				return nil, kv.ErrNotExist
			}
			e, err = types.ParseEnumName(colInfo.FieldType.Elems, str, colInfo.FieldType.Collate)
			if err != nil {
				return nil, kv.ErrNotExist
			}
			idxVals[i].SetMysqlEnum(e, colInfo.FieldType.Collate)
		} else {
			// If a truncated error or an overflow error is thrown when converting the type of `idxVal[i]` to
			// the type of `colInfo`, the `idxVal` does not exist in the `idxInfo` for sure.
			idxVals[i], err = table.CastValue(ctx, idxVals[i], colInfo, true, false)
			if types.ErrOverflow.Equal(err) || types.ErrDataTooLong.Equal(err) ||
				types.ErrTruncated.Equal(err) || types.ErrTruncatedWrongVal.Equal(err) {
				return nil, kv.ErrNotExist
			}
		}
		if err != nil {
			return nil, err
		}
	}

	encodedIdxVals, err := codec.EncodeKey(sc, nil, idxVals...)
	if err != nil {
		return nil, err
	}
	return encodedIdxVals, nil
}

// DecodeRowValToChunk decodes row value into chunk checking row format used.
func DecodeRowValToChunk(sctx sessionctx.Context, schema *expression.Schema, tblInfo *model.TableInfo,
	handle kv.Handle, rowVal []byte, chk *chunk.Chunk, rd *rowcodec.ChunkDecoder) error {
	if rowcodec.IsNewFormat(rowVal) {
		return rd.DecodeToChunk(rowVal, handle, chk)
	}
	return decodeOldRowValToChunk(sctx, schema, tblInfo, handle, rowVal, chk)
}

func decodeOldRowValToChunk(sctx sessionctx.Context, schema *expression.Schema, tblInfo *model.TableInfo, handle kv.Handle,
	rowVal []byte, chk *chunk.Chunk) error {
	pkCols := tables.TryGetCommonPkColumnIds(tblInfo)
	prefixColIDs := tables.PrimaryPrefixColumnIDs(tblInfo)
	colID2CutPos := make(map[int64]int, schema.Len())
	for _, col := range schema.Columns {
		if _, ok := colID2CutPos[col.ID]; !ok {
			colID2CutPos[col.ID] = len(colID2CutPos)
		}
	}
	cutVals, err := tablecodec.CutRowNew(rowVal, colID2CutPos)
	if err != nil {
		return err
	}
	if cutVals == nil {
		cutVals = make([][]byte, len(colID2CutPos))
	}
	decoder := codec.NewDecoder(chk, sctx.GetSessionVars().Location())
	for i, col := range schema.Columns {
		// fill the virtual column value after row calculation
		if col.VirtualExpr != nil {
			chk.AppendNull(i)
			continue
		}
		ok, err := tryDecodeFromHandle(tblInfo, i, col, handle, chk, decoder, pkCols, prefixColIDs)
		if err != nil {
			return err
		}
		if ok {
			continue
		}
		cutPos := colID2CutPos[col.ID]
		if len(cutVals[cutPos]) == 0 {
			colInfo := getColInfoByID(tblInfo, col.ID)
			d, err1 := table.GetColOriginDefaultValue(sctx, colInfo)
			if err1 != nil {
				return err1
			}
			chk.AppendDatum(i, &d)
			continue
		}
		_, err = decoder.DecodeOne(cutVals[cutPos], i, col.RetType)
		if err != nil {
			return err
		}
	}
	return nil
}

func tryDecodeFromHandle(tblInfo *model.TableInfo, schemaColIdx int, col *expression.Column, handle kv.Handle, chk *chunk.Chunk,
	decoder *codec.Decoder, pkCols []int64, prefixColIDs []int64) (bool, error) {
	if tblInfo.PKIsHandle && mysql.HasPriKeyFlag(col.RetType.Flag) {
		chk.AppendInt64(schemaColIdx, handle.IntValue())
		return true, nil
	}
	if col.ID == model.ExtraHandleID {
		chk.AppendInt64(schemaColIdx, handle.IntValue())
		return true, nil
	}
	if types.NeedRestoredData(col.RetType) {
		return false, nil
	}
	// Try to decode common handle.
	if mysql.HasPriKeyFlag(col.RetType.Flag) {
		for i, hid := range pkCols {
			if col.ID == hid && notPKPrefixCol(hid, prefixColIDs) {
				_, err := decoder.DecodeOne(handle.EncodedCol(i), schemaColIdx, col.RetType)
				if err != nil {
					return false, errors.Trace(err)
				}
				return true, nil
			}
		}
	}
	return false, nil
}

func notPKPrefixCol(colID int64, prefixColIDs []int64) bool {
	for _, pCol := range prefixColIDs {
		if pCol == colID {
			return false
		}
	}
	return true
}

func getColInfoByID(tbl *model.TableInfo, colID int64) *model.ColumnInfo {
	for _, col := range tbl.Columns {
		if col.ID == colID {
			return col
		}
	}
	return nil
}

type runtimeStatsWithSnapshot struct {
	*txnsnapshot.SnapshotRuntimeStats
}

func (e *runtimeStatsWithSnapshot) String() string {
	if e.SnapshotRuntimeStats != nil {
		return e.SnapshotRuntimeStats.String()
	}
	return ""
}

// Clone implements the RuntimeStats interface.
func (e *runtimeStatsWithSnapshot) Clone() execdetails.RuntimeStats {
	newRs := &runtimeStatsWithSnapshot{}
	if e.SnapshotRuntimeStats != nil {
		snapshotStats := e.SnapshotRuntimeStats.Clone()
		newRs.SnapshotRuntimeStats = snapshotStats
	}
	return newRs
}

// Merge implements the RuntimeStats interface.
func (e *runtimeStatsWithSnapshot) Merge(other execdetails.RuntimeStats) {
	tmp, ok := other.(*runtimeStatsWithSnapshot)
	if !ok {
		return
	}
	if tmp.SnapshotRuntimeStats != nil {
		if e.SnapshotRuntimeStats == nil {
			snapshotStats := tmp.SnapshotRuntimeStats.Clone()
			e.SnapshotRuntimeStats = snapshotStats
			return
		}
		e.SnapshotRuntimeStats.Merge(tmp.SnapshotRuntimeStats)
	}
}

// Tp implements the RuntimeStats interface.
func (e *runtimeStatsWithSnapshot) Tp() int {
	return execdetails.TpRuntimeStatsWithSnapshot
}
