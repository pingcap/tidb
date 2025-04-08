// Copyright 2022-present PingCAP, Inc.
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

package stream

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/restore/ingestrec"
	"github.com/pingcap/tidb/br/pkg/restore/tiflashrec"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/br/pkg/utils/consts"
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/model"
	"go.uber.org/zap"
)

type UpstreamID = int64
type DownstreamID = int64

// TableReplace specifies table information mapping from up-stream cluster to down-stream cluster.
type TableReplace struct {
	Name         string
	TableID      DownstreamID
	PartitionMap map[UpstreamID]DownstreamID
	IndexMap     map[UpstreamID]DownstreamID
	FilteredOut  bool
}

// DBReplace specifies database information mapping from up-stream cluster to down-stream cluster.
type DBReplace struct {
	Name        string
	DbID        DownstreamID
	TableMap    map[UpstreamID]*TableReplace
	FilteredOut bool
}

// SchemasReplace specifies schemas information mapping from up-stream cluster to down-stream cluster.
type SchemasReplace struct {
	DbReplaceMap map[UpstreamID]*DBReplace

	delRangeRecorder *brDelRangeExecWrapper
	ingestRecorder   *ingestrec.IngestRecorder
	TiflashRecorder  *tiflashrec.TiFlashRecorder
	RewriteTS        uint64 // used to rewrite commit ts in meta kv.

	AfterTableRewrittenFn func(deleted bool, tableInfo *model.TableInfo)
}

// NewTableReplace creates a TableReplace struct.
func NewTableReplace(name string, newID DownstreamID) *TableReplace {
	return &TableReplace{
		Name:         name,
		TableID:      newID,
		PartitionMap: make(map[UpstreamID]DownstreamID),
		IndexMap:     make(map[UpstreamID]DownstreamID),
		FilteredOut:  false,
	}
}

// NewDBReplace creates a DBReplace struct.
func NewDBReplace(name string, newID DownstreamID) *DBReplace {
	return &DBReplace{
		Name:        name,
		DbID:        newID,
		TableMap:    make(map[UpstreamID]*TableReplace),
		FilteredOut: false,
	}
}

// NewSchemasReplace creates a SchemasReplace struct.
func NewSchemasReplace(
	dbReplaceMap map[UpstreamID]*DBReplace,
	tiflashRecorder *tiflashrec.TiFlashRecorder,
	restoreTS uint64,
	recordDeleteRange func(*PreDelRangeQuery),
) *SchemasReplace {
	globalTableIdMap := make(map[UpstreamID]DownstreamID)
	for _, dr := range dbReplaceMap {
		if dr.FilteredOut {
			continue
		}
		for tblID, tr := range dr.TableMap {
			if tr.FilteredOut {
				continue
			}
			globalTableIdMap[tblID] = tr.TableID
			for oldpID, newpID := range tr.PartitionMap {
				globalTableIdMap[oldpID] = newpID
			}
		}
	}

	return &SchemasReplace{
		DbReplaceMap:     dbReplaceMap,
		delRangeRecorder: newDelRangeExecWrapper(globalTableIdMap, recordDeleteRange),
		ingestRecorder:   ingestrec.New(),
		TiflashRecorder:  tiflashRecorder,
		RewriteTS:        restoreTS,
	}
}

func (sr *SchemasReplace) rewriteKeyForDB(key []byte, cf string) ([]byte, error) {
	rawMetaKey, err := ParseTxnMetaKeyFrom(key)
	if err != nil {
		return nil, errors.Trace(err)
	}

	dbID, err := meta.ParseDBKey(rawMetaKey.Field)
	if err != nil {
		return nil, errors.Trace(err)
	}

	dbMap, exist := sr.DbReplaceMap[dbID]
	if !exist {
		return nil, errors.Annotatef(berrors.ErrInvalidArgument, "failed to find db id:%v in maps", dbID)
	}
	if dbMap.FilteredOut {
		return nil, nil
	}

	rawMetaKey.UpdateField(meta.DBkey(dbMap.DbID))
	if cf == consts.WriteCF {
		rawMetaKey.UpdateTS(sr.RewriteTS)
	}
	return rawMetaKey.EncodeMetaKey(), nil
}

func (sr *SchemasReplace) rewriteDBInfo(value []byte) ([]byte, error) {
	dbInfo := new(model.DBInfo)
	if err := json.Unmarshal(value, dbInfo); err != nil {
		return nil, errors.Trace(err)
	}

	dbMap, exist := sr.DbReplaceMap[dbInfo.ID]
	if !exist {
		return nil, errors.Annotatef(berrors.ErrInvalidArgument, "failed to find db id:%v in maps", dbInfo.ID)
	}
	if dbMap.FilteredOut {
		return nil, nil
	}

	dbInfo.ID = dbMap.DbID
	newValue, err := json.Marshal(dbInfo)
	if err != nil {
		return nil, err
	}
	return newValue, nil
}

func (sr *SchemasReplace) rewriteEntryForDB(e *kv.Entry, cf string) (*kv.Entry, error) {
	r, err := sr.rewriteValue(
		e.Value,
		cf,
		func(value []byte) ([]byte, error) {
			return sr.rewriteDBInfo(value)
		},
	)
	if err != nil {
		return nil, errors.Trace(err)
	}

	newValue := r.NewValue
	newKey, err := sr.rewriteKeyForDB(e.Key, cf)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if newKey == nil {
		return nil, nil
	}

	return &kv.Entry{Key: newKey, Value: newValue}, nil
}

func (sr *SchemasReplace) rewriteKeyForTable(
	key []byte,
	cf string,
	parseField func([]byte) (tableID int64, err error),
	encodeField func(tableID int64) []byte,
) ([]byte, error) {
	var (
		err   error
		exist bool
	)
	rawMetaKey, err := ParseTxnMetaKeyFrom(key)
	if err != nil {
		return nil, errors.Trace(err)
	}

	dbID, err := meta.ParseDBKey(rawMetaKey.Key)
	if err != nil {
		return nil, errors.Trace(err)
	}
	tableID, err := parseField(rawMetaKey.Field)
	if err != nil {
		log.Warn("parse table key failed", zap.ByteString("field", rawMetaKey.Field))
		return nil, errors.Trace(err)
	}

	dbReplace, exist := sr.DbReplaceMap[dbID]
	if !exist {
		return nil, errors.Annotatef(berrors.ErrInvalidArgument, "failed to find db id:%v in maps", dbID)
	}
	if dbReplace.FilteredOut {
		return nil, nil
	}

	tableReplace, exist := dbReplace.TableMap[tableID]
	if !exist {
		return nil, errors.Annotatef(berrors.ErrInvalidArgument, "failed to find table id:%v in maps", tableID)
	}

	// don't restore meta kv change for system db, not supported yet
	if tableReplace.FilteredOut || utils.IsSysOrTempSysDB(dbReplace.Name) {
		return nil, nil
	}

	rawMetaKey.UpdateKey(meta.DBkey(dbReplace.DbID))
	rawMetaKey.UpdateField(encodeField(tableReplace.TableID))
	if cf == consts.WriteCF {
		rawMetaKey.UpdateTS(sr.RewriteTS)
	}
	return rawMetaKey.EncodeMetaKey(), nil
}

func (sr *SchemasReplace) rewriteTableInfo(value []byte, dbID int64) ([]byte, error) {
	var (
		tableInfo    model.TableInfo
		err          error
		exist        bool
		dbReplace    *DBReplace
		tableReplace *TableReplace
	)
	if err := json.Unmarshal(value, &tableInfo); err != nil {
		return nil, errors.Trace(err)
	}

	// construct or find the id map.
	dbReplace, exist = sr.DbReplaceMap[dbID]
	if !exist {
		return nil, errors.Annotatef(berrors.ErrInvalidArgument, "failed to find db id:%v in maps", dbID)
	}
	if dbReplace.FilteredOut {
		return nil, nil
	}

	tableReplace, exist = dbReplace.TableMap[tableInfo.ID]
	if !exist {
		return nil, errors.Annotatef(berrors.ErrInvalidArgument, "failed to find table id:%v in maps", tableInfo.ID)
	}
	if tableReplace.FilteredOut {
		return nil, nil
	}

	// update table ID and partition ID.
	tableInfo.ID = tableReplace.TableID
	partitions := tableInfo.GetPartitionInfo()
	if partitions != nil {
		for i, tbl := range partitions.Definitions {
			newID, exist := tableReplace.PartitionMap[tbl.ID]
			if !exist {
				log.Error("expect partition info in table replace but got none", zap.Int64("partitionID", tbl.ID))
				return nil, errors.Annotatef(berrors.ErrInvalidArgument, "failed to find partition id:%v in replace maps", tbl.ID)
			}
			partitions.Definitions[i].ID = newID
		}
	}

	// Force to disable TTL_ENABLE when restore
	if tableInfo.TTLInfo != nil {
		tableInfo.TTLInfo.Enable = false
	}
	if sr.AfterTableRewrittenFn != nil {
		sr.AfterTableRewrittenFn(false, &tableInfo)
	}

	// marshal to json
	newValue, err := json.Marshal(&tableInfo)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return newValue, nil
}

func (sr *SchemasReplace) rewriteEntryForTable(e *kv.Entry, cf string) (*kv.Entry, error) {
	dbID, err := ParseDBIDFromTableKey(e.Key)
	if err != nil {
		return nil, errors.Trace(err)
	}

	result, err := sr.rewriteValue(
		e.Value,
		cf,
		func(value []byte) ([]byte, error) {
			return sr.rewriteTableInfo(value, dbID)
		},
	)
	if err != nil {
		return nil, errors.Trace(err)
	}

	var newTableID int64 = 0
	newKey, err := sr.rewriteKeyForTable(e.Key, cf, meta.ParseTableKey, func(tableID int64) []byte {
		newTableID = tableID
		return meta.TableKey(tableID)
	})
	if err != nil {
		return nil, errors.Trace(err)
	}
	// got filtered
	if newKey == nil {
		return nil, nil
	}

	// NOTE: the normal path is in the `SchemaReplace.rewriteTableInfo`
	//       for now, we rewrite key and value separately hence we cannot
	//       get a view of (is_delete, table_id, table_info) at the same time :(.
	//       Maybe we can extract the rewrite part from rewriteTableInfo.
	if result.Deleted && sr.AfterTableRewrittenFn != nil {
		sr.AfterTableRewrittenFn(true, &model.TableInfo{ID: newTableID})
	}

	return &kv.Entry{Key: newKey, Value: result.NewValue}, nil
}

func (sr *SchemasReplace) rewriteEntryForAutoIncrementIDKey(e *kv.Entry, cf string) (*kv.Entry, error) {
	newKey, err := sr.rewriteKeyForTable(
		e.Key,
		cf,
		meta.ParseAutoIncrementIDKey,
		meta.AutoIncrementIDKey,
	)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if newKey == nil {
		return nil, nil
	}

	return &kv.Entry{Key: newKey, Value: e.Value}, nil
}

func (sr *SchemasReplace) rewriteEntryForAutoTableIDKey(e *kv.Entry, cf string) (*kv.Entry, error) {
	newKey, err := sr.rewriteKeyForTable(
		e.Key,
		cf,
		meta.ParseAutoTableIDKey,
		meta.AutoTableIDKey,
	)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if newKey == nil {
		return nil, nil
	}

	return &kv.Entry{Key: newKey, Value: e.Value}, nil
}

func (sr *SchemasReplace) rewriteEntryForSequenceKey(e *kv.Entry, cf string) (*kv.Entry, error) {
	newKey, err := sr.rewriteKeyForTable(
		e.Key,
		cf,
		meta.ParseSequenceKey,
		meta.SequenceKey,
	)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if newKey == nil {
		return nil, nil
	}

	return &kv.Entry{Key: newKey, Value: e.Value}, nil
}

func (sr *SchemasReplace) rewriteEntryForAutoRandomTableIDKey(e *kv.Entry, cf string) (*kv.Entry, error) {
	newKey, err := sr.rewriteKeyForTable(
		e.Key,
		cf,
		meta.ParseAutoRandomTableIDKey,
		meta.AutoRandomTableIDKey,
	)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if newKey == nil {
		return nil, nil
	}

	return &kv.Entry{Key: newKey, Value: e.Value}, nil
}

type rewriteResult struct {
	Deleted  bool
	NewValue []byte
}

// rewriteValue rewrite the value if cf is "default", or rewrite the shortValue if cf is "write".
func (sr *SchemasReplace) rewriteValue(value []byte, cf string, rewriteFunc func([]byte) ([]byte, error)) (rewriteResult, error) {
	switch cf {
	case consts.DefaultCF:
		newValue, err := rewriteFunc(value)
		if err != nil {
			return rewriteResult{}, errors.Trace(err)
		}
		return rewriteResult{
			NewValue: newValue,
			Deleted:  false,
		}, nil
	case consts.WriteCF:
		rawWriteCFValue := new(RawWriteCFValue)
		if err := rawWriteCFValue.ParseFrom(value); err != nil {
			return rewriteResult{}, errors.Trace(err)
		}

		if rawWriteCFValue.IsDelete() {
			return rewriteResult{
				NewValue: value,
				Deleted:  true,
			}, nil
		}
		if rawWriteCFValue.IsRollback() {
			return rewriteResult{
				NewValue: value,
				Deleted:  false,
			}, nil
		}
		if !rawWriteCFValue.HasShortValue() {
			return rewriteResult{
				NewValue: value,
			}, nil
		}

		shortValue, err := rewriteFunc(rawWriteCFValue.GetShortValue())
		if err != nil {
			log.Info("failed to rewrite short value",
				zap.ByteString("write-type", []byte{rawWriteCFValue.GetWriteType()}),
				zap.Int("short-value-len", len(rawWriteCFValue.GetShortValue())))
			return rewriteResult{}, errors.Trace(err)
		}

		rawWriteCFValue.UpdateShortValue(shortValue)
		return rewriteResult{NewValue: rawWriteCFValue.EncodeTo()}, nil
	default:
		panic(fmt.Sprintf("not support cf:%s", cf))
	}
}

func (sr *SchemasReplace) GetIngestRecorder() *ingestrec.IngestRecorder {
	return sr.ingestRecorder
}

// RewriteMetaKvEntry uses to rewrite tableID/dbID in entry.key and entry.value
func (sr *SchemasReplace) RewriteMetaKvEntry(e *kv.Entry, cf string) (*kv.Entry, error) {
	if !utils.IsMetaDBKey(e.Key) {
		// need to special handle ddl job history during actual restore phase. The job history contains index ingestion
		// and range deletion that need to be handled separately after restore.
		if cf == consts.DefaultCF && utils.IsMetaDDLJobHistoryKey(e.Key) { // mDDLJobHistory
			job := &model.Job{}
			if err := job.Decode(e.Value); err != nil {
				log.Debug("failed to decode the job",
					zap.String("error", err.Error()), zap.String("job", string(e.Value)))
				// The value in write-cf is like "p\XXXX\XXX" need not restore. skip it
				// The value in default-cf that can Decode() need restore.
				return nil, nil
			}

			return nil, sr.processIngestIndexAndDeleteRangeFromJob(job)
		}
		return nil, nil
	}

	rawKey, err := ParseTxnMetaKeyFrom(e.Key)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if meta.IsDBkey(rawKey.Field) {
		return sr.rewriteEntryForDB(e, cf)
	} else if !meta.IsDBkey(rawKey.Key) {
		return nil, nil
	}

	if meta.IsTableKey(rawKey.Field) {
		return sr.rewriteEntryForTable(e, cf)
	} else if meta.IsAutoIncrementIDKey(rawKey.Field) {
		return sr.rewriteEntryForAutoIncrementIDKey(e, cf)
	} else if meta.IsAutoTableIDKey(rawKey.Field) {
		return sr.rewriteEntryForAutoTableIDKey(e, cf)
	} else if meta.IsSequenceKey(rawKey.Field) {
		return sr.rewriteEntryForSequenceKey(e, cf)
	} else if meta.IsAutoRandomTableIDKey(rawKey.Field) {
		return sr.rewriteEntryForAutoRandomTableIDKey(e, cf)
	}
	return nil, nil
}

func (sr *SchemasReplace) tryRecordIngestIndex(job *model.Job) error {
	if job.Type != model.ActionMultiSchemaChange {
		return sr.ingestRecorder.TryAddJob(job, false)
	}

	for i, sub := range job.MultiSchemaInfo.SubJobs {
		proxyJob := sub.ToProxyJob(job, i)
		// ASSERT: the proxyJob can not be MultiSchemaInfo anymore
		if err := sr.ingestRecorder.TryAddJob(&proxyJob, true); err != nil {
			return err
		}
	}
	return nil
}

// processIngestIndexAndDeleteRangeFromJob handles two special cases during log backup meta key replay.
// 1. index ingestion is not captured by the log backup, thus we need to restore them manually later
// 2. delete range also needs to be handled to clean up dropped table since it was previously relying on GC to clean it up
func (sr *SchemasReplace) processIngestIndexAndDeleteRangeFromJob(job *model.Job) error {
	if ddl.JobNeedGC(job) {
		if err := ddl.AddDelRangeJobInternal(context.TODO(), sr.delRangeRecorder, job); err != nil {
			return err
		}
	}

	return sr.tryRecordIngestIndex(job)
}

type DelRangeParams struct {
	JobID    int64
	ElemID   int64
	StartKey string
	EndKey   string
}

type PreDelRangeQuery struct {
	Sql        string
	ParamsList []DelRangeParams
}

type brDelRangeExecWrapper struct {
	globalTableIdMap map[UpstreamID]DownstreamID

	recordDeleteRange func(*PreDelRangeQuery)

	// temporary values
	query *PreDelRangeQuery
}

func newDelRangeExecWrapper(
	globalTableIdMap map[UpstreamID]DownstreamID,
	recordDeleteRange func(*PreDelRangeQuery),
) *brDelRangeExecWrapper {
	return &brDelRangeExecWrapper{
		globalTableIdMap:  globalTableIdMap,
		recordDeleteRange: recordDeleteRange,

		query: nil,
	}
}

// UpdateTSOForJob just does nothing. BR would generate ts after log restore done.
func (bdr *brDelRangeExecWrapper) UpdateTSOForJob() error {
	return nil
}

func (bdr *brDelRangeExecWrapper) PrepareParamsList(sz int) {
	bdr.query = &PreDelRangeQuery{
		ParamsList: make([]DelRangeParams, 0, sz),
	}
}

func (bdr *brDelRangeExecWrapper) RewriteTableID(tableID int64) (int64, bool) {
	newTableID, exists := bdr.globalTableIdMap[tableID]
	if !exists {
		log.Warn("failed to find the downstream id when rewrite delete range, "+
			"it might due to table has been filtered out if filters have been specified", zap.Int64("old tableID", tableID))
	}
	return newTableID, exists
}

func (bdr *brDelRangeExecWrapper) AppendParamsList(jobID, elemID int64, startKey, endKey string) {
	bdr.query.ParamsList = append(bdr.query.ParamsList, DelRangeParams{jobID, elemID, startKey, endKey})
}

func (bdr *brDelRangeExecWrapper) ConsumeDeleteRange(ctx context.Context, sql string) error {
	bdr.query.Sql = sql
	bdr.recordDeleteRange(bdr.query)
	bdr.query = nil
	return nil
}
