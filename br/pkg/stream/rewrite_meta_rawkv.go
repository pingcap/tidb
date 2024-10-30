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
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/log"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/logutil"
	"github.com/pingcap/tidb/br/pkg/restore/ingestrec"
	"github.com/pingcap/tidb/br/pkg/restore/tiflashrec"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/tablecodec"
	filter "github.com/pingcap/tidb/util/table-filter"
	"go.uber.org/zap"
)

// Default columnFamily and write columnFamily
const (
	DefaultCF = "default"
	WriteCF   = "write"
)

type RewriteStatus int

const (
	RewriteStatusPreConstructMap = iota // represents construct map status.
	RewriteStatusRestoreKV              // represents restore meta kv status.
)

type UpstreamID = int64
type DownstreamID = int64

// TableReplace specifies table information mapping from up-stream cluster to up-stream cluster.
type TableReplace struct {
	Name         string
	TableID      DownstreamID
	PartitionMap map[UpstreamID]DownstreamID
	IndexMap     map[UpstreamID]DownstreamID
}

// DBReplace specifies database information mapping from up-stream cluster to up-stream cluster.
type DBReplace struct {
	Name     string
	DbID     DownstreamID
	TableMap map[UpstreamID]*TableReplace
}

// SchemasReplace specifies schemas information mapping from up-stream cluster to up-stream cluster.
type SchemasReplace struct {
	status             RewriteStatus
	DbMap              map[UpstreamID]*DBReplace
	globalTableIdMap   map[UpstreamID]DownstreamID
	needConstructIdMap bool

	ingestRecorder  *ingestrec.IngestRecorder
	TiflashRecorder *tiflashrec.TiFlashRecorder
	RewriteTS       uint64        // used to rewrite commit ts in meta kv.
	TableFilter     filter.Filter // used to filter schema/table

	genGenGlobalID            func(ctx context.Context) (int64, error)
	genGenGlobalIDs           func(ctx context.Context, n int) ([]int64, error)
	insertDeleteRangeForTable func(jobID int64, tableIDs []int64)
	insertDeleteRangeForIndex func(jobID int64, elementID *int64, tableID int64, indexIDs []int64)

	AfterTableRewritten func(deleted bool, tableInfo *model.TableInfo)
}

// NewTableReplace creates a TableReplace struct.
func NewTableReplace(name string, newID DownstreamID) *TableReplace {
	return &TableReplace{
		Name:         name,
		TableID:      newID,
		PartitionMap: make(map[UpstreamID]DownstreamID),
		IndexMap:     make(map[UpstreamID]DownstreamID),
	}
}

// NewDBReplace creates a DBReplace struct.
func NewDBReplace(name string, newID DownstreamID) *DBReplace {
	return &DBReplace{
		Name:     name,
		DbID:     newID,
		TableMap: make(map[UpstreamID]*TableReplace),
	}
}

// NewSchemasReplace creates a SchemasReplace struct.
func NewSchemasReplace(
	dbMap map[UpstreamID]*DBReplace,
	needConstructIdMap bool,
	tiflashRecorder *tiflashrec.TiFlashRecorder,
	restoreTS uint64,
	tableFilter filter.Filter,
	genID func(ctx context.Context) (int64, error),
	genIDs func(ctx context.Context, n int) ([]int64, error),
	insertDeleteRangeForTable func(jobID int64, tableIDs []int64),
	insertDeleteRangeForIndex func(jobID int64, elementID *int64, tableID int64, indexIDs []int64),
) *SchemasReplace {
	globalTableIdMap := make(map[UpstreamID]DownstreamID)
	for _, dr := range dbMap {
		for tblID, tr := range dr.TableMap {
			globalTableIdMap[tblID] = tr.TableID
			for oldpID, newpID := range tr.PartitionMap {
				globalTableIdMap[oldpID] = newpID
			}
		}
	}

	return &SchemasReplace{
		DbMap:                     dbMap,
		globalTableIdMap:          globalTableIdMap,
		needConstructIdMap:        needConstructIdMap,
		ingestRecorder:            ingestrec.New(),
		TiflashRecorder:           tiflashRecorder,
		RewriteTS:                 restoreTS,
		TableFilter:               tableFilter,
		genGenGlobalID:            genID,
		genGenGlobalIDs:           genIDs,
		insertDeleteRangeForTable: insertDeleteRangeForTable,
		insertDeleteRangeForIndex: insertDeleteRangeForIndex,
	}
}

func (sr *SchemasReplace) NeedConstructIdMap() bool {
	return sr.needConstructIdMap
}

// TidySchemaMaps produces schemas id maps from up-stream to down-stream.
func (sr *SchemasReplace) TidySchemaMaps() []*backuppb.PitrDBMap {
	dbMaps := make([]*backuppb.PitrDBMap, 0, len(sr.DbMap))

	for dbID, dr := range sr.DbMap {
		dbm := backuppb.PitrDBMap{
			Name: dr.Name,
			IdMap: &backuppb.IDMap{
				UpstreamId:   dbID,
				DownstreamId: dr.DbID,
			},
			Tables: make([]*backuppb.PitrTableMap, 0, len(dr.TableMap)),
		}

		for tblID, tr := range dr.TableMap {
			tm := backuppb.PitrTableMap{
				Name: tr.Name,
				IdMap: &backuppb.IDMap{
					UpstreamId:   tblID,
					DownstreamId: tr.TableID,
				},
				Partitions: make([]*backuppb.IDMap, 0, len(tr.PartitionMap)),
			}

			for upID, downID := range tr.PartitionMap {
				pm := backuppb.IDMap{
					UpstreamId:   upID,
					DownstreamId: downID,
				}
				tm.Partitions = append(tm.Partitions, &pm)
			}
			dbm.Tables = append(dbm.Tables, &tm)
		}
		dbMaps = append(dbMaps, &dbm)
	}

	return dbMaps
}

func FromSchemaMaps(dbMaps []*backuppb.PitrDBMap) map[UpstreamID]*DBReplace {
	dbReplaces := make(map[UpstreamID]*DBReplace)

	for _, db := range dbMaps {
		dr := NewDBReplace(db.Name, db.IdMap.DownstreamId)
		dbReplaces[db.IdMap.UpstreamId] = dr

		for _, tbl := range db.Tables {
			tr := NewTableReplace(tbl.Name, tbl.IdMap.DownstreamId)
			dr.TableMap[tbl.IdMap.UpstreamId] = tr
			for _, p := range tbl.Partitions {
				tr.PartitionMap[p.UpstreamId] = p.DownstreamId
			}
		}
	}

	return dbReplaces
}

// IsPreConsturctMapStatus checks the status is PreConsturctMap.
func (sr *SchemasReplace) IsPreConsturctMapStatus() bool {
	return sr.status == RewriteStatusPreConstructMap
}

// IsRestoreKVStatus checks the status is RestoreKV.
func (sr *SchemasReplace) IsRestoreKVStatus() bool {
	return sr.status == RewriteStatusRestoreKV
}

// SetPreConstructMapStatus sets the PreConstructMap status.
func (sr *SchemasReplace) SetPreConstructMapStatus() {
	sr.status = RewriteStatusPreConstructMap
}

// SetRestoreKVStatus sets the RestoreKV status.
func (sr *SchemasReplace) SetRestoreKVStatus() {
	sr.status = RewriteStatusRestoreKV
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

	if sr.IsPreConsturctMapStatus() {
		if _, exist := sr.DbMap[dbID]; !exist {
			newID, err := sr.genGenGlobalID(context.Background())
			if err != nil {
				return nil, errors.Trace(err)
			}
			sr.DbMap[dbID] = NewDBReplace("", newID)
			sr.globalTableIdMap[dbID] = newID
		}
		return nil, nil
	}

	dbMap, exist := sr.DbMap[dbID]
	if !exist {
		return nil, errors.Annotatef(berrors.ErrInvalidArgument, "failed to find id:%v in maps", dbID)
	}

	rawMetaKey.UpdateField(meta.DBkey(dbMap.DbID))
	if cf == WriteCF {
		rawMetaKey.UpdateTS(sr.RewriteTS)
	}
	return rawMetaKey.EncodeMetaKey(), nil
}

func (sr *SchemasReplace) rewriteDBInfo(value []byte) ([]byte, error) {
	dbInfo := new(model.DBInfo)
	if err := json.Unmarshal(value, dbInfo); err != nil {
		return nil, errors.Trace(err)
	}

	if sr.IsPreConsturctMapStatus() {
		if dr, exist := sr.DbMap[dbInfo.ID]; !exist {
			newID, err := sr.genGenGlobalID(context.Background())
			if err != nil {
				return nil, errors.Trace(err)
			}
			sr.DbMap[dbInfo.ID] = NewDBReplace(dbInfo.Name.O, newID)
		} else {
			dr.Name = dbInfo.Name.O
		}
		return nil, nil
	}

	dbMap, exist := sr.DbMap[dbInfo.ID]
	if !exist {
		return nil, errors.Annotatef(berrors.ErrInvalidArgument, "failed to find id:%v in maps", dbInfo.ID)
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

	return &kv.Entry{Key: newKey, Value: newValue}, nil
}

func (sr *SchemasReplace) getDBIDFromTableKey(key []byte) (int64, error) {
	rawMetaKey, err := ParseTxnMetaKeyFrom(key)
	if err != nil {
		return 0, errors.Trace(err)
	}
	return meta.ParseDBKey(rawMetaKey.Key)
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

	dbReplace, exist := sr.DbMap[dbID]
	if !exist {
		if sr.IsPreConsturctMapStatus() {
			newID, err := sr.genGenGlobalID(context.Background())
			if err != nil {
				return nil, errors.Trace(err)
			}
			dbReplace = NewDBReplace("", newID)
			sr.DbMap[dbID] = dbReplace
		} else {
			return nil, errors.Annotatef(berrors.ErrInvalidArgument, "failed to find id:%v in maps", dbID)
		}
	}

	tableReplace, exist := dbReplace.TableMap[tableID]
	if !exist {
		newID, exist := sr.globalTableIdMap[tableID]
		if !exist {
			if sr.IsRestoreKVStatus() {
				return nil, errors.Annotatef(berrors.ErrInvalidArgument, "failed to find id:%v in maps", tableID)
			}

			newID, err = sr.genGenGlobalID(context.Background())
			if err != nil {
				return nil, errors.Trace(err)
			}
			sr.globalTableIdMap[tableID] = newID
		}

		tableReplace = NewTableReplace("", newID)
		dbReplace.TableMap[tableID] = tableReplace
	}

	if sr.IsPreConsturctMapStatus() {
		return nil, nil
	}

	rawMetaKey.UpdateKey(meta.DBkey(dbReplace.DbID))
	rawMetaKey.UpdateField(encodeField(tableReplace.TableID))
	if cf == WriteCF {
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
	dbReplace, exist = sr.DbMap[dbID]
	if !exist {
		if sr.IsRestoreKVStatus() {
			return nil, errors.Annotatef(berrors.ErrInvalidArgument, "failed to find id:%v in maps", dbID)
		}

		newID, err := sr.genGenGlobalID(context.Background())
		if err != nil {
			return nil, errors.Trace(err)
		}
		dbReplace = NewDBReplace("", newID)
		sr.DbMap[dbID] = dbReplace
	}

	tableReplace, exist = dbReplace.TableMap[tableInfo.ID]
	if !exist {
		newID, exist := sr.globalTableIdMap[tableInfo.ID]
		if !exist {
			if sr.IsRestoreKVStatus() {
				return nil, errors.Annotatef(berrors.ErrInvalidArgument, "failed to find id:%v in maps", tableInfo.ID)
			}

			newID, err = sr.genGenGlobalID(context.Background())
			if err != nil {
				return nil, errors.Trace(err)
			}
			sr.globalTableIdMap[tableInfo.ID] = newID
		}

		tableReplace = NewTableReplace(tableInfo.Name.O, newID)
		dbReplace.TableMap[tableInfo.ID] = tableReplace
	} else {
		tableReplace.Name = tableInfo.Name.O
	}

	// update table ID and partition ID.
	tableInfo.ID = tableReplace.TableID
	partitions := tableInfo.GetPartitionInfo()
	if partitions != nil {
		for i, tbl := range partitions.Definitions {
			newID, exist := tableReplace.PartitionMap[tbl.ID]
			if !exist {
				newID, exist = sr.globalTableIdMap[tbl.ID]
				if !exist {
					if sr.IsRestoreKVStatus() {
						return nil, errors.Annotatef(berrors.ErrInvalidArgument, "failed to find id:%v in maps", tbl.ID)
					}

					newID, err = sr.genGenGlobalID(context.Background())
					if err != nil {
						return nil, errors.Trace(err)
					}
					sr.globalTableIdMap[tbl.ID] = newID
				}
				tableReplace.PartitionMap[tbl.ID] = newID
			}
			partitions.Definitions[i].ID = newID
		}
	}

	if sr.IsPreConsturctMapStatus() {
		return nil, nil
	}

	// Force to disable TTL_ENABLE when restore
	if tableInfo.TTLInfo != nil {
		tableInfo.TTLInfo.Enable = false
	}
	if sr.AfterTableRewritten != nil {
		sr.AfterTableRewritten(false, &tableInfo)
	}

	// marshal to json
	newValue, err := json.Marshal(&tableInfo)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return newValue, nil
}

func (sr *SchemasReplace) rewriteEntryForTable(e *kv.Entry, cf string) (*kv.Entry, error) {
	dbID, err := sr.getDBIDFromTableKey(e.Key)
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

	if sr.IsPreConsturctMapStatus() {
		return nil, nil
	}
	// NOTE: the normal path is in the `SchemaReplace.rewriteTableInfo`
	//       for now, we rewrite key and value separately hence we cannot
	//       get a view of (is_delete, table_id, table_info) at the same time :(.
	//       Maybe we can extract the rewrite part from rewriteTableInfo.
	if result.Deleted && sr.AfterTableRewritten != nil {
		sr.AfterTableRewritten(true, &model.TableInfo{ID: newTableID})
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

	return &kv.Entry{Key: newKey, Value: e.Value}, nil
}

type rewriteResult struct {
	Deleted  bool
	NewValue []byte
}

// rewriteValue rewrite the value if cf is "default", or rewrite the shortValue if cf is "write".
func (sr *SchemasReplace) rewriteValue(value []byte, cf string, rewrite func([]byte) ([]byte, error)) (rewriteResult, error) {
	switch cf {
	case DefaultCF:
		newValue, err := rewrite(value)
		if err != nil {
			return rewriteResult{}, errors.Trace(err)
		}
		return rewriteResult{
			NewValue: newValue,
			Deleted:  false,
		}, nil
	case WriteCF:
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

		shortValue, err := rewrite(rawWriteCFValue.GetShortValue())
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

// RewriteKvEntry uses to rewrite tableID/dbID in entry.key and entry.value
func (sr *SchemasReplace) RewriteKvEntry(e *kv.Entry, cf string) (*kv.Entry, error) {
	// skip mDDLJob
	if !IsMetaDBKey(e.Key) {
		if sr.IsRestoreKVStatus() && cf == DefaultCF && IsMetaDDLJobHistoryKey(e.Key) { // mDDLJobHistory
			job := &model.Job{}
			if err := job.Decode(e.Value); err != nil {
				log.Debug("failed to decode the job",
					zap.String("error", err.Error()), zap.String("job", string(e.Value)))
				// The value in write-cf is like "p\XXXX\XXX" need not restore. skip it
				// The value in default-cf that can Decode() need restore.
				return nil, nil
			}

			return nil, sr.restoreFromHistory(job, false)
		}
		return nil, nil
	}

	rawKey, err := ParseTxnMetaKeyFrom(e.Key)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if meta.IsDBkey(rawKey.Field) {
		return sr.rewriteEntryForDB(e, cf)
	} else if meta.IsDBkey(rawKey.Key) {
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
		} else {
			return nil, nil
		}
	} else {
		return nil, nil
	}
}

func (sr *SchemasReplace) restoreFromHistory(job *model.Job, isSubJob bool) error {
	if !job.IsCancelled() {
		switch job.Type {
		case model.ActionAddIndex, model.ActionAddPrimaryKey:
			// AddJob would filter out the job state
			if err := sr.ingestRecorder.AddJob(job, isSubJob); err != nil {
				return err
			}
			return sr.deleteRange(job)
		case model.ActionDropSchema, model.ActionDropTable,
			model.ActionTruncateTable, model.ActionDropIndex,
			model.ActionDropPrimaryKey,
			model.ActionDropTablePartition, model.ActionTruncateTablePartition,
			model.ActionDropColumn, model.ActionModifyColumn,
			model.ActionReorganizePartition:
			return sr.deleteRange(job)
		case model.ActionMultiSchemaChange:
			for _, sub := range job.MultiSchemaInfo.SubJobs {
				proxyJob := sub.ToProxyJob(job)
				// ASSERT: the proxyJob can not be MultiSchemaInfo anymore
				if err := sr.restoreFromHistory(&proxyJob, true); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (sr *SchemasReplace) deleteRange(job *model.Job) error {
	lctx := logutil.ContextWithField(context.Background(), logutil.RedactAny("category", "ddl: rewrite delete range"))
	dbReplace, exist := sr.DbMap[job.SchemaID]
	if !exist {
		// skip this mddljob, the same below
		logutil.CL(lctx).Warn("try to drop a non-existent range, missing oldDBID", zap.Int64("oldDBID", job.SchemaID))
		return nil
	}

	// allocate a new fake job id to avoid row conflicts in table `gc_delete_range`
	newJobID, err := sr.genGenGlobalID(context.Background())
	if err != nil {
		return errors.Trace(err)
	}

	switch job.Type {
	case model.ActionDropSchema:
		var tableIDs []int64
		if err := job.DecodeArgs(&tableIDs); err != nil {
			return errors.Trace(err)
		}
		// Note: tableIDs contains partition ids, cannot directly use dbReplace.TableMap
		/* TODO: use global ID replace map
		 *
		 *	for i := 0; i < len(tableIDs); i++ {
		 *		tableReplace, exist := dbReplace.TableMap[tableIDs[i]]
		 *		if !exist {
		 *			return errors.Errorf("DropSchema: try to drop a non-existent table, missing oldTableID")
		 *		}
		 *		tableIDs[i] = tableReplace.NewTableID
		 *	}
		 */

		argsSet := make(map[int64]struct{}, len(tableIDs))
		for _, tableID := range tableIDs {
			argsSet[tableID] = struct{}{}
		}

		newTableIDs := make([]int64, 0, len(tableIDs))
		for tableID, tableReplace := range dbReplace.TableMap {
			if _, exist := argsSet[tableID]; !exist {
				logutil.CL(lctx).Warn("DropSchema: record a table, but it doesn't exist in job args",
					zap.Int64("oldTableID", tableID))
				continue
			}
			newTableIDs = append(newTableIDs, tableReplace.TableID)
			for partitionID, newPartitionID := range tableReplace.PartitionMap {
				if _, exist := argsSet[partitionID]; !exist {
					logutil.CL(lctx).Warn("DropSchema: record a partition, but it doesn't exist in job args",
						zap.Int64("oldPartitionID", partitionID))
					continue
				}
				newTableIDs = append(newTableIDs, newPartitionID)
			}
		}

		if len(newTableIDs) != len(tableIDs) {
			logutil.CL(lctx).Warn(
				"DropSchema: try to drop a non-existent table/partition, whose oldID doesn't exist in tableReplace")
			// only drop newTableIDs' ranges
		}

		if len(newTableIDs) > 0 {
			sr.insertDeleteRangeForTable(newJobID, newTableIDs)
		}

		return nil
	// Truncate will generates new id for table or partition, so ts can be large enough
	case model.ActionDropTable, model.ActionTruncateTable:
		tableReplace, exist := dbReplace.TableMap[job.TableID]
		if !exist {
			logutil.CL(lctx).Warn("DropTable/TruncateTable: try to drop a non-existent table, missing oldTableID",
				zap.Int64("oldTableID", job.TableID))
			return nil
		}

		// The startKey here is for compatibility with previous versions, old version did not endKey so don't have to deal with.
		var startKey kv.Key // unused
		var physicalTableIDs []int64
		var ruleIDs []string // unused
		if err := job.DecodeArgs(&startKey, &physicalTableIDs, &ruleIDs); err != nil {
			return errors.Trace(err)
		}
		if len(physicalTableIDs) > 0 {
			newPhysicalTableIDs := make([]int64, 0, len(physicalTableIDs))
			// delete partition id
			for _, oldPid := range physicalTableIDs {
				newPid, exist := tableReplace.PartitionMap[oldPid]
				if !exist {
					logutil.CL(lctx).Warn("DropTable/TruncateTable: try to drop a non-existent table, missing oldPartitionID",
						zap.Int64("oldPartitionID", oldPid))
					continue
				}
				newPhysicalTableIDs = append(newPhysicalTableIDs, newPid)
			}

			// logical table may contain global index regions, so delete the logical table range.
			newPhysicalTableIDs = append(newPhysicalTableIDs, tableReplace.TableID)
			if len(newPhysicalTableIDs) > 0 {
				sr.insertDeleteRangeForTable(newJobID, newPhysicalTableIDs)
			}

			return nil
		}

		sr.insertDeleteRangeForTable(newJobID, []int64{tableReplace.TableID})
		return nil
	case model.ActionDropTablePartition, model.ActionTruncateTablePartition, model.ActionReorganizePartition:
		tableReplace, exist := dbReplace.TableMap[job.TableID]
		if !exist {
			logutil.CL(lctx).Warn(
				"DropTablePartition/TruncateTablePartition: try to drop a non-existent table, missing oldTableID",
				zap.Int64("oldTableID", job.TableID))
			return nil
		}
		var physicalTableIDs []int64
		if err := job.DecodeArgs(&physicalTableIDs); err != nil {
			return errors.Trace(err)
		}

		newPhysicalTableIDs := make([]int64, 0, len(physicalTableIDs))
		for _, oldPid := range physicalTableIDs {
			newPid, exist := tableReplace.PartitionMap[oldPid]
			if !exist {
				logutil.CL(lctx).Warn(
					"DropTablePartition/TruncateTablePartition: try to drop a non-existent table, missing oldPartitionID",
					zap.Int64("oldPartitionID", oldPid))
				continue
			}
			newPhysicalTableIDs = append(newPhysicalTableIDs, newPid)
		}
		if len(newPhysicalTableIDs) > 0 {
			sr.insertDeleteRangeForTable(newJobID, newPhysicalTableIDs)
		}
		return nil
	// ActionAddIndex, ActionAddPrimaryKey needs do it, because it needs to be rolled back when it's canceled.
	case model.ActionAddIndex, model.ActionAddPrimaryKey:
		tableReplace, exist := dbReplace.TableMap[job.TableID]
		if !exist {
			logutil.CL(lctx).Warn("AddIndex/AddPrimaryKey roll-back: try to drop a non-existent table, missing oldTableID",
				zap.Int64("oldTableID", job.TableID))
			return nil
		}
		var indexID int64
		var ifExists bool
		var partitionIDs []int64
		if err := job.DecodeArgs(&indexID, &ifExists, &partitionIDs); err != nil {
			return errors.Trace(err)
		}

		tempIdxID := tablecodec.TempIndexPrefix | indexID
		var elementID int64 = 1
		var indexIDs []int64
		if job.State == model.JobStateRollbackDone {
			indexIDs = []int64{indexID, tempIdxID}
		} else {
			indexIDs = []int64{tempIdxID}
		}

		if len(partitionIDs) > 0 {
			for _, oldPid := range partitionIDs {
				newPid, exist := tableReplace.PartitionMap[oldPid]
				if !exist {
					logutil.CL(lctx).Warn(
						"AddIndex/AddPrimaryKey roll-back: try to drop a non-existent table, missing oldPartitionID",
						zap.Int64("oldPartitionID", oldPid))
					continue
				}

				sr.insertDeleteRangeForIndex(newJobID, &elementID, newPid, indexIDs)
			}
		} else {
			sr.insertDeleteRangeForIndex(newJobID, &elementID, tableReplace.TableID, indexIDs)
		}
		return nil
	case model.ActionDropIndex, model.ActionDropPrimaryKey:
		tableReplace, exist := dbReplace.TableMap[job.TableID]
		if !exist {
			logutil.CL(lctx).Warn("DropIndex/DropPrimaryKey: try to drop a non-existent table, missing oldTableID", zap.Int64("oldTableID", job.TableID))
			return nil
		}

		var indexName interface{}
		var ifExists bool
		var indexID int64
		var partitionIDs []int64
		if err := job.DecodeArgs(&indexName, &ifExists, &indexID, &partitionIDs); err != nil {
			return errors.Trace(err)
		}

		var elementID int64 = 1
		indexIDs := []int64{indexID}

		if len(partitionIDs) > 0 {
			for _, oldPid := range partitionIDs {
				newPid, exist := tableReplace.PartitionMap[oldPid]
				if !exist {
					logutil.CL(lctx).Warn("DropIndex/DropPrimaryKey: try to drop a non-existent table, missing oldPartitionID", zap.Int64("oldPartitionID", oldPid))
					continue
				}
				// len(indexIDs) = 1
				sr.insertDeleteRangeForIndex(newJobID, &elementID, newPid, indexIDs)
			}
		} else {
			sr.insertDeleteRangeForIndex(newJobID, &elementID, tableReplace.TableID, indexIDs)
		}
		return nil
	case model.ActionDropColumn:
		var colName model.CIStr
		var ifExists bool
		var indexIDs []int64
		var partitionIDs []int64
		if err := job.DecodeArgs(&colName, &ifExists, &indexIDs, &partitionIDs); err != nil {
			return errors.Trace(err)
		}
		if len(indexIDs) > 0 {
			tableReplace, exist := dbReplace.TableMap[job.TableID]
			if !exist {
				logutil.CL(lctx).Warn("DropColumn: try to drop a non-existent table, missing oldTableID", zap.Int64("oldTableID", job.TableID))
				return nil
			}

			var elementID int64 = 1
			if len(partitionIDs) > 0 {
				for _, oldPid := range partitionIDs {
					newPid, exist := tableReplace.PartitionMap[oldPid]
					if !exist {
						logutil.CL(lctx).Warn("DropColumn: try to drop a non-existent table, missing oldPartitionID", zap.Int64("oldPartitionID", oldPid))
						continue
					}
					sr.insertDeleteRangeForIndex(newJobID, &elementID, newPid, indexIDs)
				}
			} else {
				sr.insertDeleteRangeForIndex(newJobID, &elementID, tableReplace.TableID, indexIDs)
			}
		}
		return nil
	case model.ActionModifyColumn:
		var indexIDs []int64
		var partitionIDs []int64
		if err := job.DecodeArgs(&indexIDs, &partitionIDs); err != nil {
			return errors.Trace(err)
		}
		if len(indexIDs) == 0 {
			return nil
		}
		tableReplace, exist := dbReplace.TableMap[job.TableID]
		if !exist {
			logutil.CL(lctx).Warn("DropColumn: try to drop a non-existent table, missing oldTableID", zap.Int64("oldTableID", job.TableID))
			return nil
		}

		var elementID int64 = 1
		if len(partitionIDs) > 0 {
			for _, oldPid := range partitionIDs {
				newPid, exist := tableReplace.PartitionMap[oldPid]
				if !exist {
					logutil.CL(lctx).Warn("DropColumn: try to drop a non-existent table, missing oldPartitionID", zap.Int64("oldPartitionID", oldPid))
					continue
				}
				sr.insertDeleteRangeForIndex(newJobID, &elementID, newPid, indexIDs)
			}
		} else {
			sr.insertDeleteRangeForIndex(newJobID, &elementID, tableReplace.TableID, indexIDs)
		}
	}
	return nil
}
