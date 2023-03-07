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
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/parser/model"
	filter "github.com/pingcap/tidb/util/table-filter"
	"go.uber.org/zap"
)

// Default columnFamily and write columnFamily
const (
	DefaultCF = "default"
	WriteCF   = "write"
)

// SchemasReplace specifies schemas information mapping old schemas to new schemas.

type RewriteStatus int

const (
	RewriteStatus_PreConsturctMap = iota
	RewriteStatus_Run
)

type OldID = int64
type NewID = int64

type TableReplace struct {
	Name         string
	NewTableID   NewID
	PartitionMap map[OldID]NewID
	IndexMap     map[OldID]NewID
}
type DBReplace struct {
	Name     string
	NewDBID  NewID
	TableMap map[OldID]*TableReplace
}

type SchemasReplace struct {
	Status           RewriteStatus
	DbMap            map[OldID]*DBReplace
	globalTableIdMap map[OldID]NewID

	ingestRecorder *ingestrec.IngestRecorder
	RewriteTS      uint64        // used to rewrite commit ts in meta kv.
	TableFilter    filter.Filter // used to filter schema/table

	genGenGlobalID            func(ctx context.Context) (int64, error)
	genGenGlobalIDs           func(ctx context.Context, n int) ([]int64, error)
	insertDeleteRangeForTable func(jobID int64, tableIDs []int64)
	insertDeleteRangeForIndex func(jobID int64, elementID *int64, tableID int64, indexIDs []int64)

	AfterTableRewritten func(deleted bool, tableInfo *model.TableInfo)
}

// NewTableReplace creates a TableReplace struct.
func NewTableReplace(name string, newID NewID) *TableReplace {
	return &TableReplace{
		Name:         name,
		NewTableID:   newID,
		PartitionMap: make(map[OldID]NewID),
		IndexMap:     make(map[OldID]NewID),
	}
}

// NewDBReplace creates a DBReplace struct.
func NewDBReplace(name string, newID NewID) *DBReplace {
	return &DBReplace{
		Name:     name,
		NewDBID:  newID,
		TableMap: make(map[OldID]*TableReplace),
	}
}

// NewSchemasReplace creates a SchemasReplace struct.
func NewSchemasReplace(
	dbMap map[OldID]*DBReplace,
	restoreTS uint64,
	tableFilter filter.Filter,
	genID func(ctx context.Context) (int64, error),
	genIDs func(ctx context.Context, n int) ([]int64, error),
	insertDeleteRangeForTable func(jobID int64, tableIDs []int64),
	insertDeleteRangeForIndex func(jobID int64, elementID *int64, tableID int64, indexIDs []int64),
) *SchemasReplace {
	globalTableIdMap := make(map[OldID]NewID)
	for dbID, dr := range dbMap {
		globalTableIdMap[dbID] = dr.NewDBID

		for tblID, tr := range dr.TableMap {
			globalTableIdMap[tblID] = tr.NewTableID
			for oldpID, newpID := range tr.PartitionMap {
				globalTableIdMap[oldpID] = newpID
			}
		}
	}

	return &SchemasReplace{
		DbMap:                     dbMap,
		globalTableIdMap:          globalTableIdMap,
		ingestRecorder:            ingestrec.New(),
		RewriteTS:                 restoreTS,
		TableFilter:               tableFilter,
		genGenGlobalID:            genID,
		genGenGlobalIDs:           genIDs,
		insertDeleteRangeForTable: insertDeleteRangeForTable,
		insertDeleteRangeForIndex: insertDeleteRangeForIndex,
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

	if sr.Status == RewriteStatus_PreConsturctMap {
		if _, exist := sr.DbMap[dbID]; !exist {
			newID, err := sr.genGenGlobalID(context.Background())
			if err != nil {
				return nil, errors.Trace(err)
			}
			sr.DbMap[dbID] = NewDBReplace("", newID)
		}
		return nil, nil
	}

	dbMap, exist := sr.DbMap[dbID]
	if !exist {
		return nil, errors.Annotatef(berrors.ErrInvalidArgument, "failed to find id:%v in maps", dbID)
	}

	rawMetaKey.UpdateField(meta.DBkey(dbMap.NewDBID))
	if cf == WriteCF {
		rawMetaKey.UpdateTS(sr.RewriteTS)
	}
	return rawMetaKey.EncodeMetaKey(), nil
}

func (sr *SchemasReplace) rewriteDBInfo(value []byte) ([]byte, error) {
	oldDBInfo := new(model.DBInfo)
	if err := json.Unmarshal(value, oldDBInfo); err != nil {
		return nil, errors.Trace(err)
	}

	if sr.Status == RewriteStatus_PreConsturctMap {
		if dr, exist := sr.DbMap[oldDBInfo.ID]; !exist {
			newID, err := sr.genGenGlobalID(context.Background())
			if err != nil {
				return nil, errors.Trace(err)
			}
			sr.DbMap[oldDBInfo.ID] = NewDBReplace(oldDBInfo.Name.O, newID)
		} else {
			dr.Name = oldDBInfo.Name.O
		}
		return nil, nil
	}

	dbMap, exist := sr.DbMap[oldDBInfo.ID]
	if !exist {
		return nil, errors.Annotatef(berrors.ErrInvalidArgument, "failed to find id:%v in maps", oldDBInfo.ID)
	}

	newDBInfo := oldDBInfo.Clone()
	newDBInfo.ID = dbMap.NewDBID
	newValue, err := json.Marshal(newDBInfo)
	if err != nil {
		return nil, err
	}
	return newValue, nil
}

func (sr *SchemasReplace) rewriteEntryForDB(e *kv.Entry, cf string) (*kv.Entry, error) {
	r, err := sr.rewriteValueV2(
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

	dbMap, exist := sr.DbMap[dbID]
	if !exist {
		if sr.Status == RewriteStatus_PreConsturctMap {
			newID, err := sr.genGenGlobalID(context.Background())
			if err != nil {
				return nil, errors.Trace(err)
			}
			dbMap = NewDBReplace("", newID)
			sr.DbMap[dbID] = dbMap
			sr.globalTableIdMap[dbID] = newID
		} else {
			return nil, errors.Annotatef(berrors.ErrInvalidArgument, "failed to find id:%v in maps", dbID)
		}
	}

	tableMap, exist := dbMap.TableMap[tableID]
	if !exist {
		if sr.Status == RewriteStatus_PreConsturctMap {
			newID, err := sr.genGenGlobalID(context.Background())
			if err != nil {
				return nil, errors.Trace(err)
			}
			tableMap = NewTableReplace("", newID)
			dbMap.TableMap[tableID] = tableMap
			sr.globalTableIdMap[tableID] = newID
		} else {
			return nil, errors.Annotatef(berrors.ErrInvalidArgument, "failed to find id:%v in maps", tableID)
		}
	}

	rawMetaKey.UpdateKey(meta.DBkey(dbMap.NewDBID))
	rawMetaKey.UpdateField(encodeField(tableMap.NewTableID))
	if cf == WriteCF {
		rawMetaKey.UpdateTS(sr.RewriteTS)
	}
	return rawMetaKey.EncodeMetaKey(), nil
}

func (sr *SchemasReplace) rewriteTableInfo(value []byte, dbID int64) ([]byte, error) {
	var (
		tableInfo model.TableInfo
		err       error
	)
	if err := json.Unmarshal(value, &tableInfo); err != nil {
		return nil, errors.Trace(err)
	}

	// update table ID
	dbMap, exist := sr.DbMap[dbID]
	if !exist {
		if sr.Status == RewriteStatus_PreConsturctMap {
			newID, err := sr.genGenGlobalID(context.Background())
			if err != nil {
				return nil, errors.Trace(err)
			}
			dbMap = NewDBReplace("", newID)
			sr.DbMap[dbID] = dbMap
		} else {
			return nil, errors.Annotatef(berrors.ErrInvalidArgument, "failed to find id:%v in maps", dbID)
		}
	}

	tableMap, exist := dbMap.TableMap[tableInfo.ID]
	if !exist {
		if sr.Status == RewriteStatus_PreConsturctMap {
			newID, err := sr.genGenGlobalID(context.Background())
			if err != nil {
				return nil, errors.Trace(err)
			}
			tableMap = NewTableReplace(tableInfo.Name.O, newID)
			dbMap.TableMap[tableInfo.ID] = tableMap
		} else {
			return nil, errors.Annotatef(berrors.ErrInvalidArgument, "failed to find id:%v in maps", tableInfo.ID)
		}
	}

	newTableInfo := tableInfo.Clone()

	// update table ID and partition ID.
	newTableInfo.ID = tableMap.NewTableID
	partitions := newTableInfo.GetPartitionInfo()
	if partitions != nil {
		for i, tbl := range partitions.Definitions {
			newID, exist := tableMap.PartitionMap[tbl.ID]
			if !exist {
				if sr.Status == RewriteStatus_PreConsturctMap {
					newID, err = sr.genGenGlobalID(context.Background())
					if err != nil {
						return nil, errors.Trace(err)
					}
					tableMap.PartitionMap[tbl.ID] = newID
				} else {
					return nil, errors.Annotatef(berrors.ErrInvalidArgument, "failed to find id:%v in maps", tbl.ID)
				}
			}
			partitions.Definitions[i].ID = newID
		}
	}

	if sr.Status == RewriteStatus_PreConsturctMap {
		return nil, nil
	}

	// Force to disable TTL_ENABLE when restore
	if newTableInfo.TTLInfo != nil {
		newTableInfo.TTLInfo.Enable = false
	}

	if sr.AfterTableRewritten != nil {
		sr.AfterTableRewritten(false, newTableInfo)
	}

	// marshal to json
	newValue, err := json.Marshal(&newTableInfo)
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

	result, err := sr.rewriteValueV2(
		e.Value,
		cf,
		func(value []byte) ([]byte, error) {
			return sr.rewriteTableInfo(value, dbID)
		},
	)
	if err != nil {
		return nil, errors.Trace(err)
	}

	newTableID := 0
	newKey, err := sr.rewriteKeyForTable(e.Key, cf, meta.ParseTableKey, func(tableID int64) []byte {
		newTableID = int(tableID)
		return meta.TableKey(tableID)
	})
	if err != nil {
		return nil, errors.Trace(err)
	}
	// NOTE: the normal path is in the `SchemaReplace.rewriteTableInfo`
	//       for now, we rewrite key and value separately hence we cannot
	//       get a view of (is_delete, table_id, table_info) at the same time :(.
	//       Maybe we can extract the rewrite part from rewriteTableInfo.
	if result.Deleted && sr.AfterTableRewritten != nil {
		sr.AfterTableRewritten(true, &model.TableInfo{ID: int64(newTableID)})
	}

	return &kv.Entry{Key: newKey, Value: result.NewValue}, nil
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

// rewriteValueV2 likes rewriteValueV1, but provides a richer return value.
func (sr *SchemasReplace) rewriteValueV2(value []byte, cf string, rewrite func([]byte) ([]byte, error)) (rewriteResult, error) {
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
		if sr.Status == RewriteStatus_Run && cf == DefaultCF && IsMetaDDLJobHistoryKey(e.Key) { // mDDLJobHistory
			job := &model.Job{}
			if err := job.Decode(e.Value); err != nil {
				log.Debug("failed to decode the job", zap.String("error", err.Error()),
					zap.String("job", string(e.Value)))
				// The value in write-cf is like "p\XXXX\XXX" need not restore. skip it
				// The value in default-cf that can Decode() need restore.
				return nil, nil
			}

			return nil, sr.restoreFromHistory(job)
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

func (sr *SchemasReplace) restoreFromHistory(job *model.Job) error {
	if !job.IsCancelled() {
		switch job.Type {
		case model.ActionAddIndex, model.ActionAddPrimaryKey:
			if job.State == model.JobStateRollbackDone {
				return sr.deleteRange(job)
			}
			err := sr.ingestRecorder.AddJob(job)
			return errors.Trace(err)
		case model.ActionDropSchema, model.ActionDropTable, model.ActionTruncateTable, model.ActionDropIndex, model.ActionDropPrimaryKey,
			model.ActionDropTablePartition, model.ActionTruncateTablePartition, model.ActionDropColumn, model.ActionDropColumns, model.ActionModifyColumn, model.ActionDropIndexes:
			return sr.deleteRange(job)
		case model.ActionMultiSchemaChange:
			for _, sub := range job.MultiSchemaInfo.SubJobs {
				proxyJob := sub.ToProxyJob(job)
				if err := sr.restoreFromHistory(&proxyJob); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (sr *SchemasReplace) deleteRange(job *model.Job) error {
	dbReplace, exist := sr.DbMap[job.SchemaID]
	if !exist {
		// skip this mddljob, the same below
		log.Debug("try to drop a non-existent range, missing oldDBID", zap.Int64("oldDBID", job.SchemaID))
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
				log.Debug("DropSchema: record a table, but it doesn't exist in job args", zap.Int64("oldTableID", tableID))
				continue
			}
			newTableIDs = append(newTableIDs, tableReplace.NewTableID)
			for partitionID, newPartitionID := range tableReplace.PartitionMap {
				if _, exist := argsSet[partitionID]; !exist {
					log.Debug("DropSchema: record a partition, but it doesn't exist in job args", zap.Int64("oldPartitionID", partitionID))
					continue
				}
				newTableIDs = append(newTableIDs, newPartitionID)
			}
		}

		if len(newTableIDs) != len(tableIDs) {
			log.Debug("DropSchema: try to drop a non-existent table/partition, whose oldID doesn't exist in tableReplace")
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
			log.Debug("DropTable/TruncateTable: try to drop a non-existent table, missing oldTableID", zap.Int64("oldTableID", job.TableID))
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
			// delete partition id instead of table id
			for i := 0; i < len(physicalTableIDs); i++ {
				newPid, exist := tableReplace.PartitionMap[physicalTableIDs[i]]
				if !exist {
					log.Debug("DropTable/TruncateTable: try to drop a non-existent table, missing oldPartitionID", zap.Int64("oldPartitionID", physicalTableIDs[i]))
					continue
				}
				physicalTableIDs[i] = newPid
			}
			if len(physicalTableIDs) > 0 {
				sr.insertDeleteRangeForTable(newJobID, physicalTableIDs)
			}
			return nil
		}

		sr.insertDeleteRangeForTable(newJobID, []int64{tableReplace.NewTableID})
		return nil
	case model.ActionDropTablePartition, model.ActionTruncateTablePartition:
		tableReplace, exist := dbReplace.TableMap[job.TableID]
		if !exist {
			log.Debug("DropTablePartition/TruncateTablePartition: try to drop a non-existent table, missing oldTableID", zap.Int64("oldTableID", job.TableID))
			return nil
		}
		var physicalTableIDs []int64
		if err := job.DecodeArgs(&physicalTableIDs); err != nil {
			return errors.Trace(err)
		}

		for i := 0; i < len(physicalTableIDs); i++ {
			newPid, exist := tableReplace.PartitionMap[physicalTableIDs[i]]
			if !exist {
				log.Debug("DropTablePartition/TruncateTablePartition: try to drop a non-existent table, missing oldPartitionID", zap.Int64("oldPartitionID", physicalTableIDs[i]))
				continue
			}
			physicalTableIDs[i] = newPid
		}
		if len(physicalTableIDs) > 0 {
			sr.insertDeleteRangeForTable(newJobID, physicalTableIDs)
		}
		return nil
	// ActionAddIndex, ActionAddPrimaryKey needs do it, because it needs to be rolled back when it's canceled.
	case model.ActionAddIndex, model.ActionAddPrimaryKey:
		// iff job.State = model.JobStateRollbackDone
		tableReplace, exist := dbReplace.TableMap[job.TableID]
		if !exist {
			log.Debug("AddIndex/AddPrimaryKey roll-back: try to drop a non-existent table, missing oldTableID", zap.Int64("oldTableID", job.TableID))
			return nil
		}
		var indexID int64
		var ifExists bool
		var partitionIDs []int64
		if err := job.DecodeArgs(&indexID, &ifExists, &partitionIDs); err != nil {
			return errors.Trace(err)
		}

		var elementID int64 = 1
		indexIDs := []int64{indexID}

		if len(partitionIDs) > 0 {
			for _, oldPid := range partitionIDs {
				newPid, exist := tableReplace.PartitionMap[oldPid]
				if !exist {
					log.Debug("AddIndex/AddPrimaryKey roll-back: try to drop a non-existent table, missing oldPartitionID", zap.Int64("oldPartitionID", oldPid))
					continue
				}

				sr.insertDeleteRangeForIndex(newJobID, &elementID, newPid, indexIDs)
			}
		} else {
			sr.insertDeleteRangeForIndex(newJobID, &elementID, tableReplace.NewTableID, indexIDs)
		}
		return nil
	case model.ActionDropIndex, model.ActionDropPrimaryKey:
		tableReplace, exist := dbReplace.TableMap[job.TableID]
		if !exist {
			log.Debug("DropIndex/DropPrimaryKey: try to drop a non-existent table, missing oldTableID", zap.Int64("oldTableID", job.TableID))
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
					log.Debug("DropIndex/DropPrimaryKey: try to drop a non-existent table, missing oldPartitionID", zap.Int64("oldPartitionID", oldPid))
					continue
				}
				// len(indexIDs) = 1
				sr.insertDeleteRangeForIndex(newJobID, &elementID, newPid, indexIDs)
			}
		} else {
			sr.insertDeleteRangeForIndex(newJobID, &elementID, tableReplace.NewTableID, indexIDs)
		}
		return nil
	case model.ActionDropIndexes: // // Deprecated, we use ActionMultiSchemaChange instead.
		var indexIDs []int64
		var partitionIDs []int64
		if err := job.DecodeArgs(&[]model.CIStr{}, &[]bool{}, &indexIDs, &partitionIDs); err != nil {
			return errors.Trace(err)
		}
		// Remove data in TiKV.
		if len(indexIDs) == 0 {
			return nil
		}

		tableReplace, exist := dbReplace.TableMap[job.TableID]
		if !exist {
			log.Debug("DropIndexes: try to drop a non-existent table, missing oldTableID", zap.Int64("oldTableID", job.TableID))
			return nil
		}

		var elementID int64 = 1
		if len(partitionIDs) > 0 {
			for _, oldPid := range partitionIDs {
				newPid, exist := tableReplace.PartitionMap[oldPid]
				if !exist {
					log.Debug("DropIndexes: try to drop a non-existent table, missing oldPartitionID", zap.Int64("oldPartitionID", oldPid))
					continue
				}
				sr.insertDeleteRangeForIndex(newJobID, &elementID, newPid, indexIDs)
			}
		} else {
			sr.insertDeleteRangeForIndex(newJobID, &elementID, tableReplace.NewTableID, indexIDs)
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
				log.Debug("DropColumn: try to drop a non-existent table, missing oldTableID", zap.Int64("oldTableID", job.TableID))
				return nil
			}

			var elementID int64 = 1
			if len(partitionIDs) > 0 {
				for _, oldPid := range partitionIDs {
					newPid, exist := tableReplace.PartitionMap[oldPid]
					if !exist {
						log.Debug("DropColumn: try to drop a non-existent table, missing oldPartitionID", zap.Int64("oldPartitionID", oldPid))
						continue
					}
					sr.insertDeleteRangeForIndex(newJobID, &elementID, newPid, indexIDs)
				}
			} else {
				sr.insertDeleteRangeForIndex(newJobID, &elementID, tableReplace.NewTableID, indexIDs)
			}
		}
		return nil
	case model.ActionDropColumns: // Deprecated, we use ActionMultiSchemaChange instead.
		var colNames []model.CIStr
		var ifExists []bool
		var indexIDs []int64
		var partitionIDs []int64
		if err := job.DecodeArgs(&colNames, &ifExists, &indexIDs, &partitionIDs); err != nil {
			return errors.Trace(err)
		}
		if len(indexIDs) > 0 {
			tableReplace, exist := dbReplace.TableMap[job.TableID]
			if !exist {
				log.Debug("DropColumns: try to drop a non-existent table, missing oldTableID", zap.Int64("oldTableID", job.TableID))
				return nil
			}

			var elementID int64 = 1
			if len(partitionIDs) > 0 {
				for _, oldPid := range partitionIDs {
					newPid, exist := tableReplace.PartitionMap[oldPid]
					if !exist {
						log.Debug("DropColumns: try to drop a non-existent table, missing oldPartitionID", zap.Int64("oldPartitionID", oldPid))
						continue
					}
					sr.insertDeleteRangeForIndex(newJobID, &elementID, newPid, indexIDs)
				}
			} else {
				sr.insertDeleteRangeForIndex(newJobID, &elementID, tableReplace.NewTableID, indexIDs)
			}
		}
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
			log.Debug("DropColumn: try to drop a non-existent table, missing oldTableID", zap.Int64("oldTableID", job.TableID))
			return nil
		}

		var elementID int64 = 1
		if len(partitionIDs) > 0 {
			for _, oldPid := range partitionIDs {
				newPid, exist := tableReplace.PartitionMap[oldPid]
				if !exist {
					log.Debug("DropColumn: try to drop a non-existent table, missing oldPartitionID", zap.Int64("oldPartitionID", oldPid))
					continue
				}
				sr.insertDeleteRangeForIndex(newJobID, &elementID, newPid, indexIDs)
			}
		} else {
			sr.insertDeleteRangeForIndex(newJobID, &elementID, tableReplace.NewTableID, indexIDs)
		}
	}
	return nil
}
