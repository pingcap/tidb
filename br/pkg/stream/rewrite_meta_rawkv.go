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
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
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

type OldID = int64
type NewID = int64

type TableReplace struct {
	OldTableInfo *model.TableInfo
	NewTableID   NewID
	PartitionMap map[OldID]NewID
	IndexMap     map[OldID]NewID
}

type DBReplace struct {
	OldDBInfo *model.DBInfo
	NewDBID   NewID
	TableMap  map[OldID]*TableReplace
}

type SchemasReplace struct {
	DbMap                     map[OldID]*DBReplace
	RewriteTS                 uint64
	TableFilter               filter.Filter
	genGenGlobalID            func(ctx context.Context) (int64, error)
	genGenGlobalIDs           func(ctx context.Context, n int) ([]int64, error)
	insertDeleteRangeForTable func(jobID int64, tableIDs []int64)
	insertDeleteRangeForIndex func(jobID int64, elementID *int64, tableID int64, indexIDs []int64)
}

// NewTableReplace creates a TableReplace struct.
func NewTableReplace(tableInfo *model.TableInfo, newID NewID) *TableReplace {
	return &TableReplace{
		OldTableInfo: tableInfo,
		NewTableID:   newID,
		PartitionMap: make(map[OldID]NewID),
		IndexMap:     make(map[OldID]NewID),
	}
}

// NewDBReplace creates a DBReplace struct.
func NewDBReplace(dbInfo *model.DBInfo, newID NewID) *DBReplace {
	return &DBReplace{
		OldDBInfo: dbInfo,
		NewDBID:   newID,
		TableMap:  make(map[OldID]*TableReplace),
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
	return &SchemasReplace{
		DbMap:                     dbMap,
		RewriteTS:                 restoreTS,
		TableFilter:               tableFilter,
		genGenGlobalID:            genID,
		genGenGlobalIDs:           genIDs,
		insertDeleteRangeForTable: insertDeleteRangeForTable,
		insertDeleteRangeForIndex: insertDeleteRangeForIndex,
	}
}

func (sr *SchemasReplace) rewriteKeyForDB(key []byte, cf string) ([]byte, bool, error) {
	rawMetaKey, err := ParseTxnMetaKeyFrom(key)
	if err != nil {
		return nil, false, errors.Trace(err)
	}

	dbID, err := meta.ParseDBKey(rawMetaKey.Field)
	if err != nil {
		return nil, false, errors.Trace(err)
	}

	dbReplace, exist := sr.DbMap[dbID]
	if !exist {
		newID, err := sr.genGenGlobalID(context.Background())
		if err != nil {
			return nil, false, errors.Trace(err)
		}
		dbReplace = NewDBReplace(nil, newID)
		sr.DbMap[dbID] = dbReplace
	}

	rawMetaKey.UpdateField(meta.DBkey(dbReplace.NewDBID))
	if cf == WriteCF {
		rawMetaKey.UpdateTS(sr.RewriteTS)
	}
	return rawMetaKey.EncodeMetaKey(), true, nil
}

func (sr *SchemasReplace) rewriteDBInfo(value []byte) ([]byte, bool, error) {
	oldDBInfo := new(model.DBInfo)
	if err := json.Unmarshal(value, oldDBInfo); err != nil {
		return nil, false, errors.Trace(err)
	}

	dbReplace, exist := sr.DbMap[oldDBInfo.ID]
	if !exist {
		// If the schema has existed, don't need generate a new ID.
		// Or we need a new ID to rewrite the dbID in kv entry.
		newID, err := sr.genGenGlobalID(context.Background())
		if err != nil {
			return nil, false, errors.Trace(err)
		}

		dbReplace = NewDBReplace(oldDBInfo, newID)
		sr.DbMap[oldDBInfo.ID] = dbReplace
	} else {
		// update the old DBInfo, because we need save schemas at the end of 'restore point'.
		dbReplace.OldDBInfo = oldDBInfo
	}

	log.Debug("rewrite dbinfo", zap.String("dbName", dbReplace.OldDBInfo.Name.O),
		zap.Int64("old ID", oldDBInfo.ID), zap.Int64("new ID", dbReplace.NewDBID))

	newDBInfo := oldDBInfo.Clone()
	newDBInfo.ID = dbReplace.NewDBID
	newValue, err := json.Marshal(newDBInfo)
	if err != nil {
		return nil, false, err
	}
	return newValue, true, nil
}

func (sr *SchemasReplace) rewriteEntryForDB(e *kv.Entry, cf string) (*kv.Entry, error) {
	newValue, needWrite, err := sr.rewriteValue(
		e.Value,
		cf,
		func(value []byte) ([]byte, bool, error) {
			return sr.rewriteDBInfo(value)
		},
	)
	if err != nil || !needWrite {
		return nil, errors.Trace(err)
	}

	newKey, needWrite, err := sr.rewriteKeyForDB(e.Key, cf)
	if err != nil || !needWrite {
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
) ([]byte, bool, error) {
	rawMetaKey, err := ParseTxnMetaKeyFrom(key)
	if err != nil {
		return nil, false, errors.Trace(err)
	}

	dbID, err := meta.ParseDBKey(rawMetaKey.Key)
	if err != nil {
		return nil, false, errors.Trace(err)
	}
	tableID, err := parseField(rawMetaKey.Field)
	if err != nil {
		log.Warn("parse table key failed", zap.ByteString("field", rawMetaKey.Field))
		return nil, false, errors.Trace(err)
	}

	dbReplace, exist := sr.DbMap[dbID]
	if !exist {
		newID, err := sr.genGenGlobalID(context.Background())
		if err != nil {
			return nil, false, errors.Trace(err)
		}
		dbReplace = NewDBReplace(nil, newID)
		sr.DbMap[dbID] = dbReplace
	}

	tableReplace, exist := dbReplace.TableMap[tableID]
	if !exist {
		newID, err := sr.genGenGlobalID(context.Background())
		if err != nil {
			return nil, false, errors.Trace(err)
		}
		tableReplace = NewTableReplace(nil, newID)
		dbReplace.TableMap[tableID] = tableReplace
	}

	rawMetaKey.UpdateKey(meta.DBkey(dbReplace.NewDBID))
	rawMetaKey.UpdateField(encodeField(tableReplace.NewTableID))
	if cf == WriteCF {
		rawMetaKey.UpdateTS(sr.RewriteTS)
	}
	return rawMetaKey.EncodeMetaKey(), true, nil
}

func (sr *SchemasReplace) rewriteTableInfo(value []byte, dbID int64) ([]byte, bool, error) {
	var tableInfo model.TableInfo
	if err := json.Unmarshal(value, &tableInfo); err != nil {
		return nil, false, errors.Trace(err)
	}

	// update table ID
	dbReplace, exist := sr.DbMap[dbID]
	if !exist {
		newID, err := sr.genGenGlobalID(context.Background())
		if err != nil {
			return nil, false, errors.Trace(err)
		}
		dbReplace = NewDBReplace(nil, newID)
		sr.DbMap[dbID] = dbReplace
	}

	tableReplace, exist := dbReplace.TableMap[tableInfo.ID]
	if !exist {
		newID, err := sr.genGenGlobalID(context.Background())
		if err != nil {
			return nil, false, errors.Trace(err)
		}
		tableReplace = NewTableReplace(&tableInfo, newID)
		dbReplace.TableMap[tableInfo.ID] = tableReplace
	} else {
		tableReplace.OldTableInfo = &tableInfo
	}

	log.Debug("rewrite tableInfo", zap.String("table-name", tableInfo.Name.String()),
		zap.Int64("old ID", tableInfo.ID), zap.Int64("new ID", tableReplace.NewTableID))

	newTableInfo := tableInfo.Clone()
	if tableInfo.Partition != nil {
		newTableInfo.Partition = tableInfo.Partition.Clone()
	}
	newTableInfo.ID = tableReplace.NewTableID
	// Do not restore tiflash replica to down-stream.
	//After restore meta finished, restore tiflash replica by DDL.
	newTableInfo.TiFlashReplica = nil

	// update partition table ID
	partitions := newTableInfo.GetPartitionInfo()
	if partitions != nil {
		for i, tbl := range partitions.Definitions {
			newID, exist := tableReplace.PartitionMap[tbl.ID]
			if !exist {
				var err error
				newID, err = sr.genGenGlobalID(context.Background())
				if err != nil {
					return nil, false, errors.Trace(err)
				}
				tableReplace.PartitionMap[tbl.ID] = newID
			}

			log.Debug("update partition",
				zap.String("partition-name", tbl.Name.String()),
				zap.Int64("old-id", tbl.ID), zap.Int64("new-id", newID))
			partitions.Definitions[i].ID = newID
		}
	}

	// marshal to json
	newValue, err := json.Marshal(&newTableInfo)
	if err != nil {
		return nil, false, errors.Trace(err)
	}
	return newValue, true, nil
}

func (sr *SchemasReplace) rewriteEntryForTable(e *kv.Entry, cf string) (*kv.Entry, error) {
	dbID, err := sr.getDBIDFromTableKey(e.Key)
	if err != nil {
		return nil, errors.Trace(err)
	}

	newValue, needWrite, err := sr.rewriteValue(
		e.Value,
		cf,
		func(value []byte) ([]byte, bool, error) {
			return sr.rewriteTableInfo(value, dbID)
		},
	)
	if err != nil || !needWrite {
		return nil, errors.Trace(err)
	}

	newKey, needWrite, err := sr.rewriteKeyForTable(e.Key, cf, meta.ParseTableKey, meta.TableKey)
	if err != nil || !needWrite {
		return nil, errors.Trace(err)
	}

	return &kv.Entry{Key: newKey, Value: newValue}, nil
}

func (sr *SchemasReplace) rewriteEntryForAutoTableIDKey(e *kv.Entry, cf string) (*kv.Entry, error) {
	newKey, needWrite, err := sr.rewriteKeyForTable(
		e.Key,
		cf,
		meta.ParseAutoTableIDKey,
		meta.AutoTableIDKey,
	)
	if err != nil || !needWrite {
		return nil, errors.Trace(err)
	}

	return &kv.Entry{Key: newKey, Value: e.Value}, nil
}

func (sr *SchemasReplace) rewriteEntryForSequenceKey(e *kv.Entry, cf string) (*kv.Entry, error) {
	newKey, needWrite, err := sr.rewriteKeyForTable(
		e.Key,
		cf,
		meta.ParseSequenceKey,
		meta.SequenceKey,
	)
	if err != nil || !needWrite {
		return nil, errors.Trace(err)
	}

	return &kv.Entry{Key: newKey, Value: e.Value}, nil
}

func (sr *SchemasReplace) rewriteEntryForAutoRandomTableIDKey(e *kv.Entry, cf string) (*kv.Entry, error) {
	newKey, needWrite, err := sr.rewriteKeyForTable(
		e.Key,
		cf,
		meta.ParseAutoRandomTableIDKey,
		meta.AutoRandomTableIDKey,
	)
	if err != nil || !needWrite {
		return nil, errors.Trace(err)
	}

	return &kv.Entry{Key: newKey, Value: e.Value}, nil
}

func (sr *SchemasReplace) rewriteValue(
	value []byte,
	cf string,
	cbRewrite func([]byte) ([]byte, bool, error),
) ([]byte, bool, error) {
	switch cf {
	case DefaultCF:
		return cbRewrite(value)
	case WriteCF:
		rawWriteCFValue := new(RawWriteCFValue)
		if err := rawWriteCFValue.ParseFrom(value); err != nil {
			return nil, false, errors.Trace(err)
		}

		if !rawWriteCFValue.HasShortValue() {
			return value, true, nil
		}

		shortValue, needWrite, err := cbRewrite(rawWriteCFValue.GetShortValue())
		if err != nil || !needWrite {
			return nil, needWrite, errors.Trace(err)
		}

		rawWriteCFValue.UpdateShortValue(shortValue)
		return rawWriteCFValue.EncodeTo(), true, nil
	default:
		panic(fmt.Sprintf("not support cf:%s", cf))
	}
}

// RewriteKvEntry uses to rewrite tableID/dbID in entry.key and entry.value
func (sr *SchemasReplace) RewriteKvEntry(e *kv.Entry, cf string) (*kv.Entry, error) {
	// skip mDDLJob

	if !strings.HasPrefix(string(e.Key), "mDB") {
		if cf == DefaultCF && strings.HasPrefix(string(e.Key), "mDDLJobH") { // mDDLJobHistory
			job := &model.Job{}
			if err := job.Decode(e.Value); err != nil {
				log.Debug("failed to decode the job", zap.String("error", err.Error()), zap.String("job", string(e.Value)))
				// The value in write-cf is like "p\XXXX\XXX" need not restore. skip it
				// The value in default-cf that can Decode() need restore.
				return nil, nil
			}

			return nil, sr.tryToGCJob(job)
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

func (sr *SchemasReplace) tryToGCJob(job *model.Job) error {
	if !job.IsCancelled() {
		switch job.Type {
		case model.ActionAddIndex, model.ActionAddPrimaryKey:
			if job.State == model.JobStateRollbackDone {
				return sr.deleteRange(job)
			}
			return nil
		case model.ActionDropSchema, model.ActionDropTable, model.ActionTruncateTable, model.ActionDropIndex, model.ActionDropPrimaryKey,
			model.ActionDropTablePartition, model.ActionTruncateTablePartition, model.ActionDropColumn, model.ActionDropColumns, model.ActionModifyColumn, model.ActionDropIndexes:
			return sr.deleteRange(job)
		case model.ActionMultiSchemaChange:
			for _, sub := range job.MultiSchemaInfo.SubJobs {
				proxyJob := sub.ToProxyJob(job)
				if err := sr.tryToGCJob(&proxyJob); err != nil {
					return err
				}
			}
		case model.ActionExchangeTablePartition:
			return errors.Errorf("restore of ddl `exchange-table-partition` is not supported")
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
