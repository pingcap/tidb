// Copyright 2015 PingCAP, Inc.
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

package ddl

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/parser/model"
	field_types "github.com/pingcap/parser/types"
	"github.com/pingcap/tidb/ddl/util"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util/charset"
)

func onCreateTable(d *ddlCtx, t *meta.Meta, job *model.Job) (ver int64, _ error) {
	schemaID := job.SchemaID
	tbInfo := &model.TableInfo{}
	if err := job.DecodeArgs(tbInfo); err != nil {
		// Invalid arguments, cancel this job.
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	tbInfo.State = model.StateNone
	err := checkTableNotExists(t, job, schemaID, tbInfo.Name.L)
	if err != nil {
		return ver, errors.Trace(err)
	}

	if tbInfo.Partition != nil {
		err = checkAddPartitionTooManyPartitions(uint64(len(tbInfo.Partition.Definitions)))
		if err != nil {
			job.State = model.JobStateCancelled
			return ver, errors.Trace(err)
		}
	}

	ver, err = updateSchemaVersion(t, job)
	if err != nil {
		return ver, errors.Trace(err)
	}

	switch tbInfo.State {
	case model.StateNone:
		// none -> public
		tbInfo.State = model.StatePublic
		tbInfo.UpdateTS = t.StartTS
		err = createTableWithCheck(t, job, schemaID, tbInfo)
		if err != nil {
			return ver, errors.Trace(err)
		}
		// Finish this job.
		job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tbInfo)
		asyncNotifyEvent(d, &util.Event{Tp: model.ActionCreateTable, TableInfo: tbInfo})
		return ver, nil
	default:
		return ver, ErrInvalidTableState.GenWithStack("invalid table state %v", tbInfo.State)
	}
}

func createTableWithCheck(t *meta.Meta, job *model.Job, schemaID int64, tbInfo *model.TableInfo) error {
	err := checkTableInfoValid(tbInfo)
	if err != nil {
		job.State = model.JobStateCancelled
		return errors.Trace(err)
	}
	return t.CreateTable(schemaID, tbInfo)
}

func onDropTable(t *meta.Meta, job *model.Job) (ver int64, _ error) {
	schemaID := job.SchemaID
	tableID := job.TableID
	// Check this table's database.
	tblInfo, err := t.GetTable(schemaID, tableID)
	if err != nil {
		if meta.ErrDBNotExists.Equal(err) {
			job.State = model.JobStateCancelled
			return ver, errors.Trace(infoschema.ErrDatabaseNotExists.GenWithStackByArgs(
				fmt.Sprintf("(Schema ID %d)", schemaID),
			))
		}
		return ver, errors.Trace(err)
	}

	// Check the table.
	if tblInfo == nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(infoschema.ErrTableNotExists.GenWithStackByArgs(
			fmt.Sprintf("(Schema ID %d)", schemaID),
			fmt.Sprintf("(Table ID %d)", tableID),
		))
	}

	if job.IsRollingback() {
		// To simplify the rollback logic, cannot be canceled after job start to run.
		// Normally won't fetch here, because there is check when cancel ddl jobs. see function: isJobRollbackable.
		if tblInfo.State == model.StatePublic {
			job.State = model.JobStateCancelled
			return ver, errCancelledDDLJob
		}
		job.State = model.JobStateRunning
	}

	originalState := job.SchemaState
	switch tblInfo.State {
	case model.StatePublic:
		// public -> write only
		job.SchemaState = model.StateWriteOnly
		tblInfo.State = model.StateWriteOnly
		ver, err = updateVersionAndTableInfo(t, job, tblInfo, originalState != tblInfo.State)
	case model.StateWriteOnly:
		// write only -> delete only
		job.SchemaState = model.StateDeleteOnly
		tblInfo.State = model.StateDeleteOnly
		ver, err = updateVersionAndTableInfo(t, job, tblInfo, originalState != tblInfo.State)
	case model.StateDeleteOnly:
		tblInfo.State = model.StateNone
		ver, err = updateVersionAndTableInfo(t, job, tblInfo, originalState != tblInfo.State)
		if err != nil {
			return ver, errors.Trace(err)
		}
		if err = t.DropTable(job.SchemaID, tableID, true); err != nil {
			break
		}
		// Finish this job.
		job.FinishTableJob(model.JobStateDone, model.StateNone, ver, tblInfo)
		startKey := tablecodec.EncodeTablePrefix(tableID)
		job.Args = append(job.Args, startKey, getPartitionIDs(tblInfo))
	default:
		err = ErrInvalidTableState.GenWithStack("invalid table state %v", tblInfo.State)
	}

	return ver, errors.Trace(err)
}

func getTable(store kv.Storage, schemaID int64, tblInfo *model.TableInfo) (table.Table, error) {
	alloc := autoid.NewAllocator(store, tblInfo.GetDBID(schemaID), tblInfo.IsAutoIncColUnsigned())
	tbl, err := table.TableFromMeta(alloc, tblInfo)
	return tbl, errors.Trace(err)
}

func getTableInfoAndCancelFaultJob(t *meta.Meta, job *model.Job, schemaID int64) (*model.TableInfo, error) {
	tblInfo, err := checkTableExistAndCancelNonExistJob(t, job, schemaID)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if tblInfo.State != model.StatePublic {
		job.State = model.JobStateCancelled
		return nil, ErrInvalidTableState.GenWithStack("table %s is not in public, but %s", tblInfo.Name, tblInfo.State)
	}

	return tblInfo, nil
}

func checkTableExistAndCancelNonExistJob(t *meta.Meta, job *model.Job, schemaID int64) (*model.TableInfo, error) {
	tableID := job.TableID
	// Check this table's database.
	tblInfo, err := t.GetTable(schemaID, tableID)
	if err != nil {
		if meta.ErrDBNotExists.Equal(err) {
			job.State = model.JobStateCancelled
			return nil, errors.Trace(infoschema.ErrDatabaseNotExists.GenWithStackByArgs(
				fmt.Sprintf("(Schema ID %d)", schemaID),
			))
		}
		return nil, errors.Trace(err)
	}

	// Check the table.
	if tblInfo == nil {
		job.State = model.JobStateCancelled
		return nil, errors.Trace(infoschema.ErrTableNotExists.GenWithStackByArgs(
			fmt.Sprintf("(Schema ID %d)", schemaID),
			fmt.Sprintf("(Table ID %d)", tableID),
		))
	}
	return tblInfo, nil
}

func getTableInfo(t *meta.Meta, job *model.Job, schemaID int64) (*model.TableInfo, error) {
	tableID := job.TableID
	tblInfo, err := t.GetTable(schemaID, tableID)
	if err != nil {
		if meta.ErrDBNotExists.Equal(err) {
			job.State = model.JobStateCancelled
			return nil, errors.Trace(infoschema.ErrDatabaseNotExists.GenWithStackByArgs(
				fmt.Sprintf("(Schema ID %d)", schemaID),
			))
		}
		return nil, errors.Trace(err)
	} else if tblInfo == nil {
		job.State = model.JobStateCancelled
		return nil, errors.Trace(infoschema.ErrTableNotExists.GenWithStackByArgs(
			fmt.Sprintf("(Schema ID %d)", schemaID),
			fmt.Sprintf("(Table ID %d)", tableID),
		))
	}

	if tblInfo.State != model.StatePublic {
		job.State = model.JobStateCancelled
		return nil, ErrInvalidTableState.GenWithStack("table %s is not in public, but %s", tblInfo.Name, tblInfo.State)
	}

	return tblInfo, nil
}

// onTruncateTable delete old table meta, and creates a new table identical to old table except for table ID.
// As all the old data is encoded with old table ID, it can not be accessed any more.
// A background job will be created to delete old data.
func onTruncateTable(d *ddlCtx, t *meta.Meta, job *model.Job) (ver int64, _ error) {
	schemaID := job.SchemaID
	tableID := job.TableID
	var newTableID int64
	err := job.DecodeArgs(&newTableID)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	tblInfo, err := getTableInfo(t, job, schemaID)
	if err != nil {
		return ver, errors.Trace(err)
	}

	err = t.DropTable(schemaID, tblInfo.ID, true)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	failpoint.Inject("truncateTableErr", func(val failpoint.Value) {
		if val.(bool) {
			job.State = model.JobStateCancelled
			failpoint.Return(ver, errors.New("occur an error after dropping table"))
		}
	})

	var oldPartitionIDs []int64
	if tblInfo.GetPartitionInfo() != nil {
		oldPartitionIDs = getPartitionIDs(tblInfo)
		// We use the new partition ID because all the old data is encoded with the old partition ID, it can not be accessed anymore.
		err = truncateTableByReassignPartitionIDs(t, tblInfo)
		if err != nil {
			job.State = model.JobStateCancelled
			return ver, errors.Trace(err)
		}
	}

	tblInfo.ID = newTableID
	err = t.CreateTable(schemaID, tblInfo)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	ver, err = updateSchemaVersion(t, job)
	if err != nil {
		return ver, errors.Trace(err)
	}
	job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tblInfo)
	asyncNotifyEvent(d, &util.Event{Tp: model.ActionTruncateTable, TableInfo: tblInfo})
	startKey := tablecodec.EncodeTablePrefix(tableID)
	job.Args = []interface{}{startKey, oldPartitionIDs}
	return ver, nil
}

func onRebaseAutoID(store kv.Storage, t *meta.Meta, job *model.Job) (ver int64, _ error) {
	schemaID := job.SchemaID
	var newBase int64
	err := job.DecodeArgs(&newBase)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	tblInfo, err := getTableInfo(t, job, schemaID)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	tblInfo.AutoIncID = newBase
	tbl, err := getTable(store, schemaID, tblInfo)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	// The operation of the minus 1 to make sure that the current value doesn't be used,
	// the next Alloc operation will get this value.
	// Its behavior is consistent with MySQL.
	err = tbl.RebaseAutoID(nil, tblInfo.AutoIncID-1, false)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	ver, err = updateVersionAndTableInfo(t, job, tblInfo, true)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tblInfo)
	return ver, nil
}

func onModifyTableAutoIDCache(t *meta.Meta, job *model.Job) (int64, error) {
	var cache int64
	if err := job.DecodeArgs(&cache); err != nil {
		job.State = model.JobStateCancelled
		return 0, errors.Trace(err)
	}

	tblInfo, err := getTableInfoAndCancelFaultJob(t, job, job.SchemaID)
	if err != nil {
		return 0, errors.Trace(err)
	}

	tblInfo.AutoIdCache = cache
	ver, err := updateVersionAndTableInfo(t, job, tblInfo, true)
	if err != nil {
		return ver, errors.Trace(err)
	}
	job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tblInfo)
	return ver, nil
}

func (w *worker) onShardRowID(d *ddlCtx, t *meta.Meta, job *model.Job) (ver int64, _ error) {
	var shardRowIDBits uint64
	err := job.DecodeArgs(&shardRowIDBits)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	tblInfo, err := getTableInfo(t, job, job.SchemaID)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	if shardRowIDBits < tblInfo.ShardRowIDBits {
		tblInfo.ShardRowIDBits = shardRowIDBits
	} else {
		tbl, err := getTable(d.store, job.SchemaID, tblInfo)
		if err != nil {
			return ver, errors.Trace(err)
		}
		err = verifyNoOverflowShardBits(w.sessPool, tbl, shardRowIDBits)
		if err != nil {
			job.State = model.JobStateCancelled
			return ver, err
		}
		tblInfo.ShardRowIDBits = shardRowIDBits
		// MaxShardRowIDBits use to check the overflow of auto ID.
		tblInfo.MaxShardRowIDBits = shardRowIDBits
	}
	ver, err = updateVersionAndTableInfo(t, job, tblInfo, true)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tblInfo)
	return ver, nil
}

func verifyNoOverflowShardBits(s *sessionPool, tbl table.Table, shardRowIDBits uint64) error {
	ctx, err := s.get()
	if err != nil {
		return errors.Trace(err)
	}
	defer s.put(ctx)

	// Check next global max auto ID first.
	autoIncID, err := tbl.Allocator(ctx).NextGlobalAutoID(tbl.Meta().ID)
	if err != nil {
		return errors.Trace(err)
	}
	if tables.OverflowShardBits(autoIncID, shardRowIDBits) {
		return autoid.ErrAutoincReadFailed.GenWithStack("shard_row_id_bits %d will cause next global auto ID overflow", shardRowIDBits)
	}
	return nil
}

func onRenameTable(t *meta.Meta, job *model.Job) (ver int64, _ error) {
	var oldSchemaID int64
	var tableName model.CIStr
	if err := job.DecodeArgs(&oldSchemaID, &tableName); err != nil {
		// Invalid arguments, cancel this job.
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	tblInfo, err := getTableInfo(t, job, oldSchemaID)
	if err != nil {
		return ver, errors.Trace(err)
	}
	newSchemaID := job.SchemaID
	err = checkTableNotExists(t, job, newSchemaID, tableName.L)
	if err != nil {
		return ver, errors.Trace(err)
	}

	var baseID int64
	shouldDelAutoID := false
	if newSchemaID != oldSchemaID {
		shouldDelAutoID = true
		baseID, err = t.GetAutoTableID(tblInfo.GetDBID(oldSchemaID), tblInfo.ID)
		if err != nil {
			job.State = model.JobStateCancelled
			return ver, errors.Trace(err)
		}
		// It's compatible with old version.
		// TODO: Remove it.
		tblInfo.OldSchemaID = 0
	}

	err = t.DropTable(oldSchemaID, tblInfo.ID, shouldDelAutoID)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	failpoint.Inject("renameTableErr", func(val failpoint.Value) {
		if val.(bool) {
			job.State = model.JobStateCancelled
			failpoint.Return(ver, errors.New("occur an error after renaming table"))
		}
	})

	tblInfo.Name = tableName
	err = t.CreateTable(newSchemaID, tblInfo)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	// Update the table's auto-increment ID.
	if newSchemaID != oldSchemaID {
		_, err = t.GenAutoTableID(newSchemaID, tblInfo.ID, baseID)
		if err != nil {
			job.State = model.JobStateCancelled
			return ver, errors.Trace(err)
		}
	}

	ver, err = updateSchemaVersion(t, job)
	if err != nil {
		return ver, errors.Trace(err)
	}
	job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tblInfo)
	return ver, nil
}

func onModifyTableComment(t *meta.Meta, job *model.Job) (ver int64, _ error) {
	var comment string
	if err := job.DecodeArgs(&comment); err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	tblInfo, err := getTableInfo(t, job, job.SchemaID)
	if err != nil {
		return ver, errors.Trace(err)
	}

	tblInfo.Comment = comment
	ver, err = updateVersionAndTableInfo(t, job, tblInfo, true)
	if err != nil {
		return ver, errors.Trace(err)
	}
	job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tblInfo)
	return ver, nil
}

func onModifyTableCharsetAndCollate(t *meta.Meta, job *model.Job) (ver int64, _ error) {
	var toCharset, toCollate string
	if err := job.DecodeArgs(&toCharset, &toCollate); err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	dbInfo, err := checkSchemaExistAndCancelNotExistJob(t, job)
	if err != nil {
		return ver, errors.Trace(err)
	}

	tblInfo, err := getTableInfo(t, job, job.SchemaID)
	if err != nil {
		return ver, errors.Trace(err)
	}

	// double check.
	_, err = checkAlterTableCharset(tblInfo, dbInfo, toCharset, toCollate)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	// update column charset.
	for _, col := range tblInfo.Columns {
		if field_types.HasCharset(&col.FieldType) {
			col.Charset = toCharset
			col.Collate = toCollate
		} else {
			col.Charset = charset.CharsetBin
			col.Collate = charset.CharsetBin
		}
	}

	tblInfo.Charset = toCharset
	tblInfo.Collate = toCollate
	ver, err = updateVersionAndTableInfo(t, job, tblInfo, true)
	if err != nil {
		return ver, errors.Trace(err)
	}
	job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tblInfo)
	return ver, nil
}

func checkTableNotExists(t *meta.Meta, job *model.Job, schemaID int64, tableName string) error {
	// Check this table's database.
	tables, err := t.ListTables(schemaID)
	if err != nil {
		if meta.ErrDBNotExists.Equal(err) {
			job.State = model.JobStateCancelled
			return infoschema.ErrDatabaseNotExists.GenWithStackByArgs("")
		}
		return errors.Trace(err)
	}

	// Check the table.
	for _, tbl := range tables {
		if tbl.Name.L == tableName {
			// This table already exists and can't be created, we should cancel this job now.
			job.State = model.JobStateCancelled
			return infoschema.ErrTableExists.GenWithStackByArgs(tbl.Name)
		}
	}

	return nil
}

// updateVersionAndTableInfoWithCheck checks table info validate and updates the schema version and the table information
func updateVersionAndTableInfoWithCheck(t *meta.Meta, job *model.Job, tblInfo *model.TableInfo, shouldUpdateVer bool) (
	ver int64, err error) {
	err = checkTableInfoValid(tblInfo)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	return updateVersionAndTableInfo(t, job, tblInfo, shouldUpdateVer)
}

// updateVersionAndTableInfo updates the schema version and the table information.
func updateVersionAndTableInfo(t *meta.Meta, job *model.Job, tblInfo *model.TableInfo, shouldUpdateVer bool) (
	ver int64, err error) {
	if shouldUpdateVer {
		ver, err = updateSchemaVersion(t, job)
		if err != nil {
			return 0, errors.Trace(err)
		}
	}

	if tblInfo.State == model.StatePublic {
		tblInfo.UpdateTS = t.StartTS
	}
	return ver, t.UpdateTable(job.SchemaID, tblInfo)
}

// TODO: It may have the issue when two clients concurrently add partitions to a table.
func onAddTablePartition(t *meta.Meta, job *model.Job) (ver int64, _ error) {
	partInfo := &model.PartitionInfo{}
	err := job.DecodeArgs(&partInfo)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	tblInfo, err := getTableInfo(t, job, job.SchemaID)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	err = checkAddPartitionTooManyPartitions(uint64(len(tblInfo.Partition.Definitions) + len(partInfo.Definitions)))
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	err = checkAddPartitionValue(tblInfo, partInfo)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	err = checkPartitionNameUnique(tblInfo, partInfo)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	updatePartitionInfo(partInfo, tblInfo)
	ver, err = updateVersionAndTableInfo(t, job, tblInfo, true)
	if err != nil {
		return ver, errors.Trace(err)
	}
	// Finish this job.
	job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tblInfo)
	return ver, errors.Trace(err)
}

func updatePartitionInfo(partitionInfo *model.PartitionInfo, tblInfo *model.TableInfo) {
	parInfo := &model.PartitionInfo{}
	oldDefs, newDefs := tblInfo.Partition.Definitions, partitionInfo.Definitions
	parInfo.Definitions = make([]model.PartitionDefinition, 0, len(newDefs)+len(oldDefs))
	parInfo.Definitions = append(parInfo.Definitions, oldDefs...)
	parInfo.Definitions = append(parInfo.Definitions, newDefs...)
	tblInfo.Partition.Definitions = parInfo.Definitions
}

// checkAddPartitionValue values less than value must be strictly increasing for each partition.
func checkAddPartitionValue(meta *model.TableInfo, part *model.PartitionInfo) error {
	if meta.Partition.Type == model.PartitionTypeRange {
		newDefs, oldDefs := part.Definitions, meta.Partition.Definitions
		rangeValue := oldDefs[len(oldDefs)-1].LessThan[0]
		if strings.EqualFold(rangeValue, "MAXVALUE") {
			return errors.Trace(ErrPartitionMaxvalue)
		}

		currentRangeValue, err := strconv.Atoi(rangeValue)
		if err != nil {
			return errors.Trace(err)
		}

		for i := 0; i < len(newDefs); i++ {
			ifMaxvalue := strings.EqualFold(newDefs[i].LessThan[0], "MAXVALUE")
			if ifMaxvalue && i == len(newDefs)-1 {
				return nil
			} else if ifMaxvalue && i != len(newDefs)-1 {
				return errors.Trace(ErrPartitionMaxvalue)
			}

			nextRangeValue, err := strconv.Atoi(newDefs[i].LessThan[0])
			if err != nil {
				return errors.Trace(err)
			}
			if nextRangeValue <= currentRangeValue {
				return errors.Trace(ErrRangeNotIncreasing)
			}
			currentRangeValue = nextRangeValue
		}
	}
	return nil
}
