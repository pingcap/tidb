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

package model

import (
	"encoding/json"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/terror"
)

// ActionType is the type for DDL action.
type ActionType byte

// List DDL actions.
const (
	ActionNone                          ActionType = 0
	ActionCreateSchema                  ActionType = 1
	ActionDropSchema                    ActionType = 2
	ActionCreateTable                   ActionType = 3
	ActionDropTable                     ActionType = 4
	ActionAddColumn                     ActionType = 5
	ActionDropColumn                    ActionType = 6
	ActionAddIndex                      ActionType = 7
	ActionDropIndex                     ActionType = 8
	ActionAddForeignKey                 ActionType = 9
	ActionDropForeignKey                ActionType = 10
	ActionTruncateTable                 ActionType = 11
	ActionModifyColumn                  ActionType = 12
	ActionRebaseAutoID                  ActionType = 13
	ActionRenameTable                   ActionType = 14
	ActionSetDefaultValue               ActionType = 15
	ActionShardRowID                    ActionType = 16
	ActionModifyTableComment            ActionType = 17
	ActionRenameIndex                   ActionType = 18
	ActionAddTablePartition             ActionType = 19
	ActionDropTablePartition            ActionType = 20
	ActionCreateView                    ActionType = 21
	ActionModifyTableCharsetAndCollate  ActionType = 22
	ActionTruncateTablePartition        ActionType = 23
	ActionDropView                      ActionType = 24
	ActionRecoverTable                  ActionType = 25
	ActionModifySchemaCharsetAndCollate ActionType = 26
	ActionLockTable                     ActionType = 27
	ActionUnlockTable                   ActionType = 28
	ActionRepairTable                   ActionType = 29
	ActionSetTiFlashReplica             ActionType = 30
	ActionUpdateTiFlashReplicaStatus    ActionType = 31
	ActionAddPrimaryKey                 ActionType = 32
	ActionDropPrimaryKey                ActionType = 33
	ActionCreateSequence                ActionType = 34
	ActionAlterSequence                 ActionType = 35
	ActionDropSequence                  ActionType = 36
	ActionAddColumns                    ActionType = 37 // Deprecated, we use ActionMultiSchemaChange instead.
	ActionDropColumns                   ActionType = 38 // Deprecated, we use ActionMultiSchemaChange instead.
	ActionModifyTableAutoIdCache        ActionType = 39 //nolint:revive
	ActionRebaseAutoRandomBase          ActionType = 40
	ActionAlterIndexVisibility          ActionType = 41
	ActionExchangeTablePartition        ActionType = 42
	ActionAddCheckConstraint            ActionType = 43
	ActionDropCheckConstraint           ActionType = 44
	ActionAlterCheckConstraint          ActionType = 45

	// `ActionAlterTableAlterPartition` is removed and will never be used.
	// Just left a tombstone here for compatibility.
	__DEPRECATED_ActionAlterTableAlterPartition ActionType = 46 //nolint:revive

	ActionRenameTables                  ActionType = 47
	ActionDropIndexes                   ActionType = 48 // Deprecated, we use ActionMultiSchemaChange instead.
	ActionAlterTableAttributes          ActionType = 49
	ActionAlterTablePartitionAttributes ActionType = 50
	ActionCreatePlacementPolicy         ActionType = 51
	ActionAlterPlacementPolicy          ActionType = 52
	ActionDropPlacementPolicy           ActionType = 53
	ActionAlterTablePartitionPlacement  ActionType = 54
	ActionModifySchemaDefaultPlacement  ActionType = 55
	ActionAlterTablePlacement           ActionType = 56
	ActionAlterCacheTable               ActionType = 57
	ActionAlterTableStatsOptions        ActionType = 58
	ActionAlterNoCacheTable             ActionType = 59
	ActionCreateTables                  ActionType = 60
	ActionMultiSchemaChange             ActionType = 61
	ActionFlashbackCluster              ActionType = 62
	ActionRecoverSchema                 ActionType = 63
	ActionReorganizePartition           ActionType = 64
	ActionAlterTTLInfo                  ActionType = 65
	ActionAlterTTLRemove                ActionType = 67
	ActionCreateResourceGroup           ActionType = 68
	ActionAlterResourceGroup            ActionType = 69
	ActionDropResourceGroup             ActionType = 70
)

var actionMap = map[ActionType]string{
	ActionCreateSchema:                  "create schema",
	ActionDropSchema:                    "drop schema",
	ActionCreateTable:                   "create table",
	ActionCreateTables:                  "create tables",
	ActionDropTable:                     "drop table",
	ActionAddColumn:                     "add column",
	ActionDropColumn:                    "drop column",
	ActionAddIndex:                      "add index",
	ActionDropIndex:                     "drop index",
	ActionAddForeignKey:                 "add foreign key",
	ActionDropForeignKey:                "drop foreign key",
	ActionTruncateTable:                 "truncate table",
	ActionModifyColumn:                  "modify column",
	ActionRebaseAutoID:                  "rebase auto_increment ID",
	ActionRenameTable:                   "rename table",
	ActionRenameTables:                  "rename tables",
	ActionSetDefaultValue:               "set default value",
	ActionShardRowID:                    "shard row ID",
	ActionModifyTableComment:            "modify table comment",
	ActionRenameIndex:                   "rename index",
	ActionAddTablePartition:             "add partition",
	ActionDropTablePartition:            "drop partition",
	ActionCreateView:                    "create view",
	ActionModifyTableCharsetAndCollate:  "modify table charset and collate",
	ActionTruncateTablePartition:        "truncate partition",
	ActionDropView:                      "drop view",
	ActionRecoverTable:                  "recover table",
	ActionModifySchemaCharsetAndCollate: "modify schema charset and collate",
	ActionLockTable:                     "lock table",
	ActionUnlockTable:                   "unlock table",
	ActionRepairTable:                   "repair table",
	ActionSetTiFlashReplica:             "set tiflash replica",
	ActionUpdateTiFlashReplicaStatus:    "update tiflash replica status",
	ActionAddPrimaryKey:                 "add primary key",
	ActionDropPrimaryKey:                "drop primary key",
	ActionCreateSequence:                "create sequence",
	ActionAlterSequence:                 "alter sequence",
	ActionDropSequence:                  "drop sequence",
	ActionModifyTableAutoIdCache:        "modify auto id cache",
	ActionRebaseAutoRandomBase:          "rebase auto_random ID",
	ActionAlterIndexVisibility:          "alter index visibility",
	ActionExchangeTablePartition:        "exchange partition",
	ActionAddCheckConstraint:            "add check constraint",
	ActionDropCheckConstraint:           "drop check constraint",
	ActionAlterCheckConstraint:          "alter check constraint",
	ActionAlterTableAttributes:          "alter table attributes",
	ActionAlterTablePartitionPlacement:  "alter table partition placement",
	ActionAlterTablePartitionAttributes: "alter table partition attributes",
	ActionCreatePlacementPolicy:         "create placement policy",
	ActionAlterPlacementPolicy:          "alter placement policy",
	ActionDropPlacementPolicy:           "drop placement policy",
	ActionModifySchemaDefaultPlacement:  "modify schema default placement",
	ActionAlterTablePlacement:           "alter table placement",
	ActionAlterCacheTable:               "alter table cache",
	ActionAlterNoCacheTable:             "alter table nocache",
	ActionAlterTableStatsOptions:        "alter table statistics options",
	ActionMultiSchemaChange:             "alter table multi-schema change",
	ActionFlashbackCluster:              "flashback cluster",
	ActionRecoverSchema:                 "flashback schema",
	ActionReorganizePartition:           "alter table reorganize partition",
	ActionAlterTTLInfo:                  "alter table ttl",
	ActionAlterTTLRemove:                "alter table no_ttl",
	ActionCreateResourceGroup:           "create resource group",
	ActionAlterResourceGroup:            "alter resource group",
	ActionDropResourceGroup:             "drop resource group",

	// `ActionAlterTableAlterPartition` is removed and will never be used.
	// Just left a tombstone here for compatibility.
	__DEPRECATED_ActionAlterTableAlterPartition: "alter partition",
}

// String return current ddl action in string
func (action ActionType) String() string {
	if v, ok := actionMap[action]; ok {
		return v
	}
	return "none"
}

// HistoryInfo is used for binlog.
type HistoryInfo struct {
	SchemaVersion int64
	DBInfo        *DBInfo
	TableInfo     *TableInfo
	FinishedTS    uint64

	// MultipleTableInfos is like TableInfo but only for operations updating multiple tables.
	MultipleTableInfos []*TableInfo
}

// AddDBInfo adds schema version and schema information that are used for binlog.
// dbInfo is added in the following operations: create database, drop database.
func (h *HistoryInfo) AddDBInfo(schemaVer int64, dbInfo *DBInfo) {
	h.SchemaVersion = schemaVer
	h.DBInfo = dbInfo
}

// AddTableInfo adds schema version and table information that are used for binlog.
// tblInfo is added except for the following operations: create database, drop database.
func (h *HistoryInfo) AddTableInfo(schemaVer int64, tblInfo *TableInfo) {
	h.SchemaVersion = schemaVer
	h.TableInfo = tblInfo
}

// SetTableInfos is like AddTableInfo, but will add multiple table infos to the binlog.
func (h *HistoryInfo) SetTableInfos(schemaVer int64, tblInfos []*TableInfo) {
	h.SchemaVersion = schemaVer
	h.MultipleTableInfos = make([]*TableInfo, len(tblInfos))
	copy(h.MultipleTableInfos, tblInfos)
}

// Clean cleans history information.
func (h *HistoryInfo) Clean() {
	h.SchemaVersion = 0
	h.DBInfo = nil
	h.TableInfo = nil
	h.MultipleTableInfos = nil
}

// DDLReorgMeta is meta info of DDL reorganization.
type DDLReorgMeta struct {
	// EndHandle is the last handle of the adding indices table.
	// We should only backfill indices in the range [startHandle, EndHandle].
	EndHandle int64 `json:"end_handle"`

	SQLMode       mysql.SQLMode                    `json:"sql_mode"`
	Warnings      map[errors.ErrorID]*terror.Error `json:"warnings"`
	WarningsCount map[errors.ErrorID]int64         `json:"warnings_count"`
	Location      *TimeZoneLocation                `json:"location"`
	ReorgTp       ReorgType                        `json:"reorg_tp"`
	IsDistReorg   bool                             `json:"is_dist_reorg"`
}

// ReorgType indicates which process is used for the data reorganization.
type ReorgType int8

const (
	// ReorgTypeNone means the backfill task is not started yet.
	ReorgTypeNone ReorgType = iota
	// ReorgTypeTxn means the index records are backfill with transactions.
	// All the index KVs are written through the transaction interface.
	// This is the original backfill implementation.
	ReorgTypeTxn
	// ReorgTypeLitMerge means the index records are backfill with lightning.
	// The index KVs are encoded to SST files and imported to the storage directly.
	// The incremental index KVs written by DML are redirected to a temporary index.
	// After the backfill is finished, the temporary index records are merged back to the original index.
	ReorgTypeLitMerge
	// ReorgTypeTxnMerge means backfill with transactions and merge incremental changes.
	// The backfill index KVs are written through the transaction interface.
	// The incremental index KVs written by DML are redirected to a temporary index.
	// After the backfill is finished, the temporary index records are merged back to the original index.
	ReorgTypeTxnMerge
)

// NeedMergeProcess means the incremental changes need to be merged.
func (tp ReorgType) NeedMergeProcess() bool {
	return tp == ReorgTypeLitMerge || tp == ReorgTypeTxnMerge
}

// String implements fmt.Stringer interface.
func (tp ReorgType) String() string {
	switch tp {
	case ReorgTypeTxn:
		return "txn"
	case ReorgTypeLitMerge:
		return "ingest"
	case ReorgTypeTxnMerge:
		return "txn-merge"
	}
	return ""
}

// TimeZoneLocation represents a single time zone.
type TimeZoneLocation struct {
	Name     string `json:"name"`
	Offset   int    `json:"offset"` // seconds east of UTC
	location *time.Location
}

// GetLocation gets the timezone location.
func (tz *TimeZoneLocation) GetLocation() (*time.Location, error) {
	if tz.location != nil {
		return tz.location, nil
	}

	var err error
	if tz.Offset == 0 {
		tz.location, err = time.LoadLocation(tz.Name)
	} else {
		tz.location = time.FixedZone(tz.Name, tz.Offset)
	}
	return tz.location, err
}

// NewDDLReorgMeta new a DDLReorgMeta.
func NewDDLReorgMeta() *DDLReorgMeta {
	return &DDLReorgMeta{
		EndHandle: math.MaxInt64,
	}
}

// MultiSchemaInfo keeps some information for multi schema change.
type MultiSchemaInfo struct {
	SubJobs    []*SubJob `json:"sub_jobs"`
	Revertible bool      `json:"revertible"`

	// SkipVersion is used to control whether generating a new schema version for a sub-job.
	SkipVersion bool `json:"-"`

	AddColumns    []CIStr `json:"-"`
	DropColumns   []CIStr `json:"-"`
	ModifyColumns []CIStr `json:"-"`
	AddIndexes    []CIStr `json:"-"`
	DropIndexes   []CIStr `json:"-"`
	AlterIndexes  []CIStr `json:"-"`

	AddForeignKeys []AddForeignKeyInfo `json:"-"`

	RelativeColumns []CIStr `json:"-"`
	PositionColumns []CIStr `json:"-"`
}

// AddForeignKeyInfo contains foreign key information.
type AddForeignKeyInfo struct {
	Name CIStr
	Cols []CIStr
}

// NewMultiSchemaInfo new a MultiSchemaInfo.
func NewMultiSchemaInfo() *MultiSchemaInfo {
	return &MultiSchemaInfo{
		SubJobs:    nil,
		Revertible: true,
	}
}

// SubJob is a representation of one DDL schema change. A Job may contain zero(when multi-schema change is not applicable) or more SubJobs.
type SubJob struct {
	Type        ActionType      `json:"type"`
	Args        []interface{}   `json:"-"`
	RawArgs     json.RawMessage `json:"raw_args"`
	SchemaState SchemaState     `json:"schema_state"`
	SnapshotVer uint64          `json:"snapshot_ver"`
	Revertible  bool            `json:"revertible"`
	State       JobState        `json:"state"`
	RowCount    int64           `json:"row_count"`
	Warning     *terror.Error   `json:"warning"`
	CtxVars     []interface{}   `json:"-"`
	SchemaVer   int64           `json:"schema_version"`
	ReorgTp     ReorgType       `json:"reorg_tp"`
}

// IsNormal returns true if the sub-job is normally running.
func (sub *SubJob) IsNormal() bool {
	switch sub.State {
	case JobStateCancelling, JobStateCancelled,
		JobStateRollingback, JobStateRollbackDone:
		return false
	default:
		return true
	}
}

// IsFinished returns true if the job is done.
func (sub *SubJob) IsFinished() bool {
	return sub.State == JobStateDone ||
		sub.State == JobStateRollbackDone ||
		sub.State == JobStateCancelled
}

// ToProxyJob converts a sub-job to a proxy job.
func (sub *SubJob) ToProxyJob(parentJob *Job) Job {
	return Job{
		ID:              parentJob.ID,
		Type:            sub.Type,
		SchemaID:        parentJob.SchemaID,
		TableID:         parentJob.TableID,
		SchemaName:      parentJob.SchemaName,
		State:           sub.State,
		Warning:         sub.Warning,
		Error:           nil,
		ErrorCount:      0,
		RowCount:        sub.RowCount,
		Mu:              sync.Mutex{},
		CtxVars:         sub.CtxVars,
		Args:            sub.Args,
		RawArgs:         sub.RawArgs,
		SchemaState:     sub.SchemaState,
		SnapshotVer:     sub.SnapshotVer,
		RealStartTS:     parentJob.RealStartTS,
		StartTS:         parentJob.StartTS,
		DependencyID:    parentJob.DependencyID,
		Query:           parentJob.Query,
		BinlogInfo:      parentJob.BinlogInfo,
		Version:         parentJob.Version,
		ReorgMeta:       parentJob.ReorgMeta,
		MultiSchemaInfo: &MultiSchemaInfo{Revertible: sub.Revertible},
		Priority:        parentJob.Priority,
		SeqNum:          parentJob.SeqNum,
	}
}

// FromProxyJob converts a proxy job to a sub-job.
func (sub *SubJob) FromProxyJob(proxyJob *Job, ver int64) {
	sub.Revertible = proxyJob.MultiSchemaInfo.Revertible
	sub.SchemaState = proxyJob.SchemaState
	sub.SnapshotVer = proxyJob.SnapshotVer
	sub.Args = proxyJob.Args
	sub.State = proxyJob.State
	sub.Warning = proxyJob.Warning
	sub.RowCount = proxyJob.RowCount
	sub.SchemaVer = ver
	sub.ReorgTp = proxyJob.ReorgMeta.ReorgTp
}

// JobMeta is meta info of Job.
type JobMeta struct {
	SchemaID int64 `json:"schema_id"`
	TableID  int64 `json:"table_id"`
	// Type is the DDL job's type.
	Type ActionType `json:"job_type"`
	// Query is the DDL job's SQL string.
	Query string `json:"query"`
	// Priority is only used to set the operation priority of adding indices.
	Priority int `json:"priority"`
}

// BackfillMeta is meta info of the backfill job.
type BackfillMeta struct {
	PhysicalTableID int64         `json:"physical_table_id"`
	IsUnique        bool          `json:"is_unique"`
	EndInclude      bool          `json:"end_include"`
	Error           *terror.Error `json:"err"`

	SQLMode       mysql.SQLMode                    `json:"sql_mode"`
	Warnings      map[errors.ErrorID]*terror.Error `json:"warnings"`
	WarningsCount map[errors.ErrorID]int64         `json:"warnings_count"`
	Location      *TimeZoneLocation                `json:"location"`
	ReorgTp       ReorgType                        `json:"reorg_tp"`

	*JobMeta `json:"job_meta"`
}

// Encode encodes BackfillMeta with json format.
func (bm *BackfillMeta) Encode() ([]byte, error) {
	b, err := json.Marshal(bm)
	return b, errors.Trace(err)
}

// Decode decodes BackfillMeta from the json buffer.
func (bm *BackfillMeta) Decode(b []byte) error {
	err := json.Unmarshal(b, bm)
	return errors.Trace(err)
}

// Job is for a DDL operation.
type Job struct {
	ID         int64         `json:"id"`
	Type       ActionType    `json:"type"`
	SchemaID   int64         `json:"schema_id"`
	TableID    int64         `json:"table_id"`
	SchemaName string        `json:"schema_name"`
	TableName  string        `json:"table_name"`
	State      JobState      `json:"state"`
	Warning    *terror.Error `json:"warning"`
	Error      *terror.Error `json:"err"`
	// ErrorCount will be increased, every time we meet an error when running job.
	ErrorCount int64 `json:"err_count"`
	// RowCount means the number of rows that are processed.
	RowCount int64      `json:"row_count"`
	Mu       sync.Mutex `json:"-"`
	// CtxVars are variables attached to the job. It is for internal usage.
	// E.g. passing arguments between functions by one single *Job pointer.
	CtxVars []interface{} `json:"-"`
	Args    []interface{} `json:"-"`
	// RawArgs : We must use json raw message to delay parsing special args.
	RawArgs     json.RawMessage `json:"raw_args"`
	SchemaState SchemaState     `json:"schema_state"`
	// SnapshotVer means snapshot version for this job.
	SnapshotVer uint64 `json:"snapshot_ver"`
	// RealStartTS uses timestamp allocated by TSO.
	// Now it's the TS when we actually start the job.
	RealStartTS uint64 `json:"real_start_ts"`
	// StartTS uses timestamp allocated by TSO.
	// Now it's the TS when we put the job to TiKV queue.
	StartTS uint64 `json:"start_ts"`
	// DependencyID is the job's ID that the current job depends on.
	DependencyID int64 `json:"dependency_id"`
	// Query string of the ddl job.
	Query      string       `json:"query"`
	BinlogInfo *HistoryInfo `json:"binlog"`

	// Version indicates the DDL job version. For old jobs, it will be 0.
	Version int64 `json:"version"`

	// ReorgMeta is meta info of ddl reorganization.
	ReorgMeta *DDLReorgMeta `json:"reorg_meta"`

	// MultiSchemaInfo keeps some warning now for multi schema change.
	MultiSchemaInfo *MultiSchemaInfo `json:"multi_schema_info"`

	// Priority is only used to set the operation priority of adding indices.
	Priority int `json:"priority"`

	// SeqNum is the total order in all DDLs, it's used to identify the order of DDL.
	SeqNum uint64 `json:"seq_num"`
}

// FinishTableJob is called when a job is finished.
// It updates the job's state information and adds tblInfo to the binlog.
func (job *Job) FinishTableJob(jobState JobState, schemaState SchemaState, ver int64, tblInfo *TableInfo) {
	job.State = jobState
	job.SchemaState = schemaState
	job.BinlogInfo.AddTableInfo(ver, tblInfo)
}

// FinishMultipleTableJob is called when a job is finished.
// It updates the job's state information and adds tblInfos to the binlog.
func (job *Job) FinishMultipleTableJob(jobState JobState, schemaState SchemaState, ver int64, tblInfos []*TableInfo) {
	job.State = jobState
	job.SchemaState = schemaState
	job.BinlogInfo.SchemaVersion = ver
	job.BinlogInfo.MultipleTableInfos = tblInfos
	job.BinlogInfo.TableInfo = tblInfos[len(tblInfos)-1]
}

// FinishDBJob is called when a job is finished.
// It updates the job's state information and adds dbInfo the binlog.
func (job *Job) FinishDBJob(jobState JobState, schemaState SchemaState, ver int64, dbInfo *DBInfo) {
	job.State = jobState
	job.SchemaState = schemaState
	job.BinlogInfo.AddDBInfo(ver, dbInfo)
}

// MarkNonRevertible mark the current job to be non-revertible.
// It means the job cannot be cancelled or rollbacked.
func (job *Job) MarkNonRevertible() {
	if job.MultiSchemaInfo != nil {
		job.MultiSchemaInfo.Revertible = false
	}
}

// Clone returns a copy of the job.
func (job *Job) Clone() *Job {
	encode, err := job.Encode(true)
	if err != nil {
		return nil
	}
	var clone Job
	err = clone.Decode(encode)
	if err != nil {
		return nil
	}
	if len(job.Args) > 0 {
		clone.Args = make([]interface{}, len(job.Args))
		copy(clone.Args, job.Args)
	}
	if job.MultiSchemaInfo != nil {
		for i, sub := range job.MultiSchemaInfo.SubJobs {
			clone.MultiSchemaInfo.SubJobs[i].Args = make([]interface{}, len(sub.Args))
			copy(clone.MultiSchemaInfo.SubJobs[i].Args, sub.Args)
		}
	}
	return &clone
}

// TSConvert2Time converts timestamp to time.
func TSConvert2Time(ts uint64) time.Time {
	t := int64(ts >> 18) // 18 is for the logical time.
	return time.UnixMilli(t)
}

// SetRowCount sets the number of rows. Make sure it can pass `make race`.
func (job *Job) SetRowCount(count int64) {
	job.Mu.Lock()
	defer job.Mu.Unlock()

	job.RowCount = count
}

// GetRowCount gets the number of rows. Make sure it can pass `make race`.
func (job *Job) GetRowCount() int64 {
	job.Mu.Lock()
	defer job.Mu.Unlock()

	return job.RowCount
}

// SetWarnings sets the warnings of rows handled.
func (job *Job) SetWarnings(warnings map[errors.ErrorID]*terror.Error, warningsCount map[errors.ErrorID]int64) {
	job.ReorgMeta.Warnings = warnings
	job.ReorgMeta.WarningsCount = warningsCount
}

// GetWarnings gets the warnings of the rows handled.
func (job *Job) GetWarnings() (map[errors.ErrorID]*terror.Error, map[errors.ErrorID]int64) {
	return job.ReorgMeta.Warnings, job.ReorgMeta.WarningsCount
}

// Encode encodes job with json format.
// updateRawArgs is used to determine whether to update the raw args.
func (job *Job) Encode(updateRawArgs bool) ([]byte, error) {
	var err error
	if updateRawArgs {
		job.RawArgs, err = json.Marshal(job.Args)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if job.MultiSchemaInfo != nil {
			for _, sub := range job.MultiSchemaInfo.SubJobs {
				// Only update the args of executing sub-jobs.
				if sub.Args == nil {
					continue
				}
				sub.RawArgs, err = json.Marshal(sub.Args)
				if err != nil {
					return nil, errors.Trace(err)
				}
			}
		}
	}

	var b []byte
	job.Mu.Lock()
	defer job.Mu.Unlock()
	b, err = json.Marshal(job)

	return b, errors.Trace(err)
}

// Decode decodes job from the json buffer, we must use DecodeArgs later to
// decode special args for this job.
func (job *Job) Decode(b []byte) error {
	err := json.Unmarshal(b, job)
	return errors.Trace(err)
}

// DecodeArgs decodes job args.
func (job *Job) DecodeArgs(args ...interface{}) error {
	var rawArgs []json.RawMessage
	if err := json.Unmarshal(job.RawArgs, &rawArgs); err != nil {
		return errors.Trace(err)
	}

	sz := len(rawArgs)
	if sz > len(args) {
		sz = len(args)
	}

	for i := 0; i < sz; i++ {
		if err := json.Unmarshal(rawArgs[i], args[i]); err != nil {
			return errors.Trace(err)
		}
	}
	job.Args = args[:sz]
	return nil
}

// String implements fmt.Stringer interface.
func (job *Job) String() string {
	rowCount := job.GetRowCount()
	ret := fmt.Sprintf("ID:%d, Type:%s, State:%s, SchemaState:%s, SchemaID:%d, TableID:%d, RowCount:%d, ArgLen:%d, start time: %v, Err:%v, ErrCount:%d, SnapshotVersion:%v",
		job.ID, job.Type, job.State, job.SchemaState, job.SchemaID, job.TableID, rowCount, len(job.Args), TSConvert2Time(job.StartTS), job.Error, job.ErrorCount, job.SnapshotVer)
	if job.ReorgMeta != nil {
		ret += fmt.Sprintf(", UniqueWarnings:%d", len(job.ReorgMeta.Warnings))
	}
	if job.Type != ActionMultiSchemaChange && job.MultiSchemaInfo != nil {
		ret += fmt.Sprintf(", Multi-Schema Change:true, Revertible:%v", job.MultiSchemaInfo.Revertible)
	}
	return ret
}

func (job *Job) hasDependentSchema(other *Job) (bool, error) {
	if other.Type == ActionDropSchema || other.Type == ActionCreateSchema {
		if other.SchemaID == job.SchemaID {
			return true, nil
		}
		if job.Type == ActionRenameTable {
			var oldSchemaID int64
			if err := job.DecodeArgs(&oldSchemaID); err != nil {
				return false, errors.Trace(err)
			}
			if other.SchemaID == oldSchemaID {
				return true, nil
			}
		}
		if job.Type == ActionExchangeTablePartition {
			var (
				defID          int64
				ptSchemaID     int64
				ptID           int64
				partName       string
				withValidation bool
			)
			if err := job.DecodeArgs(&defID, &ptSchemaID, &ptID, &partName, &withValidation); err != nil {
				return false, errors.Trace(err)
			}
			if other.SchemaID == ptSchemaID {
				return true, nil
			}
		}
	}
	return false, nil
}

func (job *Job) hasDependentTableForExchangePartition(other *Job) (bool, error) {
	if job.Type == ActionExchangeTablePartition {
		var (
			defID          int64
			ptSchemaID     int64
			ptID           int64
			partName       string
			withValidation bool
		)

		if err := job.DecodeArgs(&defID, &ptSchemaID, &ptID, &partName, &withValidation); err != nil {
			return false, errors.Trace(err)
		}
		if ptID == other.TableID || defID == other.TableID {
			return true, nil
		}

		if other.Type == ActionExchangeTablePartition {
			var (
				otherDefID          int64
				otherPtSchemaID     int64
				otherPtID           int64
				otherPartName       string
				otherWithValidation bool
			)
			if err := other.DecodeArgs(&otherDefID, &otherPtSchemaID, &otherPtID, &otherPartName, &otherWithValidation); err != nil {
				return false, errors.Trace(err)
			}
			if job.TableID == other.TableID || job.TableID == otherPtID || job.TableID == otherDefID {
				return true, nil
			}
			if ptID == other.TableID || ptID == otherPtID || ptID == otherDefID {
				return true, nil
			}
			if defID == other.TableID || defID == otherPtID || defID == otherDefID {
				return true, nil
			}
		}
	}
	return false, nil
}

// IsDependentOn returns whether the job depends on "other".
// How to check the job depends on "other"?
// 1. The two jobs handle the same database when one of the two jobs is an ActionDropSchema or ActionCreateSchema type.
// 2. Or the two jobs handle the same table.
// 3. Or other job is flashback cluster.
func (job *Job) IsDependentOn(other *Job) (bool, error) {
	if other.Type == ActionFlashbackCluster {
		return true, nil
	}

	isDependent, err := job.hasDependentSchema(other)
	if err != nil || isDependent {
		return isDependent, errors.Trace(err)
	}
	isDependent, err = other.hasDependentSchema(job)
	if err != nil || isDependent {
		return isDependent, errors.Trace(err)
	}

	// TODO: If a job is ActionRenameTable, we need to check table name.
	if other.TableID == job.TableID {
		return true, nil
	}
	isDependent, err = job.hasDependentTableForExchangePartition(other)
	if err != nil || isDependent {
		return isDependent, errors.Trace(err)
	}
	isDependent, err = other.hasDependentTableForExchangePartition(job)
	if err != nil || isDependent {
		return isDependent, errors.Trace(err)
	}
	return false, nil
}

// IsFinished returns whether job is finished or not.
// If the job state is Done or Cancelled, it is finished.
func (job *Job) IsFinished() bool {
	return job.State == JobStateDone || job.State == JobStateRollbackDone || job.State == JobStateCancelled
}

// IsCancelled returns whether the job is cancelled or not.
func (job *Job) IsCancelled() bool {
	return job.State == JobStateCancelled
}

// IsRollbackDone returns whether the job is rolled back or not.
func (job *Job) IsRollbackDone() bool {
	return job.State == JobStateRollbackDone
}

// IsRollingback returns whether the job is rolling back or not.
func (job *Job) IsRollingback() bool {
	return job.State == JobStateRollingback
}

// IsCancelling returns whether the job is cancelling or not.
func (job *Job) IsCancelling() bool {
	return job.State == JobStateCancelling
}

// IsSynced returns whether the DDL modification is synced among all TiDB servers.
func (job *Job) IsSynced() bool {
	return job.State == JobStateSynced
}

// IsDone returns whether job is done.
func (job *Job) IsDone() bool {
	return job.State == JobStateDone
}

// IsRunning returns whether job is still running or not.
func (job *Job) IsRunning() bool {
	return job.State == JobStateRunning
}

// IsQueueing returns whether job is queuing or not.
func (job *Job) IsQueueing() bool {
	return job.State == JobStateQueueing
}

// NotStarted returns true if the job is never run by a worker.
func (job *Job) NotStarted() bool {
	return job.State == JobStateNone || job.State == JobStateQueueing
}

// MayNeedReorg indicates that this job may need to reorganize the data.
func (job *Job) MayNeedReorg() bool {
	switch job.Type {
	case ActionAddIndex, ActionAddPrimaryKey:
		return true
	case ActionModifyColumn:
		if len(job.CtxVars) > 0 {
			needReorg, ok := job.CtxVars[0].(bool)
			return ok && needReorg
		}
		return false
	case ActionMultiSchemaChange:
		for _, sub := range job.MultiSchemaInfo.SubJobs {
			proxyJob := Job{Type: sub.Type, CtxVars: sub.CtxVars}
			if proxyJob.MayNeedReorg() {
				return true
			}
		}
		return false
	default:
		return false
	}
}

// IsRollbackable checks whether the job can be rollback.
func (job *Job) IsRollbackable() bool {
	switch job.Type {
	case ActionDropIndex, ActionDropPrimaryKey:
		// We can't cancel if index current state is in StateDeleteOnly or StateDeleteReorganization or StateWriteOnly, otherwise there will be an inconsistent issue between record and index.
		// In WriteOnly state, we can rollback for normal index but can't rollback for expression index(need to drop hidden column). Since we can't
		// know the type of index here, we consider all indices except primary index as non-rollbackable.
		// TODO: distinguish normal index and expression index so that we can rollback `DropIndex` for normal index in WriteOnly state.
		// TODO: make DropPrimaryKey rollbackable in WriteOnly, it need to deal with some tests.
		if job.SchemaState == StateDeleteOnly ||
			job.SchemaState == StateDeleteReorganization ||
			job.SchemaState == StateWriteOnly {
			return false
		}
	case ActionAddTablePartition:
		return job.SchemaState == StateNone || job.SchemaState == StateReplicaOnly
	case ActionDropColumn, ActionDropSchema, ActionDropTable, ActionDropSequence,
		ActionDropForeignKey, ActionDropTablePartition:
		return job.SchemaState == StatePublic
	case ActionRebaseAutoID, ActionShardRowID,
		ActionTruncateTable, ActionAddForeignKey, ActionRenameTable,
		ActionModifyTableCharsetAndCollate, ActionTruncateTablePartition,
		ActionModifySchemaCharsetAndCollate, ActionRepairTable,
		ActionModifyTableAutoIdCache, ActionModifySchemaDefaultPlacement:
		return job.SchemaState == StateNone
	case ActionMultiSchemaChange:
		return job.MultiSchemaInfo.Revertible
	case ActionFlashbackCluster:
		if job.SchemaState == StateWriteReorganization ||
			job.SchemaState == StateWriteOnly {
			return false
		}
	}
	return true
}

// JobState is for job state.
type JobState byte

// List job states.
const (
	JobStateNone    JobState = 0
	JobStateRunning JobState = 1
	// When DDL encountered an unrecoverable error at reorganization state,
	// some keys has been added already, we need to remove them.
	// JobStateRollingback is the state to do the rolling back job.
	JobStateRollingback  JobState = 2
	JobStateRollbackDone JobState = 3
	JobStateDone         JobState = 4
	JobStateCancelled    JobState = 5
	// JobStateSynced is used to mark the information about the completion of this job
	// has been synchronized to all servers.
	JobStateSynced JobState = 6
	// JobStateCancelling is used to mark the DDL job is cancelled by the client, but the DDL work hasn't handle it.
	JobStateCancelling JobState = 7
	// JobStateQueueing means the job has not yet been started.
	JobStateQueueing JobState = 8
)

// String implements fmt.Stringer interface.
func (s JobState) String() string {
	switch s {
	case JobStateRunning:
		return "running"
	case JobStateRollingback:
		return "rollingback"
	case JobStateRollbackDone:
		return "rollback done"
	case JobStateDone:
		return "done"
	case JobStateCancelled:
		return "cancelled"
	case JobStateCancelling:
		return "cancelling"
	case JobStateSynced:
		return "synced"
	case JobStateQueueing:
		return "queueing"
	default:
		return "none"
	}
}

// SchemaDiff contains the schema modification at a particular schema version.
// It is used to reduce schema reload cost.
type SchemaDiff struct {
	Version  int64      `json:"version"`
	Type     ActionType `json:"type"`
	SchemaID int64      `json:"schema_id"`
	TableID  int64      `json:"table_id"`

	// OldTableID is the table ID before truncate, only used by truncate table DDL.
	OldTableID int64 `json:"old_table_id"`
	// OldSchemaID is the schema ID before rename table, only used by rename table DDL.
	OldSchemaID int64 `json:"old_schema_id"`
	// RegenerateSchemaMap means whether to rebuild the schema map when applying to the schema diff.
	RegenerateSchemaMap bool `json:"regenerate_schema_map"`

	AffectedOpts []*AffectedOption `json:"affected_options"`
}

// AffectedOption is used when a ddl affects multi tables.
type AffectedOption struct {
	SchemaID    int64 `json:"schema_id"`
	TableID     int64 `json:"table_id"`
	OldTableID  int64 `json:"old_table_id"`
	OldSchemaID int64 `json:"old_schema_id"`
}
