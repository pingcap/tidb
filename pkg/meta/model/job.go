// Copyright 2024 PingCAP, Inc.
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

package model

import (
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/util/intest"
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
	_DEPRECATEDActionAddColumns         ActionType = 37 // Deprecated, we use ActionMultiSchemaChange instead.
	_DEPRECATEDActionDropColumns        ActionType = 38 // Deprecated, we use ActionMultiSchemaChange instead.
	ActionModifyTableAutoIDCache        ActionType = 39
	ActionRebaseAutoRandomBase          ActionType = 40
	ActionAlterIndexVisibility          ActionType = 41
	ActionExchangeTablePartition        ActionType = 42
	ActionAddCheckConstraint            ActionType = 43
	ActionDropCheckConstraint           ActionType = 44
	ActionAlterCheckConstraint          ActionType = 45

	// `ActionAlterTableAlterPartition` is removed and will never be used.
	// Just left a tombstone here for compatibility.
	_DEPRECATEDActionAlterTableAlterPartition ActionType = 46

	ActionRenameTables                  ActionType = 47
	_DEPRECATEDActionDropIndexes        ActionType = 48 // Deprecated, we use ActionMultiSchemaChange instead.
	ActionAlterTableAttributes          ActionType = 49
	ActionAlterTablePartitionAttributes ActionType = 50
	ActionCreatePlacementPolicy         ActionType = 51
	ActionAlterPlacementPolicy          ActionType = 52
	ActionDropPlacementPolicy           ActionType = 53
	ActionAlterTablePartitionPlacement  ActionType = 54
	ActionModifySchemaDefaultPlacement  ActionType = 55
	ActionAlterTablePlacement           ActionType = 56
	ActionAlterCacheTable               ActionType = 57
	// not used
	ActionAlterTableStatsOptions ActionType = 58
	ActionAlterNoCacheTable      ActionType = 59
	ActionCreateTables           ActionType = 60
	ActionMultiSchemaChange      ActionType = 61
	ActionFlashbackCluster       ActionType = 62
	ActionRecoverSchema          ActionType = 63
	ActionReorganizePartition    ActionType = 64
	ActionAlterTTLInfo           ActionType = 65
	ActionAlterTTLRemove         ActionType = 67
	ActionCreateResourceGroup    ActionType = 68
	ActionAlterResourceGroup     ActionType = 69
	ActionDropResourceGroup      ActionType = 70
	ActionAlterTablePartitioning ActionType = 71
	ActionRemovePartitioning     ActionType = 72
	ActionAddVectorIndex         ActionType = 73
)

// ActionMap is the map of DDL ActionType to string.
var ActionMap = map[ActionType]string{
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
	ActionModifyTableAutoIDCache:        "modify auto id cache",
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
	ActionAlterTablePartitioning:        "alter table partition by",
	ActionRemovePartitioning:            "alter table remove partitioning",
	ActionAddVectorIndex:                "add vector index",

	// `ActionAlterTableAlterPartition` is removed and will never be used.
	// Just left a tombstone here for compatibility.
	_DEPRECATEDActionAlterTableAlterPartition: "alter partition",
}

// String return current ddl action in string
func (action ActionType) String() string {
	if v, ok := ActionMap[action]; ok {
		return v
	}
	return "none"
}

// SchemaState is the state for schema elements.
type SchemaState byte

const (
	// StateNone means this schema element is absent and can't be used.
	StateNone SchemaState = iota
	// StateDeleteOnly means we can only delete items for this schema element.
	StateDeleteOnly
	// StateWriteOnly means we can use any write operation on this schema element,
	// but outer can't read the changed data.
	StateWriteOnly
	// StateWriteReorganization means we are re-organizing whole data after write only state.
	StateWriteReorganization
	// StateDeleteReorganization means we are re-organizing whole data after delete only state.
	StateDeleteReorganization
	// StatePublic means this schema element is ok for all write and read operations.
	StatePublic
	// StateReplicaOnly means we're waiting tiflash replica to be finished.
	StateReplicaOnly
	// StateGlobalTxnOnly means we can only use global txn for operator on this schema element
	StateGlobalTxnOnly
	/*
	 *  Please add the new state at the end to keep the values consistent across versions.
	 */
)

// String implements fmt.Stringer interface.
func (s SchemaState) String() string {
	switch s {
	case StateDeleteOnly:
		return "delete only"
	case StateWriteOnly:
		return "write only"
	case StateWriteReorganization:
		return "write reorganization"
	case StateDeleteReorganization:
		return "delete reorganization"
	case StatePublic:
		return "public"
	case StateReplicaOnly:
		return "replica only"
	case StateGlobalTxnOnly:
		return "global txn only"
	default:
		return "none"
	}
}

// JobVersion is the version of DDL job.
type JobVersion int64

const (
	// JobVersion1 is the first version of DDL job where job args are stored as un-typed
	// array. Before v8.4.0, all DDL jobs are in this version.
	JobVersion1 JobVersion = 1
	// JobVersion2 is the second version of DDL job where job args are stored as
	// typed structs, we start to use this version from v8.4.0.
	// Note: this version is not enabled right now except in some test cases, will
	// enable it after we have CI to run both versions.
	JobVersion2 JobVersion = 2
)

// String implements fmt.Stringer interface.
func (v JobVersion) String() string {
	if v == JobVersion1 {
		return "v1"
	} else if v == JobVersion2 {
		return "v2"
	}
	return fmt.Sprintf("unknown(%d)", v)
}

// JobVerInUse is the job version for new DDL jobs in the node.
// it's for test now.
var jobVerInUse atomic.Int64

// SetJobVerInUse sets the version of DDL job used in the node.
func SetJobVerInUse(ver JobVersion) {
	jobVerInUse.Store(int64(ver))
}

// GetJobVerInUse returns the version of DDL job used in the node.
func GetJobVerInUse() JobVersion {
	return JobVersion(jobVerInUse.Load())
}

// Job is for a DDL operation.
type Job struct {
	ID   int64      `json:"id"`
	Type ActionType `json:"type"`
	// SchemaID means different for different job types:
	// - ExchangeTablePartition: db id of non-partitioned table
	SchemaID int64 `json:"schema_id"`
	// TableID means different for different job types:
	// - ExchangeTablePartition: non-partitioned table id
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
	// for ExchangeTablePartition, RenameTables, RenameTable, it's [slice-of-db-id, slice-of-table-id]
	CtxVars []any `json:"-"`
	// Note: it might change when state changes, such as when rollback on AddColumn.
	// - CreateTable, it's [TableInfo, foreignKeyCheck]
	// - AddIndex or AddPrimaryKey: [unique, ....
	// - TruncateTable: [new-table-id, foreignKeyCheck, ...
	// - RenameTable: [old-db-id, new-table-name, old-db-name]
	// - ExchangeTablePartition: [partition-id, pt-db-id, pt-id, partition-name, with-validation]
	// when Version is JobVersion2, Args contains a single element of type JobArgs.
	// TODO make it private after we finish the migration to JobVersion2.
	Args []any `json:"-"`
	// we use json raw message to delay parsing special args.
	// the args are cleared out unless Job.FillFinishedArgs is called.
	RawArgs json.RawMessage `json:"raw_args"`

	SchemaState SchemaState `json:"schema_state"`
	// SnapshotVer means snapshot version for this job.
	SnapshotVer uint64 `json:"snapshot_ver"`
	// RealStartTS uses timestamp allocated by TSO.
	// Now it's the TS when we actually start the job.
	RealStartTS uint64 `json:"real_start_ts"`
	// StartTS uses timestamp allocated by TSO.
	// Now it's the TS when we put the job to job table.
	StartTS uint64 `json:"start_ts"`
	// DependencyID is the largest job ID before current job and current job depends on.
	DependencyID int64 `json:"dependency_id"`
	// Query string of the ddl job.
	Query      string       `json:"query"`
	BinlogInfo *HistoryInfo `json:"binlog"`

	// Version indicates the DDL job version.
	Version JobVersion `json:"version"`

	// ReorgMeta is meta info of ddl reorganization.
	ReorgMeta *DDLReorgMeta `json:"reorg_meta"`

	// MultiSchemaInfo keeps some warning now for multi schema change.
	MultiSchemaInfo *MultiSchemaInfo `json:"multi_schema_info"`

	// Priority is only used to set the operation priority of adding indices.
	Priority int `json:"priority"`

	// SeqNum is used to identify the order of moving the job into DDL history, it's
	// not the order of the job execution. for jobs with dependency, or if they are
	// run in the same session, their SeqNum will be in increasing order.
	// when using fast create table, there might duplicate seq_num as any TiDB can
	// execute the DDL in this case.
	// since 8.3, we only honor previous semantic when DDL owner not changed, on
	// owner change, new owner will start it from 1. as previous semantic forces
	// 'moving jobs into DDL history' part to be serial, it hurts performance, and
	// has very limited usage scenario.
	SeqNum uint64 `json:"seq_num"`

	// Charset is the charset when the DDL Job is created.
	Charset string `json:"charset"`
	// Collate is the collation the DDL Job is created.
	Collate string `json:"collate"`

	// InvolvingSchemaInfo indicates the schema info involved in the job.
	// nil means fallback to use job.SchemaName/TableName.
	// Keep unchanged after initialization.
	InvolvingSchemaInfo []InvolvingSchemaInfo `json:"involving_schema_info,omitempty"`

	// AdminOperator indicates where the Admin command comes, by the TiDB
	// itself (AdminCommandBySystem) or by user (AdminCommandByEndUser).
	AdminOperator AdminCommandOperator `json:"admin_operator"`

	// TraceInfo indicates the information for SQL tracing
	TraceInfo *TraceInfo `json:"trace_info"`

	// BDRRole indicates the role of BDR cluster when executing this DDL.
	BDRRole string `json:"bdr_role"`

	// CDCWriteSource indicates the source of CDC write.
	CDCWriteSource uint64 `json:"cdc_write_source"`

	// LocalMode = true means the job is running on the local TiDB that the client
	// connects to, else it's run on the DDL owner.
	// Only happens when tidb_enable_fast_create_table = on
	// this field is useless since 8.3
	LocalMode bool `json:"local_mode"`

	// SQLMode for executing DDL query.
	SQLMode mysql.SQLMode `json:"sql_mode"`
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
		clone.Args = make([]any, len(job.Args))
		copy(clone.Args, job.Args)
	}
	if job.MultiSchemaInfo != nil {
		for i, sub := range job.MultiSchemaInfo.SubJobs {
			clone.MultiSchemaInfo.SubJobs[i].Args = make([]any, len(sub.Args))
			copy(clone.MultiSchemaInfo.SubJobs[i].Args, sub.Args)
		}
	}
	return &clone
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
	job.Mu.Lock()
	job.ReorgMeta.Warnings = warnings
	job.ReorgMeta.WarningsCount = warningsCount
	job.Mu.Unlock()
}

// GetWarnings gets the warnings of the rows handled.
func (job *Job) GetWarnings() (map[errors.ErrorID]*terror.Error, map[errors.ErrorID]int64) {
	job.Mu.Lock()
	w, wc := job.ReorgMeta.Warnings, job.ReorgMeta.WarningsCount
	job.Mu.Unlock()
	return w, wc
}

// FillArgs fills args for new job.
func (job *Job) FillArgs(args JobArgs) {
	args.fillJob(job)
}

// FillFinishedArgs fills args for finished job.
func (job *Job) FillFinishedArgs(args FinishedJobArgs) {
	args.fillFinishedJob(job)
}

// Encode encodes job with json format.
// updateRawArgs is used to determine whether to update the raw args.
func (job *Job) Encode(updateRawArgs bool) ([]byte, error) {
	var err error
	if updateRawArgs {
		if job.Version == JobVersion1 {
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
		} else {
			var arg any
			if len(job.Args) > 0 {
				arg = job.Args[0]
			}
			job.RawArgs, err = json.Marshal(arg)
			if err != nil {
				return nil, errors.Trace(err)
			}
			// TODO remember update sub-jobs' RawArgs when we do it.
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

// DecodeArgs decodes serialized job arguments from job.RawArgs into the given
// variables, and also save the result in job.Args. It's for JobVersion1.
func (job *Job) DecodeArgs(args ...any) error {
	intest.Assert(job.Version == JobVersion1, "Job.DecodeArgs is only used for JobVersion1")
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
	// TODO(lance6716): don't assign to job.Args here, because the types of argument
	// `args` are always pointer type. But sometimes in the `job` literals we don't
	// use pointer
	job.Args = args[:sz]
	return nil
}

// DecodeDropIndexFinishedArgs decodes the drop index job's args when it's finished.
func (job *Job) DecodeDropIndexFinishedArgs() (
	indexName any, ifExists []bool, indexIDs []int64, partitionIDs []int64, hasVectors []bool, err error) {
	ifExists = make([]bool, 1)
	indexIDs = make([]int64, 1)
	hasVectors = make([]bool, 1)
	if err := job.DecodeArgs(&indexName, &ifExists[0], &indexIDs[0], &partitionIDs, &hasVectors[0]); err != nil {
		if err := job.DecodeArgs(&indexName, &ifExists, &indexIDs, &partitionIDs, &hasVectors); err != nil {
			return nil, []bool{false}, []int64{-1}, nil, []bool{false}, errors.Trace(err)
		}
	}
	return
}

// String implements fmt.Stringer interface.
func (job *Job) String() string {
	rowCount := job.GetRowCount()
	ret := fmt.Sprintf("ID:%d, Type:%s, State:%s, SchemaState:%s, SchemaID:%d, TableID:%d, RowCount:%d, ArgLen:%d, start time: %v, Err:%v, ErrCount:%d, SnapshotVersion:%v, Version: %s",
		job.ID, job.Type, job.State, job.SchemaState, job.SchemaID, job.TableID, rowCount, len(job.Args), TSConvert2Time(job.StartTS), job.Error, job.ErrorCount, job.SnapshotVer, job.Version)
	if job.ReorgMeta != nil {
		warnings, _ := job.GetWarnings()
		ret += fmt.Sprintf(", UniqueWarnings:%d", len(warnings))
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

// IsPaused returns whether the job is paused.
func (job *Job) IsPaused() bool {
	return job.State == JobStatePaused
}

// IsPausedBySystem returns whether the job is paused by system.
func (job *Job) IsPausedBySystem() bool {
	return job.IsPaused() && job.AdminOperator == AdminCommandBySystem
}

// IsPausing indicates whether the job is pausing.
func (job *Job) IsPausing() bool {
	return job.State == JobStatePausing
}

// IsPausable checks whether we can pause the job.
func (job *Job) IsPausable() bool {
	// TODO: We can remove it after TiFlash supports the pause operation.
	if job.Type == ActionAddVectorIndex && job.SchemaState == StateWriteReorganization {
		return false
	}
	return job.NotStarted() || (job.IsRunning() && job.IsRollbackable())
}

// IsResumable checks whether the job can be rollback.
func (job *Job) IsResumable() bool {
	return job.IsPaused()
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

// Started returns true if the job is started.
func (job *Job) Started() bool {
	return !job.NotStarted()
}

// InFinalState returns whether the job is in a final state of job FSM.
// TODO JobStateRollbackDone is not a final state, maybe we should add a JobStateRollbackSynced
// state to diff between the entrance of JobStateRollbackDone and move the job to
// history where the job is in final state.
func (job *Job) InFinalState() bool {
	return job.State == JobStateSynced || job.State == JobStateCancelled || job.State == JobStatePaused
}

// MayNeedReorg indicates that this job may need to reorganize the data.
func (job *Job) MayNeedReorg() bool {
	switch job.Type {
	case ActionAddIndex, ActionAddPrimaryKey, ActionReorganizePartition,
		ActionRemovePartitioning, ActionAlterTablePartitioning:
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
// TODO(lance6716): should make sure it's the same as convertJob2RollbackJob
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
		ActionDropForeignKey, ActionDropTablePartition, ActionTruncateTablePartition:
		return job.SchemaState == StatePublic
	case ActionRebaseAutoID, ActionShardRowID,
		ActionTruncateTable, ActionAddForeignKey, ActionRenameTable, ActionRenameTables,
		ActionModifyTableCharsetAndCollate,
		ActionModifySchemaCharsetAndCollate, ActionRepairTable,
		ActionModifyTableAutoIDCache, ActionModifySchemaDefaultPlacement, ActionDropCheckConstraint:
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

// GetInvolvingSchemaInfo returns the schema info involved in the job.
func (job *Job) GetInvolvingSchemaInfo() []InvolvingSchemaInfo {
	if len(job.InvolvingSchemaInfo) > 0 {
		return job.InvolvingSchemaInfo
	}
	table := job.TableName
	// for schema related DDL, such as 'drop schema xxx'
	if len(job.SchemaName) > 0 && table == "" {
		table = InvolvingAll
	}
	return []InvolvingSchemaInfo{
		{Database: job.SchemaName, Table: table},
	}
}

// SubJob is a representation of one DDL schema change. A Job may contain zero
// (when multi-schema change is not applicable) or more SubJobs.
type SubJob struct {
	Type        ActionType      `json:"type"`
	Args        []any           `json:"-"`
	RawArgs     json.RawMessage `json:"raw_args"`
	SchemaState SchemaState     `json:"schema_state"`
	SnapshotVer uint64          `json:"snapshot_ver"`
	RealStartTS uint64          `json:"real_start_ts"`
	Revertible  bool            `json:"revertible"`
	State       JobState        `json:"state"`
	RowCount    int64           `json:"row_count"`
	Warning     *terror.Error   `json:"warning"`
	CtxVars     []any           `json:"-"`
	SchemaVer   int64           `json:"schema_version"`
	ReorgTp     ReorgType       `json:"reorg_tp"`
	UseCloud    bool            `json:"use_cloud"`
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
func (sub *SubJob) ToProxyJob(parentJob *Job, seq int) Job {
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
		RealStartTS:     sub.RealStartTS,
		StartTS:         parentJob.StartTS,
		DependencyID:    parentJob.DependencyID,
		Query:           parentJob.Query,
		BinlogInfo:      parentJob.BinlogInfo,
		Version:         parentJob.Version,
		ReorgMeta:       parentJob.ReorgMeta,
		MultiSchemaInfo: &MultiSchemaInfo{Revertible: sub.Revertible, Seq: int32(seq)},
		Priority:        parentJob.Priority,
		SeqNum:          parentJob.SeqNum,
		Charset:         parentJob.Charset,
		Collate:         parentJob.Collate,
		AdminOperator:   parentJob.AdminOperator,
		TraceInfo:       parentJob.TraceInfo,
	}
}

// FromProxyJob converts a proxy job to a sub-job.
func (sub *SubJob) FromProxyJob(proxyJob *Job, ver int64) {
	sub.Revertible = proxyJob.MultiSchemaInfo.Revertible
	sub.SchemaState = proxyJob.SchemaState
	sub.SnapshotVer = proxyJob.SnapshotVer
	sub.RealStartTS = proxyJob.RealStartTS
	sub.Args = proxyJob.Args
	sub.State = proxyJob.State
	sub.Warning = proxyJob.Warning
	sub.RowCount = proxyJob.RowCount
	sub.SchemaVer = ver
	sub.ReorgTp = proxyJob.ReorgMeta.ReorgTp
	sub.UseCloud = proxyJob.ReorgMeta.UseCloudStorage
}

// MultiSchemaInfo keeps some information for multi schema change.
type MultiSchemaInfo struct {
	SubJobs    []*SubJob `json:"sub_jobs"`
	Revertible bool      `json:"revertible"`
	Seq        int32     `json:"seq"`

	// SkipVersion is used to control whether generating a new schema version for a sub-job.
	SkipVersion bool `json:"-"`

	AddColumns    []model.CIStr `json:"-"`
	DropColumns   []model.CIStr `json:"-"`
	ModifyColumns []model.CIStr `json:"-"`
	AddIndexes    []model.CIStr `json:"-"`
	DropIndexes   []model.CIStr `json:"-"`
	AlterIndexes  []model.CIStr `json:"-"`

	AddForeignKeys []AddForeignKeyInfo `json:"-"`

	RelativeColumns []model.CIStr `json:"-"`
	PositionColumns []model.CIStr `json:"-"`
}

// AddForeignKeyInfo contains foreign key information.
type AddForeignKeyInfo struct {
	Name model.CIStr
	Cols []model.CIStr
}

// NewMultiSchemaInfo new a MultiSchemaInfo.
func NewMultiSchemaInfo() *MultiSchemaInfo {
	return &MultiSchemaInfo{
		SubJobs:    nil,
		Revertible: true,
	}
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

// InvolvingSchemaInfo returns the schema info involved in the job. The value
// should be stored in lower case. Only one type of the three member types
// (Database&Table, Policy, ResourceGroup) should only be set in a
// InvolvingSchemaInfo.
type InvolvingSchemaInfo struct {
	Database      string                  `json:"database,omitempty"`
	Table         string                  `json:"table,omitempty"`
	Policy        string                  `json:"policy,omitempty"`
	ResourceGroup string                  `json:"resource_group,omitempty"`
	Mode          InvolvingSchemaInfoMode `json:"mode,omitempty"`
}

// InvolvingSchemaInfoMode is used by InvolvingSchemaInfo.Mode.
type InvolvingSchemaInfoMode int

// ExclusiveInvolving and SharedInvolving are considered like the exclusive lock
// and shared lock when calculate DDL job dependencies. And we also implement the
// fair lock semantic which means if we have job A/B/C arrive in order, and job B
// (exclusive request object 0) is waiting for the running job A (shared request
// object 0), and job C (shared request object 0) arrives, job C should also be
// blocked until job B is finished although job A & C has no dependency.
const (
	// ExclusiveInvolving is the default value to keep compatibility with old
	// versions.
	ExclusiveInvolving InvolvingSchemaInfoMode = iota
	SharedInvolving
)

const (
	// InvolvingAll means all schemas/tables are affected. It's used in
	// InvolvingSchemaInfo.Database/Tables fields. When both the Database and Tables
	// are InvolvingAll it also means all placement policies and resource groups are
	// affected. Currently the only case is FLASHBACK CLUSTER.
	InvolvingAll = "*"
	// InvolvingNone means no schema/table is affected.
	InvolvingNone = ""
)

// JobState is for job state.
type JobState int32

// List job states.
const (
	JobStateNone    JobState = 0
	JobStateRunning JobState = 1
	// JobStateRollingback is the state to do the rolling back job.
	// When DDL encountered an unrecoverable error at reorganization state,
	// some keys has been added already, we need to remove them.
	JobStateRollingback  JobState = 2
	JobStateRollbackDone JobState = 3
	JobStateDone         JobState = 4
	// JobStateCancelled is the state to do the job is cancelled, this state only
	// persisted to history table and queue too.
	JobStateCancelled JobState = 5
	// JobStateSynced means the job is done and has been synchronized to all servers.
	// job of this state will not be written to the tidb_ddl_job table, when job
	// is in `done` state and version synchronized, the job will be deleted from
	// tidb_ddl_job table, and we insert a `synced` job to the history table and queue directly.
	JobStateSynced JobState = 6
	// JobStateCancelling is used to mark the DDL job is cancelled by the client, but
	// the DDL worker hasn't handled it.
	JobStateCancelling JobState = 7
	// JobStateQueueing means the job has not yet been started.
	JobStateQueueing JobState = 8

	JobStatePaused  JobState = 9
	JobStatePausing JobState = 10
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
	case JobStatePaused:
		return "paused"
	case JobStatePausing:
		return "pausing"
	default:
		return "none"
	}
}

// StrToJobState converts string to JobState.
func StrToJobState(s string) JobState {
	switch s {
	case "running":
		return JobStateRunning
	case "rollingback":
		return JobStateRollingback
	case "rollback done":
		return JobStateRollbackDone
	case "done":
		return JobStateDone
	case "cancelled":
		return JobStateCancelled
	case "cancelling":
		return JobStateCancelling
	case "synced":
		return JobStateSynced
	case "queueing":
		return JobStateQueueing
	case "paused":
		return JobStatePaused
	case "pausing":
		return JobStatePausing
	default:
		return JobStateNone
	}
}

// AdminCommandOperator indicates where the Cancel/Pause/Resume command on DDL
// jobs comes from.
type AdminCommandOperator int

const (
	// AdminCommandByNotKnown indicates that unknow calling of the
	// Cancel/Pause/Resume on DDL job.
	AdminCommandByNotKnown AdminCommandOperator = iota
	// AdminCommandByEndUser indicates that the Cancel/Pause/Resume command on
	// DDL job is issued by the end user.
	AdminCommandByEndUser
	// AdminCommandBySystem indicates that the Cancel/Pause/Resume command on
	// DDL job is issued by TiDB itself, such as Upgrade(bootstrap).
	AdminCommandBySystem
)

// String implements fmt.Stringer interface.
func (a *AdminCommandOperator) String() string {
	switch *a {
	case AdminCommandByEndUser:
		return "EndUser"
	case AdminCommandBySystem:
		return "System"
	default:
		return "None"
	}
}

// SchemaDiff contains the schema modification at a particular schema version.
// It is used to reduce schema reload cost.
type SchemaDiff struct {
	Version  int64      `json:"version"`
	Type     ActionType `json:"type"`
	SchemaID int64      `json:"schema_id"`
	TableID  int64      `json:"table_id"`

	// SubActionTypes is the list of action types done together within a multiple schema
	// change job. As the job might contain multiple steps that changes schema version,
	// if some step only contains one action, Type will be that action, and SubActionTypes
	// will be empty.
	// for other types of job, it will always be empty.
	SubActionTypes []ActionType `json:"sub_action_types,omitempty"`
	// OldTableID is the table ID before truncate, only used by truncate table DDL.
	OldTableID int64 `json:"old_table_id"`
	// OldSchemaID is the schema ID before rename table, only used by rename table DDL.
	OldSchemaID int64 `json:"old_schema_id"`
	// RegenerateSchemaMap means whether to rebuild the schema map when applying to the schema diff.
	RegenerateSchemaMap bool `json:"regenerate_schema_map"`
	// ReadTableFromMeta is set to avoid the diff is too large to be saved in SchemaDiff.
	// infoschema should read latest meta directly.
	ReadTableFromMeta bool `json:"read_table_from_meta,omitempty"`

	AffectedOpts []*AffectedOption `json:"affected_options"`
}

// AffectedOption is used when a ddl affects multi tables.
type AffectedOption struct {
	SchemaID    int64 `json:"schema_id"`
	TableID     int64 `json:"table_id"`
	OldTableID  int64 `json:"old_table_id"`
	OldSchemaID int64 `json:"old_schema_id"`
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

// TraceInfo is the information for trace.
type TraceInfo struct {
	// ConnectionID is the id of the connection
	ConnectionID uint64 `json:"connection_id"`
	// SessionAlias is the alias of session
	SessionAlias string `json:"session_alias"`
}

func init() {
	SetJobVerInUse(JobVersion1)
}
