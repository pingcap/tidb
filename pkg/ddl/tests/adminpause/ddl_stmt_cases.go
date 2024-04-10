// Copyright 2023 PingCAP, Inc.
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

package adminpause

import (
	"sync"

	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/testkit"
)

type autoIncrsedID struct {
	sync.Mutex // ensures autoInc is goroutine-safe
	idx        int
}

func (a *autoIncrsedID) globalID() int {
	a.Lock()
	defer a.Unlock()

	id := a.idx
	a.idx++
	return id
}

var ai autoIncrsedID

// StmtCase is a description of all kinds of DDL statements
//
// DDL case may be referenced for more than one test cases, before which `AdminPauseTestTable` should have been created.
//
// Definitions:
//   - `globalID` indicates the sequence number among all cases, even in different case array
//   - `stmt` is normally the case you care
//   - `jobState`, refer to model.SchemaState, the target state of the DDL which we want to pause
//   - `isJobPausable` indicates that the `admin pause` should return true within certain `StmtCase` object
//   - `preConditionStmts` should always be run before all kinds cases, to satify the requirement of `stmt`
//   - `rollbackStmts` should be run if necessary to clean the object created by the 'preConditionStmts' or `stmt`, no
//     matter what is the result of `stmt`
type StmtCase struct {
	globalID          int
	stmt              string
	schemaState       model.SchemaState
	isJobPausable     bool
	preConditionStmts []string
	rollbackStmts     []string
}

const testSchema string = "test_create_db"
const createSchemaStmt string = "create database " + testSchema + ";"
const dropSchemaStmt string = "drop database " + testSchema + ";"

var schemaDDLStmtCase = [...]StmtCase{
	// Create schema.
	{ai.globalID(), createSchemaStmt, model.StateNone, true, nil, []string{dropSchemaStmt}},
	{ai.globalID(), createSchemaStmt, model.StatePublic, false, nil, []string{dropSchemaStmt}},

	// Drop schema.
	{ai.globalID(), dropSchemaStmt, model.StatePublic, true, []string{createSchemaStmt}, []string{dropSchemaStmt}},
	{ai.globalID(), dropSchemaStmt, model.StateWriteOnly, false, []string{createSchemaStmt}, []string{dropSchemaStmt}},
	{ai.globalID(), dropSchemaStmt, model.StateDeleteOnly, false, []string{createSchemaStmt}, []string{dropSchemaStmt}},
	{ai.globalID(), dropSchemaStmt, model.StateNone, false, []string{createSchemaStmt}, []string{dropSchemaStmt}},
}

const tableName = "test_create_tbl"
const createTableStmt = `create table ` + tableName + ` (	id int(11) NOT NULL AUTO_INCREMENT,
	tenant varchar(128) NOT NULL,
	name varchar(128) NOT NULL,
	age int(11) NOT NULL,
	province varchar(32) NOT NULL DEFAULT '',
	city varchar(32) NOT NULL DEFAULT '',
	phone varchar(16) NOT NULL DEFAULT '',
	created_time datetime NOT NULL,
	updated_time datetime NOT NULL
  );`

const dropTableStmt = "drop table " + tableName + ";"

var tableDDLStmt = [...]StmtCase{
	// Create table.
	{ai.globalID(), createTableStmt, model.StateNone, true, nil, []string{dropTableStmt}},
	{ai.globalID(), createTableStmt, model.StatePublic, false, nil, []string{dropTableStmt}},

	// Drop table.
	{ai.globalID(), dropTableStmt, model.StatePublic, true, []string{createTableStmt}, []string{dropTableStmt}},
	{ai.globalID(), dropTableStmt, model.StateWriteOnly, false, []string{createTableStmt}, []string{dropTableStmt}},
	{ai.globalID(), dropTableStmt, model.StateDeleteOnly, false, []string{createTableStmt}, []string{dropTableStmt}},
	{ai.globalID(), dropTableStmt, model.StateNone, false, []string{createTableStmt}, []string{dropTableStmt}},
}

const alterTablePrefix string = "alter table " + adminPauseTestTable
const alterTableAddPrefix string = alterTablePrefix + " add "
const alterTableDropPrefix string = alterTablePrefix + " drop "
const alterTableModifyPrefix string = alterTablePrefix + " modify "

const addPrimaryIndexStmt string = alterTableAddPrefix + "primary key idx_id (id);"
const dropPrimaryIndexStmt string = alterTableDropPrefix + "primary key;"

const addUniqueIndexStmt string = alterTableAddPrefix + "unique index idx_phone (phone);"
const dropUniqueIndexStmt string = alterTableDropPrefix + "index if exists idx_phone;"

const addIndexStmt string = alterTableAddPrefix + "index if not exists idx_name (name);"
const dropIndexStmt string = alterTableDropPrefix + "index if exists idx_name;"

var indexDDLStmtCase = [...]StmtCase{
	// Add primary key
	{ai.globalID(), addPrimaryIndexStmt, model.StateNone, true, nil, []string{dropPrimaryIndexStmt}},
	{ai.globalID(), addPrimaryIndexStmt, model.StateDeleteOnly, true, nil, []string{dropPrimaryIndexStmt}},
	{ai.globalID(), addPrimaryIndexStmt, model.StateWriteOnly, true, nil, []string{dropPrimaryIndexStmt}},
	{ai.globalID(), addPrimaryIndexStmt, model.StateWriteReorganization, true, nil, []string{dropPrimaryIndexStmt}},
	{ai.globalID(), addPrimaryIndexStmt, model.StatePublic, false, nil, []string{dropPrimaryIndexStmt}},

	// Drop primary key
	{ai.globalID(), dropPrimaryIndexStmt, model.StatePublic, true, []string{addPrimaryIndexStmt}, []string{dropPrimaryIndexStmt}},
	{ai.globalID(), dropPrimaryIndexStmt, model.StateWriteOnly, false, []string{addPrimaryIndexStmt}, []string{dropPrimaryIndexStmt}},
	{ai.globalID(), dropPrimaryIndexStmt, model.StateWriteOnly, false, []string{addPrimaryIndexStmt}, []string{dropPrimaryIndexStmt}},
	{ai.globalID(), dropPrimaryIndexStmt, model.StateDeleteOnly, false, []string{addPrimaryIndexStmt}, []string{dropPrimaryIndexStmt}},

	// Add unique key
	{ai.globalID(), addUniqueIndexStmt, model.StateNone, true, nil, []string{dropUniqueIndexStmt}},
	{ai.globalID(), addUniqueIndexStmt, model.StateDeleteOnly, true, nil, []string{dropUniqueIndexStmt}},
	{ai.globalID(), addUniqueIndexStmt, model.StateWriteOnly, true, nil, []string{dropUniqueIndexStmt}},
	{ai.globalID(), addUniqueIndexStmt, model.StateWriteReorganization, true, nil, []string{dropUniqueIndexStmt}},
	{ai.globalID(), addUniqueIndexStmt, model.StatePublic, false, nil, []string{dropUniqueIndexStmt}},

	// Add normal key
	{ai.globalID(), addIndexStmt, model.StateNone, true, nil, []string{dropIndexStmt}},
	{ai.globalID(), addIndexStmt, model.StateDeleteOnly, true, nil, []string{dropIndexStmt}},
	{ai.globalID(), addIndexStmt, model.StateWriteOnly, true, nil, []string{dropIndexStmt}},
	{ai.globalID(), addIndexStmt, model.StateWriteReorganization, true, nil, []string{dropIndexStmt}},
	{ai.globalID(), addIndexStmt, model.StatePublic, false, nil, []string{dropIndexStmt}},

	// Drop normal key
	{ai.globalID(), dropIndexStmt, model.StatePublic, true, []string{addIndexStmt}, []string{dropIndexStmt}},
	{ai.globalID(), dropIndexStmt, model.StateWriteOnly, false, []string{addIndexStmt}, []string{dropIndexStmt}},
	{ai.globalID(), dropIndexStmt, model.StateWriteOnly, false, []string{addIndexStmt}, []string{dropIndexStmt}},
	{ai.globalID(), dropIndexStmt, model.StateDeleteOnly, false, []string{addIndexStmt}, []string{dropIndexStmt}},
}

const addColumnStmt string = alterTableAddPrefix + "column t_col bigint default '1024';"
const dropColumnStmt string = alterTableDropPrefix + "column if exists t_col;"
const addColumnIdxStmt string = alterTableAddPrefix + "index idx_t_col(t_col);"
const alterColumnPrefix string = alterTableModifyPrefix + "column t_col "

var columnDDLStmtCase = [...]StmtCase{
	// Add column.
	{ai.globalID(), addColumnStmt, model.StateNone, true, nil, []string{dropColumnStmt}},
	{ai.globalID(), addColumnStmt, model.StateDeleteOnly, true, nil, []string{dropColumnStmt}},
	{ai.globalID(), addColumnStmt, model.StateWriteOnly, true, nil, []string{dropColumnStmt}},
	{ai.globalID(), addColumnStmt, model.StateWriteReorganization, true, nil, []string{dropColumnStmt}},
	{ai.globalID(), addColumnStmt, model.StatePublic, false, nil, []string{dropColumnStmt}},

	// Drop column.
	{ai.globalID(), dropColumnStmt, model.StatePublic, true, []string{addColumnStmt}, []string{dropColumnStmt}},
	{ai.globalID(), dropColumnStmt, model.StateDeleteOnly, false, []string{addColumnStmt}, nil},
	{ai.globalID(), dropColumnStmt, model.StateWriteOnly, false, []string{addColumnStmt}, nil},
	{ai.globalID(), dropColumnStmt, model.StateDeleteReorganization, false, []string{addColumnStmt}, nil},
	{ai.globalID(), dropColumnStmt, model.StateNone, false, []string{addColumnStmt}, nil},

	// Drop column with index.
	{ai.globalID(), dropColumnStmt, model.StatePublic, true, []string{addColumnStmt, addColumnIdxStmt}, []string{dropColumnStmt}},
	{ai.globalID(), dropColumnStmt, model.StateDeleteOnly, false, []string{addColumnStmt, addColumnIdxStmt}, []string{dropColumnStmt}},
	{ai.globalID(), dropColumnStmt, model.StateWriteOnly, false, []string{addColumnStmt, addColumnIdxStmt}, []string{dropColumnStmt}},
	{ai.globalID(), dropColumnStmt, model.StateDeleteReorganization, false, []string{addColumnStmt, addColumnIdxStmt}, []string{dropColumnStmt}},
	{ai.globalID(), dropColumnStmt, model.StateNone, false, []string{addColumnStmt, addColumnIdxStmt}, []string{dropColumnStmt}},

	// Modify column, no reorg.
	{ai.globalID(), alterColumnPrefix + "mediumint;", model.StateNone, true, []string{addColumnStmt}, []string{dropColumnStmt}},
	{ai.globalID(), alterColumnPrefix + "int;", model.StatePublic, false, []string{addColumnStmt}, []string{dropColumnStmt}},

	// Modify column, reorg.
	{ai.globalID(), alterColumnPrefix + " char(10);", model.StateNone, true, []string{addColumnStmt}, []string{dropColumnStmt}},
	{ai.globalID(), alterColumnPrefix + " char(10);", model.StateDeleteOnly, true, []string{addColumnStmt}, []string{dropColumnStmt}},
	{ai.globalID(), alterColumnPrefix + " char(10);", model.StateWriteOnly, true, []string{addColumnStmt}, []string{dropColumnStmt}},
	{ai.globalID(), alterColumnPrefix + " char(10);", model.StateWriteReorganization, true, []string{addColumnStmt}, []string{dropColumnStmt}},
	{ai.globalID(), alterColumnPrefix + " char(10);", model.StatePublic, false, []string{addColumnStmt}, []string{dropColumnStmt}},
}

const alterTablePartitionPrefix string = "alter table " + adminPauseTestPartitionTable
const alterTablePartitionAddPrefix string = alterTablePartitionPrefix + " add partition "
const alterTablePartitionDropPrefix string = alterTablePartitionPrefix + " drop partition "
const alterTablePartitionExchangePrefix string = alterTablePartitionPrefix + " exchange partition "

const alterTablePartitionAddPartition = alterTablePartitionAddPrefix + "(partition p7 values less than (200));"
const alterTablePartitionDropPartition = alterTablePartitionDropPrefix + "p7;"

var tablePartitionDDLStmtCase = [...]StmtCase{
	// Exchange partition.
	{ai.globalID(), alterTablePartitionExchangePrefix + "p6 with table " + adminPauseTestTable + ";", model.StateNone, true, []string{"set @@tidb_enable_exchange_partition=1;"}, nil},
	{ai.globalID(), alterTablePartitionExchangePrefix + "p6 with table " + adminPauseTestTable + ";", model.StatePublic, false, []string{"set @@tidb_enable_exchange_partition=1;"}, nil},

	// Add partition.
	{ai.globalID(), alterTablePartitionAddPartition, model.StateNone, true, nil, []string{alterTablePartitionDropPartition}},
	{ai.globalID(), alterTablePartitionAddPartition, model.StateReplicaOnly, true, nil, []string{alterTablePartitionDropPartition}},
	{ai.globalID(), alterTablePartitionAddPartition, model.StatePublic, false, nil, []string{alterTablePartitionDropPartition}},

	// Drop partition.
	{ai.globalID(), alterTablePartitionDropPartition, model.StatePublic, true, []string{alterTablePartitionAddPartition}, []string{alterTablePartitionDropPartition}},
	{ai.globalID(), alterTablePartitionDropPartition, model.StateDeleteOnly, false, []string{alterTablePartitionAddPartition}, []string{alterTablePartitionDropPartition}},
	{ai.globalID(), alterTablePartitionDropPartition, model.StateDeleteOnly, false, []string{alterTablePartitionAddPartition}, []string{alterTablePartitionDropPartition}},
	{ai.globalID(), alterTablePartitionDropPartition, model.StateDeleteReorganization, false, []string{alterTablePartitionAddPartition}, []string{alterTablePartitionDropPartition}},
	{ai.globalID(), alterTablePartitionDropPartition, model.StateNone, false, []string{alterTablePartitionAddPartition}, []string{alterTablePartitionDropPartition}},
}

const placementPolicy = "placement_policy"
const createPlacementPolicy = "create placement policy " + placementPolicy + " PRIMARY_REGION=\"cn-east-1\", REGIONS=\"cn-east-1\";"
const dropPlacementPolicy = "drop placement policy " + placementPolicy
const alterSchemaPolicy = "alter database " + testSchema + " placement policy = '" + placementPolicy + "';"

var placeRulDDLStmtCase = [...]StmtCase{
	{ai.globalID(), alterSchemaPolicy, model.StateNone, true, []string{createPlacementPolicy, createSchemaStmt}, []string{dropSchemaStmt, dropPlacementPolicy}},
	{ai.globalID(), alterSchemaPolicy, model.StatePublic, false, []string{createPlacementPolicy, createSchemaStmt}, []string{dropSchemaStmt, dropPlacementPolicy}},
}

func (stmtCase *StmtCase) simpleRunStmt(stmtKit *testkit.TestKit) {
	for _, prepareStmt := range stmtCase.preConditionStmts {
		stmtKit.MustExec(prepareStmt)
	}

	stmtKit.MustExec(stmtCase.stmt)
	Logger.Info("TestPauseCancelAndRerun: statement simple execution should have been finished successfully.")

	Logger.Info("TestPauseCancelAndRerun: statement simple rollback ...")
	for _, rollbackStmt := range stmtCase.rollbackStmts {
		_, _ = stmtKit.Exec(rollbackStmt)
	}
}
