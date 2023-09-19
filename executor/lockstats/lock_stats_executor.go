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

package lockstats

import (
	"context"
	"fmt"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/executor/internal/exec"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/util/chunk"
)

var _ exec.Executor = &LockExec{}

// LockExec represents a lock statistic executor.
type LockExec struct {
	exec.BaseExecutor
	// Tables is the list of tables to be locked.
	// It might contain partition names if we are locking partitions.
	// When locking partitions, Tables will only contain one table name.
	Tables []*ast.TableName
}

// Next implements the Executor Next interface.
func (e *LockExec) Next(_ context.Context, _ *chunk.Chunk) error {
	do := domain.GetDomain(e.Ctx())
	h := do.StatsHandle()
	if h == nil {
		return errors.New("Lock Stats: handle is nil")
	}
	if len(e.Tables) == 0 {
		return errors.New("Lock Stats: table should not empty")
	}
	is := do.InfoSchema()

	if e.onlyLockPartitions() {
		table := e.Tables[0]
		tid, pidNames, err := populatePartitionIDAndNames(table, table.PartitionNames, is)
		if err != nil {
			return err
		}

		tableName := fmt.Sprintf("%s.%s", table.Schema.L, table.Name.L)
		msg, err := h.LockPartitions(tid, tableName, pidNames)
		if err != nil {
			return err
		}
		if msg != "" {
			e.Ctx().GetSessionVars().StmtCtx.AppendWarning(errors.New(msg))
		}
	} else {
		tidAndNames, pidAndNames, err := populateTableAndPartitionIDs(e.Tables, is)
		if err != nil {
			return err
		}

		msg, err := h.LockTables(tidAndNames, pidAndNames)
		if err != nil {
			return err
		}
		if msg != "" {
			e.Ctx().GetSessionVars().StmtCtx.AppendWarning(errors.New(msg))
		}
	}

	return nil
}

func (e *LockExec) onlyLockPartitions() bool {
	return len(e.Tables) == 1 && len(e.Tables[0].PartitionNames) > 0
}

// Close implements the Executor Close interface.
func (*LockExec) Close() error {
	return nil
}

// Open implements the Executor Open interface.
func (*LockExec) Open(context.Context) error {
	return nil
}

// populatePartitionIDAndNames returns the table ID and partition IDs for the given table name and partition names.
func populatePartitionIDAndNames(
	table *ast.TableName,
	partitionNames []model.CIStr,
	is infoschema.InfoSchema,
) (int64, map[int64]string, error) {
	if len(partitionNames) == 0 {
		return 0, nil, errors.New("partition list should not be empty")
	}
	tbl, err := is.TableByName(table.Schema, table.Name)
	if err != nil {
		return 0, nil, err
	}

	pi := tbl.Meta().GetPartitionInfo()
	if pi == nil {
		return 0, nil, errors.Errorf("table %s is not a partition table",
			fmt.Sprintf("%s.%s", table.Schema.L, table.Name.L))
	}

	pidNames := make(map[int64]string, len(partitionNames))
	for _, partitionName := range partitionNames {
		pid, err := tables.FindPartitionByName(tbl.Meta(), partitionName.L)
		if err != nil {
			return 0, nil, err
		}
		pidNames[pid] = partitionName.L
	}

	return tbl.Meta().ID, pidNames, nil
}

// populateTableAndPartitionIDs returns table IDs and partition IDs for the given table names.
func populateTableAndPartitionIDs(
	tables []*ast.TableName,
	is infoschema.InfoSchema,
) (map[int64]string, map[int64]string, error) {
	if len(tables) == 0 {
		return nil, nil, errors.New("table list should not be empty")
	}

	tidAndNames := make(map[int64]string, len(tables))
	pidAndNames := make(map[int64]string, len(tables))

	for _, table := range tables {
		tbl, err := is.TableByName(table.Schema, table.Name)
		if err != nil {
			return nil, nil, err
		}
		tidAndNames[tbl.Meta().ID] = fmt.Sprintf("%s.%s", table.Schema.L, table.Name.L)

		pi := tbl.Meta().GetPartitionInfo()
		if pi == nil {
			continue
		}
		for _, p := range pi.Definitions {
			pidAndNames[p.ID] = genFullPartitionName(table, p.Name.L)
		}
	}

	return tidAndNames, pidAndNames, nil
}

func genFullPartitionName(table *ast.TableName, partitionName string) string {
	return fmt.Sprintf("%s.%s partition (%s)", table.Schema.L, table.Name.L, partitionName)
}
