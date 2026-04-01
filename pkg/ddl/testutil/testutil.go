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

package testutil

import (
	"context"
	"fmt"
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/ddl/logutil"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/model"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/session"
	sessiontypes "github.com/pingcap/tidb/pkg/session/types"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessiontxn"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// SessionExecInGoroutine export for testing.
func SessionExecInGoroutine(s kv.Storage, dbName, sql string, done chan error) {
	ExecMultiSQLInGoroutine(s, dbName, []string{sql}, done)
}

// ExecMultiSQLInGoroutine exports for testing.
func ExecMultiSQLInGoroutine(s kv.Storage, dbName string, multiSQL []string, done chan error) {
	go func() {
		se, err := session.CreateSession4Test(s)
		if err != nil {
			done <- errors.Trace(err)
			return
		}
		defer se.Close()
		_, err = se.Execute(context.Background(), "use "+dbName)
		if err != nil {
			done <- errors.Trace(err)
			return
		}
		for _, sql := range multiSQL {
			rs, err := se.Execute(context.Background(), sql)
			if err != nil {
				done <- errors.Trace(err)
				return
			}
			if rs != nil {
				done <- errors.Errorf("RecordSet should be empty")
				return
			}
			done <- nil
		}
	}()
}

// ExtractAllTableHandles extracts all handles of a given table.
func ExtractAllTableHandles(se sessiontypes.Session, dbName, tbName string) ([]int64, error) {
	dom := domain.GetDomain(se)
	tbl, err := dom.InfoSchema().TableByName(context.Background(), pmodel.NewCIStr(dbName), pmodel.NewCIStr(tbName))
	if err != nil {
		return nil, err
	}
	err = sessiontxn.NewTxn(context.Background(), se)
	if err != nil {
		return nil, err
	}

	var allHandles []int64
	err = tables.IterRecords(tbl, se, nil,
		func(h kv.Handle, _ []types.Datum, _ []*table.Column) (more bool, err error) {
			allHandles = append(allHandles, h.IntValue())
			return true, nil
		})
	return allHandles, err
}

// FindIdxInfo is to get IndexInfo by index name.
func FindIdxInfo(dom *domain.Domain, dbName, tbName, idxName string) *model.IndexInfo {
	tbl, err := dom.InfoSchema().TableByName(context.Background(), pmodel.NewCIStr(dbName), pmodel.NewCIStr(tbName))
	if err != nil {
		logutil.DDLLogger().Warn("cannot find table", zap.String("dbName", dbName), zap.String("tbName", tbName))
		return nil
	}
	return tbl.Meta().FindIndexByName(idxName)
}

// SubStates is a slice of SchemaState.
type SubStates = []model.SchemaState

// MatchCancelState is used to test whether the cancel state matches.
func MatchCancelState(t *testing.T, job *model.Job, cancelState any, sql string) bool {
	switch v := cancelState.(type) {
	case model.SchemaState:
		if job.Type == model.ActionMultiSchemaChange {
			msg := fmt.Sprintf("unexpected multi-schema change(sql: %s, cancel state: %s, job: %s)", sql, v, job.String())
			require.Failf(t, msg, "use []model.SchemaState as cancel states instead")
			return false
		}
		return job.SchemaState == v
	case SubStates: // For multi-schema change sub-jobs.
		if job.MultiSchemaInfo == nil {
			msg := fmt.Sprintf("not multi-schema change(sql: %s, cancel state: %v, job: %s)", sql, v, job.String())
			require.Failf(t, msg, "use model.SchemaState as the cancel state instead")
			return false
		}
		require.Equal(t, len(job.MultiSchemaInfo.SubJobs), len(v), sql)
		for i, subJobSchemaState := range v {
			if job.MultiSchemaInfo.SubJobs[i].SchemaState != subJobSchemaState {
				return false
			}
		}
		return true
	default:
		return false
	}
}

func checkTableState(t *testing.T, store kv.Storage, dbInfo *model.DBInfo, tblInfo *model.TableInfo, state model.SchemaState) {
	require.NoError(t, kv.RunInNewTxn(kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL), store, false, func(_ context.Context, txn kv.Transaction) error {
		m := meta.NewMutator(txn)
		info, err := m.GetTable(dbInfo.ID, tblInfo.ID)
		require.NoError(t, err)

		if state == model.StateNone {
			require.NoError(t, err)
			return nil
		}

		require.Equal(t, info.Name, tblInfo.Name)
		require.Equal(t, info.State, state)
		return nil
	}))
}

// CheckTableMode checks the table mode of a table in the store.
func CheckTableMode(t *testing.T, store kv.Storage, dbInfo *model.DBInfo, tblInfo *model.TableInfo, mode model.TableMode) {
	err := kv.RunInNewTxn(kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL), store, false, func(_ context.Context, txn kv.Transaction) error {
		tt := meta.NewMutator(txn)
		info, err := tt.GetTable(dbInfo.ID, tblInfo.ID)
		require.NoError(t, err)
		require.NotNil(t, info)
		require.Equal(t, mode, info.Mode)
		return nil
	})
	require.NoError(t, err)
}

// SetTableMode sets the table mode of a table in the store.
func SetTableMode(
	ctx sessionctx.Context,
	t *testing.T,
	store kv.Storage,
	de ddl.Executor,
	dbInfo *model.DBInfo,
	tblInfo *model.TableInfo,
	mode model.TableMode,
) error {
	args := &model.AlterTableModeArgs{
		TableMode: mode,
		SchemaID:  dbInfo.ID,
		TableID:   tblInfo.ID,
	}
	err := de.AlterTableMode(ctx, args)
	if err == nil {
		checkTableState(t, store, dbInfo, tblInfo, model.StatePublic)
		CheckTableMode(t, store, dbInfo, tblInfo, mode)
	}

	return err
}

// GetTableInfoByTxn get table info by transaction.
func GetTableInfoByTxn(t *testing.T, store kv.Storage, dbID int64, tableID int64) *model.TableInfo {
	var (
		tableInfo *model.TableInfo
		err       error
	)
	err = kv.RunInNewTxn(kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL), store, true, func(_ context.Context, txn kv.Transaction) error {
		m := meta.NewMutator(txn)
		_, err = m.GetDatabase(dbID)
		require.NoError(t, err)
		tableInfo, err = m.GetTable(dbID, tableID)
		require.NoError(t, err)
		require.NotNil(t, tableInfo)
		return nil
	})
	return tableInfo
}

// RefreshMeta sets the table mode of a table in the store.
func RefreshMeta(
	ctx sessionctx.Context,
	t *testing.T,
	de ddl.Executor,
	dbID, tableID int64,
	dbName, tableName string,
) {
	args := &model.RefreshMetaArgs{
		SchemaID:      dbID,
		TableID:       tableID,
		InvolvedDB:    dbName,
		InvolvedTable: tableName,
	}
	err := de.RefreshMeta(ctx, args)
	require.NoError(t, err)
}
