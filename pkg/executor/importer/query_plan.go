// Copyright 2026 PingCAP, Inc.
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

package importer

import (
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/infoschema"
	tidbkv "github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/sessionstates"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
)

// QueryPlan records source metadata and session settings after tenant privilege checks.
type QueryPlan struct {
	ReadTS        uint64
	SchemaVersion int64
	Timestamp     int64
	Keyspace      string
	// Tables lists the included source tables.
	Tables []SourceTable
	// SessionStates are the states of the submitting session.
	SessionStates *sessionstates.SessionStates
}

// SourceTable identifies a source table definition of an import query.
type SourceTable struct {
	DBID     int64
	ID       int64
	Revision uint64
}

// importQueryWorkerVars are decided by the worker instead of the submitter:
// the snapshot and transaction it reads in, and its memory budget.
var importQueryWorkerVars = map[string]struct{}{
	vardef.TiDBSnapshot:             {},
	vardef.TiDBReadStaleness:        {},
	vardef.TiDBTxnReadTS:            {},
	vardef.TiDBEnableExternalTSRead: {},
	vardef.AutoCommit:               {},
	vardef.TiDBTxnMode:              {},
	vardef.TxnIsolation:             {},
	vardef.TransactionIsolation:     {},
	vardef.TxReadOnly:               {},
	vardef.TransactionReadOnly:      {},
	vardef.TiDBMemQuotaQuery:        {},
}

// RestoreStates restores the captured session states into the worker session
// sctx, except the variables in importQueryWorkerVars.
func (q *QueryPlan) RestoreStates(ctx context.Context, sctx sessionctx.Context) error {
	vars := sctx.GetSessionVars()
	states := *q.SessionStates
	states.SystemVars = make(map[string]string, len(q.SessionStates.SystemVars))
	for name, value := range q.SessionStates.SystemVars {
		if _, ok := importQueryWorkerVars[name]; ok {
			continue
		}
		// Internal session variables cannot be set in a session.
		if sv := variable.GetSysVar(name); sv != nil && sv.InternalSessionVariable {
			continue
		}
		states.SystemVars[name] = value
	}
	// The status tells whether the submitter is in a transaction, which the
	// worker's session is not, and the warnings belong to the statements of the
	// submitter before.
	states.Status = uint32(vars.Status())
	states.Warnings = nil
	return sctx.DecodeStates(ctx, &states)
}

// BuildMinimalInfoSchema builds an info schema that holds only the source
// tables at q.ReadTS, and fails if a table changed after the submitter resolved
// the query.
func (q *QueryPlan) BuildMinimalInfoSchema(
	store tidbkv.Storage, parent infoschema.InfoSchema,
) (infoschema.InfoSchema, error) {
	m := meta.NewReader(store.GetSnapshot(tidbkv.NewVersion(q.ReadTS)))
	dbByID := make(map[int64]*model.DBInfo)
	dbs := make([]*model.DBInfo, 0, 1)
	for _, src := range q.Tables {
		db := dbByID[src.DBID]
		if db == nil {
			var err error
			if db, err = m.GetDatabase(src.DBID); err != nil {
				return nil, err
			}
			if db == nil {
				return nil, errors.Errorf("source database %d not found at the import query snapshot", src.DBID)
			}
			dbByID[src.DBID] = db
			dbs = append(dbs, db)
		}
		tbl, err := m.GetTable(src.DBID, src.ID)
		if err != nil {
			return nil, err
		}
		// The submitter resolved the query with this revision.
		if tbl == nil || tbl.Revision != src.Revision {
			return nil, errors.Errorf("source table %d changed before the import query snapshot", src.ID)
		}
		infoschema.ConvertOldVersionUTF8ToUTF8MB4IfNeed(tbl)
		infoschema.ConvertCharsetCollateToLowerCaseIfNeed(tbl)
		tbl.DBID = db.ID
		tbl.TableCacheStatusType = model.TableCacheStatusDisable
		db.Deprecated.Tables = append(db.Deprecated.Tables, tbl)
	}
	b := infoschema.NewBuilder(parent.GetAutoIDRequirement(), 0, nil, infoschema.NewData(), false).
		WithCrossKS(true)
	if err := b.InitWithDBInfos(dbs, parent.AllPlacementPolicies(), nil, nil, q.SchemaVersion); err != nil {
		return nil, err
	}
	return b.Build(q.ReadTS), nil
}
