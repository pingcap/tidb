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

package autoanalyze

import (
	"context"
	"sync"

	"github.com/ngaut/pools"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/domain/sqlsvrapi"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	tidbmeta "github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/session/syssession"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/statistics/handle"
	statsutil "github.com/pingcap/tidb/pkg/statistics/handle/util"
)

// This file runs an ANALYZE of a user keyspace on a DXF service node. The keyspace has no domain
// there: the statement session gets an info schema with only the table of the task meta, and an
// AnalyzeHandle is the stats handle.

// statementPoolSize is the number of idle sessions a statementPool keeps, more than a statement
// uses at once: a session that the pool drops would stay with the runtime until the statement ends.
const statementPoolSize = 1024

// statementPool is the session pool of the stats handle of one statement. Close waits for the users
// of its sessions: a killed statement returns before the workers that save its results do.
type statementPool struct {
	*syssession.AdvancedSessionPool
	runtime sqlsvrapi.Runtime
	mu      sync.Mutex
	closing bool
	users   sync.WaitGroup
	// taken are the sessions taken from the runtime.
	taken []pools.Resource
}

func (p *statementPool) enter() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closing {
		return errors.New("the stats handle of the statement is closing")
	}
	p.users.Add(1)
	return nil
}

// WithSession implements syssession.Pool.
func (p *statementPool) WithSession(fn func(*syssession.Session) error) error {
	if err := p.enter(); err != nil {
		return err
	}
	defer p.users.Done()
	return p.AdvancedSessionPool.WithSession(fn)
}

// waitIdle makes the pool refuse new users, and waits for the current ones.
func (p *statementPool) waitIdle() {
	p.mu.Lock()
	p.closing = true
	p.mu.Unlock()
	p.users.Wait()
}

func newStatementPool(runtime sqlsvrapi.Runtime) *statementPool {
	p := &statementPool{runtime: runtime}
	p.AdvancedSessionPool = syssession.NewAdvancedSessionPool(statementPoolSize, p.newSession)
	return p
}

// newSession takes a session from the runtime for the pool.
func (p *statementPool) newSession() (syssession.SessionContext, error) {
	res, err := p.runtime.SysSessionPool().Get()
	if err != nil {
		return nil, errors.Trace(err)
	}
	sctx, ok := res.(syssession.SessionContext)
	if !ok {
		p.runtime.SysSessionPool().Destroy(res)
		return nil, errors.Errorf("unexpected session type %T", res)
	}
	p.mu.Lock()
	p.taken = append(p.taken, res)
	p.mu.Unlock()
	return sctx, nil
}

// Close closes the sessions when nobody works in them any more, and makes them leave the runtime.
// They are not put back: they carry settings of the statement, and one of them its stats handle.
func (p *statementPool) Close() {
	p.waitIdle()
	p.AdvancedSessionPool.Close()
	p.mu.Lock()
	defer p.mu.Unlock()
	// The pool has closed the sessions. The runtime closes a session that
	// leaves it once more, which does no harm.
	for _, res := range p.taken {
		p.runtime.SysSessionPool().Destroy(res)
	}
	p.taken = nil
}

// runOnServiceNode runs the statement of meta with the runtime of the task
// keyspace, on a node that does not belong to that keyspace.
func runOnServiceNode(
	ctx context.Context,
	runtime sqlsvrapi.Runtime,
	localDom *domain.Domain,
	meta *TaskMeta,
) error {
	pool := newStatementPool(runtime)
	defer pool.Close()
	// The stats handle of this node lends its goroutine pool and its stats lease.
	localHandle := localDom.StatsHandle()
	statsHandle := handle.NewAnalyzeHandleWithoutCache(statsutil.NewSessionPool(pool), localHandle.Pool(), localHandle)

	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnStatsForegroundPriority)
	return statsutil.CallWithSCtx(pool, func(sctx sessionctx.Context) error {
		domain.BindAnalyzeStatsHandle(sctx, statsHandle)
		is, err := tableInfoSchema(sctx, meta.DBID, meta.TableID)
		if err != nil {
			return err
		}
		vars := sctx.GetSessionVars()
		// Only the info schema is set, not SnapshotTS, so the statement reads data as ANALYZE on
		// the stats owner does.
		vars.SnapshotInfoschema = is
		// The merge of the global stats at the end of the statement still reads it. Then the pool
		// can lend the session to a worker that saves results and reads system tables.
		defer func() { vars.SnapshotInfoschema = nil }()
		// The node's own stats handle only allocates the process ID of the statement.
		return runAnalyze(ctx, sctx, localHandle.ProcIDs(), localDom.SysProcTracker(), meta)
	})
}

// tableInfoSchema builds an info schema with only the table, read from the meta of the keyspace of
// sctx at the current TS. The statement uses this version of the table, as ANALYZE on a TiDB node
// uses the version at its start.
func tableInfoSchema(sctx sessionctx.Context, dbID, tableID int64) (infoschema.InfoSchema, error) {
	store := sctx.GetStore()
	ver, err := store.CurrentVersion(kv.GlobalTxnScope)
	if err != nil {
		return nil, errors.Trace(err)
	}
	snapshot := store.GetSnapshot(ver)
	snapshot.SetOption(kv.TiKVClientReadTimeout, uint64(3000)) // 3000ms, as the info schema loader.
	m := tidbmeta.NewReader(snapshot)
	db, err := m.GetDatabase(dbID)
	if err != nil {
		return nil, errors.Trace(err)
	}
	var tbl *model.TableInfo
	if db != nil && db.State == model.StatePublic {
		if tbl, err = m.GetTable(dbID, tableID); err != nil {
			return nil, errors.Trace(err)
		}
	}
	if tbl == nil || tbl.State != model.StatePublic {
		return nil, errors.Errorf("table %d of database %d doesn't exist", tableID, dbID)
	}
	schemaVersion, err := m.GetSchemaVersion()
	if err != nil {
		return nil, errors.Trace(err)
	}
	parent := sctx.GetLatestInfoSchema().(infoschema.InfoSchema)
	return infoschema.BuildForTables(parent, schemaVersion, ver.Ver, infoschema.TableWithDB{DB: db, Table: tbl})
}
