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

package issyncer

import (
	"context"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"go.uber.org/zap"
)

// LoadHistoricalWithTS builds an isolated snapshot without publishing a schema
// version that the background schema syncer has not applied yet.
func (l *Loader) LoadHistoricalWithTS(ts uint64) (infoschema.InfoSchema, error) {
	snapshot := l.store.GetSnapshot(kv.NewVersion(ts))
	snapshot.SetOption(kv.TiKVClientReadTimeout, uint64(3000))
	m := meta.NewReader(snapshot)
	version, err := m.GetSchemaVersionWithNonEmptyDiff()
	if err != nil {
		return nil, err
	}
	schemaCacheSize := vardef.SchemaCacheSize.Load()
	failpoint.InjectCall("beforeFullLoadFetchSchemas")
	schemas, err := l.fetchAllSchemasWithTables(m, schemaCacheSize)
	if err != nil {
		return nil, err
	}
	policies, err := l.fetchPolicies(m)
	if err != nil {
		return nil, err
	}
	groups, err := l.fetchResourceGroups(m)
	if err != nil {
		return nil, err
	}
	builder := infoschema.NewBuilder(l, schemaCacheSize, l.sysExecutorFactory, infoschema.NewData(), schemaCacheSize > 0).
		WithCrossKS(l.crossKS)
	if err := builder.InitWithDBInfos(schemas, policies, groups, nil, version); err != nil {
		return nil, err
	}
	return builder.Build(ts), nil
}

// preloadMaskingPolicies warms the candidate schema before publishing it. A
// failure is retried by policy access and must not stop schema lease renewal.
func (l *Loader) preloadMaskingPolicies(is infoschema.InfoSchema, started time.Time) {
	failpoint.Inject("skipLoadMaskingPolicies", func() { failpoint.Return() })
	if !is.TableExists(ast.NewCIStr(mysql.SystemDB), ast.NewCIStr("tidb_masking_policy")) {
		return
	}
	budget := min(time.Second, l.schemaLease/4)
	parent := l.schemaLoadContext
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithDeadline(parent, started.Add(budget))
	defer cancel()
	if err := infoschema.PreloadMaskingPolicies(ctx, is); err != nil {
		l.logger.Warn("failed to preload masking policies", zap.Error(err))
	}
}
