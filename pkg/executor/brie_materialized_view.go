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

package executor

import (
	"context"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/glue"
	"github.com/pingcap/tidb/br/pkg/restore"
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta/model"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/util/sqlescape"
)

// BRIEMaterializedViewSession shares metadata restore between BR and SQL RESTORE.
type BRIEMaterializedViewSession struct{ sessionctx.Context }

var _ glue.MaterializedViewSession = (*BRIEMaterializedViewSession)(nil)

// CreateMaterializedViewWithInfo rebuilds an MV without rendering its physical schema as SQL.
func (gs *BRIEMaterializedViewSession) CreateMaterializedViewWithInfo(
	ctx context.Context, dbName pmodel.CIStr, info *model.TableInfo,
) error {
	vars := gs.GetSessionVars()
	oldMode, oldZone, oldPrecision := vars.SQLMode, vars.TimeZone, vars.DivPrecisionIncrement
	defer func() { vars.SQLMode, vars.TimeZone, vars.DivPrecisionIncrement = oldMode, oldZone, oldPrecision }()
	mv := info.MaterializedView
	location, err := mv.DefinitionTimeZone.GetLocation()
	if err != nil {
		return errors.Trace(err)
	}
	vars.SQLMode, vars.TimeZone = mv.DefinitionSQLMode, location
	vars.DivPrecisionIncrement = mv.DefinitionDivPrecisionIncrement
	return gs.restoreMaterializedObject(ctx, dbName, info, func(table *model.TableInfo) error {
		return domain.GetDomain(gs.Context).DDLExecutor().CreateMaterializedViewWithInfo(
			gs.Context, dbName, table, ddl.WithOnExist(ddl.OnExistIgnore))
	})
}

// CreateMaterializedViewLogWithInfo rebuilds an MLog and initializes fresh purge state.
func (gs *BRIEMaterializedViewSession) CreateMaterializedViewLogWithInfo(
	ctx context.Context, dbName pmodel.CIStr, info *model.TableInfo,
) error {
	return gs.restoreMaterializedObject(ctx, dbName, info, func(table *model.TableInfo) error {
		return domain.GetDomain(gs.Context).DDLExecutor().CreateMaterializedViewLogWithInfo(
			gs.Context, dbName, table, ddl.WithOnExist(ddl.OnExistIgnore))
	})
}

// An earlier CREATE can outlive the BR process. An existing Building object is
// not a completed checkpoint: wait for Ready, or create it again after rollback.
func (gs *BRIEMaterializedViewSession) restoreMaterializedObject(
	ctx context.Context, dbName pmodel.CIStr, info *model.TableInfo, create func(*model.TableInfo) error,
) error {
	vars := gs.GetSessionVars()
	oldInternal, oldDB := vars.InRestrictedSQL, vars.CurrentDB
	vars.InRestrictedSQL, vars.CurrentDB = true, dbName.O
	defer func() { vars.InRestrictedSQL, vars.CurrentDB = oldInternal, oldDB }()
	dom := domain.GetDomain(gs.Context)
	oldQuery := gs.Value(sessionctx.QueryString)
	defer gs.SetValue(sessionctx.QueryString, oldQuery)
	for {
		if err := ctx.Err(); err != nil {
			return errors.Trace(err)
		}
		actual, err := dom.InfoSchema().TableByName(ctx, dbName, info.Name)
		if infoschema.ErrTableNotExists.Equal(err) {
			if mv := info.MaterializedView; mv != nil {
				// Also covers a parent MV just rebuilt during cascading restore.
				if err := restore.WaitTiFlashReplicaReady(ctx, dom, mv.BaseTableIDs[0]); err != nil {
					return err
				}
			}
			clone := info.Clone()
			// CREATE configures the replicas together with the physical table, but
			// their availability must be established on the new target.
			if replica := clone.TiFlashReplica; replica != nil {
				clone.TiFlashReplica = &model.TiFlashReplicaInfo{
					Count: replica.Count, LocationLabels: replica.LocationLabels,
				}
			}
			if err := gs.setMaterializedObjectJobQuery(ctx, dbName, clone); err != nil {
				return err
			}
			if err := create(clone); err != nil {
				return errors.Trace(err)
			}
			continue
		}
		if err != nil {
			return errors.Trace(err)
		}
		target := actual.Meta()
		if info.MaterializedView != nil {
			if target.MaterializedView == nil {
				return errors.Errorf("restored object %s.%s is not a materialized view", dbName.O, info.Name.O)
			}
			if target.MaterializedView.GetInitBuildState() == model.MViewInitBuildDeferred {
				return errors.Errorf("materialized view %s.%s has a deferred initial build", dbName.O, info.Name.O)
			}
			if len(target.MaterializedView.BaseTableIDs) != 1 ||
				target.MaterializedView.BaseTableIDs[0] != info.MaterializedView.BaseTableIDs[0] {
				return errors.Errorf("materialized view %s.%s has a different base", dbName.O, info.Name.O)
			}
			if target.State == model.StatePublic && target.MaterializedView.GetInitBuildState().IsReady() {
				break
			}
		} else {
			if target.MaterializedViewLog == nil ||
				target.MaterializedViewLog.BaseTableID != info.MaterializedViewLog.BaseTableID {
				return errors.Errorf("restored object %s.%s is not the expected materialized view log", dbName.O, info.Name.O)
			}
			if target.State == model.StatePublic {
				break
			}
		}
		timer := time.NewTimer(3 * time.Second)
		select {
		case <-ctx.Done():
			timer.Stop()
			return errors.Trace(ctx.Err())
		case <-timer.C:
		}
	}
	return nil
}

// DDL history still needs a statement describing the operation. This text is
// diagnostic only: physical columns, indexes and schedules come from TableInfo,
// and no restore SQL is parsed or executed.
func (gs *BRIEMaterializedViewSession) setMaterializedObjectJobQuery(
	ctx context.Context, dbName pmodel.CIStr, info *model.TableInfo,
) error {
	var query strings.Builder
	query.WriteString("/*from(br)*/ CREATE MATERIALIZED VIEW ")
	columns := make([]string, 0, len(info.Columns))
	if info.MaterializedView != nil {
		sqlescape.MustFormatSQL(&query, "%n.%n", dbName.O, info.Name.O)
		for _, column := range info.Columns {
			columns = append(columns, column.Name.O)
		}
	} else {
		base, ok := domain.GetDomain(gs.Context).InfoSchema().TableByID(ctx, info.MaterializedViewLog.BaseTableID)
		if !ok {
			return errors.Errorf("base table ID %d does not exist", info.MaterializedViewLog.BaseTableID)
		}
		sqlescape.MustFormatSQL(&query, "LOG ON %n.%n", dbName.O, base.Meta().Name.O)
		for _, column := range info.MaterializedViewLog.Columns {
			columns = append(columns, column.O)
		}
	}
	query.WriteByte('(')
	for i, column := range columns {
		if i > 0 {
			query.WriteByte(',')
		}
		sqlescape.MustFormatSQL(&query, "%n", column)
	}
	query.WriteByte(')')
	if info.MaterializedView != nil {
		query.WriteString(" AS " + info.MaterializedView.SQLContent)
	}
	gs.SetValue(sessionctx.QueryString, query.String())
	return nil
}
