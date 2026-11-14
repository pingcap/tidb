// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.

package task

import (
	"cmp"
	"context"
	"slices"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/glue"
	"github.com/pingcap/tidb/br/pkg/metautil"
	"github.com/pingcap/tidb/br/pkg/restore"
	"github.com/pingcap/tidb/br/pkg/stream"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"go.uber.org/zap"
)

// materializedViewRestorePlan retains source definitions separately from the
// tables passed to CreateTables. Objects are ordered by dependency.
type materializedViewRestorePlan struct {
	tables  map[int64]*metautil.Table
	objects []*metautil.Table
}

func hasMaterializedViewDefinition(info *model.TableInfo) bool {
	return info != nil &&
		(info.MaterializedView != nil || info.MaterializedViewLog != nil || info.MaterializedViewBase != nil)
}

func clearMaterializedViewInfo(info *model.TableInfo) {
	info.MaterializedView = nil
	info.MaterializedViewLog = nil
	info.MaterializedViewBase = nil
	info.MaterializedViewShadow = nil
	info.State = model.StatePublic
}

// indexMaterializedViewTables retains source definitions and identifies derived
// objects, including those whose own MV fields disappeared during DDL.
func indexMaterializedViewTables(tables []*metautil.Table) (*materializedViewRestorePlan, map[int64]bool) {
	p := &materializedViewRestorePlan{tables: make(map[int64]*metautil.Table)}
	derivedTables := make(map[int64]bool)
	for _, table := range tables {
		info := table.Info
		if info == nil || info.MaterializedViewShadow != nil {
			continue
		}
		p.tables[info.ID] = table
		// A reverse reference still identifies an MLog/MV whose own fields
		// disappeared during DDL. It must not become an ordinary base.
		if info.MaterializedView != nil || info.MaterializedViewLog != nil {
			derivedTables[info.ID] = true
		}
		if base := info.MaterializedViewBase; base != nil {
			if base.MLogID != 0 {
				derivedTables[base.MLogID] = true
			}
			for _, id := range base.MViewIDs {
				derivedTables[id] = true
			}
		}
		if log := info.MaterializedViewLog; log != nil {
			for _, id := range log.DependentMViewIDs {
				derivedTables[id] = true
			}
		}
	}
	return p, derivedTables
}

// newMaterializedViewRestorePlan selects the schemas/SSTs to import now and
// retains source definitions for later logical rebuild. Invalid derived
// objects are skipped; their ordinary bases can still be restored.
func newMaterializedViewRestorePlan(
	tables []*metautil.Table, pitr bool,
) (*materializedViewRestorePlan, []*metautil.Table, error) {
	p, derivedTables := indexMaterializedViewTables(tables)
	// PITR rebuilds from the recorder after replay, which may change or drop
	// these definitions. Only snapshot-only restore needs their dependencies.
	if !pitr {
		if err := p.collectRestorableObjects(tables, derivedTables); err != nil {
			return nil, nil, errors.Trace(err)
		}
	}
	// A base whose own MV fields disappeared can still be identified by its
	// dependents. Restore it as an ordinary public table as well.
	baseIDs := make(map[int64]bool)
	for _, table := range p.tables {
		if log := table.Info.MaterializedViewLog; log != nil {
			baseIDs[log.BaseTableID] = true
		}
		if mv := table.Info.MaterializedView; mv != nil {
			for _, id := range mv.BaseTableIDs {
				baseIDs[id] = true
			}
		}
	}
	newTables := make([]*metautil.Table, 0, len(tables))
	for _, source := range tables {
		table := p.tables[source.Info.ID]
		if table == nil || derivedTables[source.Info.ID] {
			continue
		}
		info := table.Info
		// Preserve the plan's definitions and schedules when preparing schemas.
		clone := *table
		clone.Info = info.Clone()
		if hasMaterializedViewDefinition(info) || baseIDs[info.ID] {
			clearMaterializedViewInfo(clone.Info)
		}
		newTables = append(newTables, &clone)
	}
	return p, newTables, nil
}

// collectRestorableObjects accepts each public MLog and ready MV only after
// its base is accepted. Appending accepted MVs as bases also handles cascades.
func (p *materializedViewRestorePlan) collectRestorableObjects(
	tables []*metautil.Table, derivedTables map[int64]bool,
) error {
	var pendingBases []*metautil.Table
	for _, source := range tables {
		table := p.tables[source.Info.ID]
		if table == nil || table.Info.State != model.StatePublic {
			continue
		}
		info := table.Info
		if !derivedTables[info.ID] && info.MaterializedViewBase != nil {
			pendingBases = append(pendingBases, table)
		}
	}
	for i := 0; i < len(pendingBases); i++ {
		base := pendingBases[i].Info
		log := p.tables[base.MaterializedViewBase.MLogID]
		if log == nil || log.Info.State != model.StatePublic || log.Info.MaterializedViewLog == nil ||
			log.Info.MaterializedViewLog.BaseTableID != base.ID {
			continue
		}
		p.objects = append(p.objects, log)
		for _, id := range base.MaterializedViewBase.MViewIDs {
			view := p.tables[id]
			if view == nil || view.Info.State != model.StatePublic {
				continue
			}
			// Both sides must agree on the dependency. DDL or filtering may
			// leave a missing view, a plain table, or a link to another base's MV.
			mv := view.Info.MaterializedView
			if mv == nil || !mv.GetInitBuildState().IsReady() {
				continue
			}
			if len(mv.BaseTableIDs) != 1 {
				return errors.Errorf("materialized view %s must have exactly one base table", view.Info.Name.O)
			}
			if mv.BaseTableIDs[0] != base.ID {
				continue
			}
			p.objects = append(p.objects, view)
			if view.Info.MaterializedViewBase != nil {
				pendingBases = append(pendingBases, view)
			}
		}
	}
	return nil
}

// restore rebuilds derived data and lets native DDL establish the new dependency IDs.
// Restore relies on automatically generated grouping indexes being immutable:
// renaming or dropping them is outside the supported MV schema contract.
func (p *materializedViewRestorePlan) restore(ctx context.Context, g glue.Glue, dom *domain.Domain, cfg *RestoreConfig) error {
	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnBR)
	if len(p.objects) == 0 {
		return nil
	}
	se, err := g.CreateSession(dom.Store())
	if err != nil {
		return errors.Trace(err)
	}
	defer se.Close()
	// A fresh BR session loads global execution settings on its first statement.
	// Trigger that before applying definition-time overrides and capturing MV job
	// settings; restoreMaterializedObject sets the current database for each object.
	rs, err := se.GetSessionCtx().GetSQLExecutor().ExecuteStmt(ctx, &ast.UseStmt{DBName: p.objects[0].DB.Name.O})
	if err != nil {
		return errors.Trace(err)
	}
	if rs != nil {
		if err := rs.Close(); err != nil {
			return errors.Trace(err)
		}
	}
	// Match ordinary restore sessions, including the default STRICT mode.
	vars := se.GetSessionCtx().GetSessionVars()
	vars.PlacementMode = variable.PlacementModeStrict
	if strings.EqualFold(cfg.WithPlacementPolicy, variable.PlacementModeIgnore) {
		vars.PlacementMode = variable.PlacementModeIgnore
	}
	creator, ok := se.(glue.MaterializedViewSession)
	if !ok {
		return errors.New("restore session does not support materialized view metadata")
	}
	var tiFlashStoreCount *uint64
	var tiFlashTableIDs []int64
	for _, table := range p.objects {
		info := table.Info.Clone()
		info.ID = 0
		info.MaterializedViewBase = nil
		info.MaterializedViewShadow = nil
		// In-progress source DDL must not leave unfinished columns or indexes in
		// the restored schema. MLog ADD COLUMN appends non-public columns until commit.
		info.Columns = slices.DeleteFunc(info.Columns, func(column *model.ColumnInfo) bool {
			return column.State != model.StatePublic
		})
		info.Indices = slices.DeleteFunc(info.Indices, func(index *model.IndexInfo) bool {
			return index.State != model.StatePublic
		})
		var baseID int64
		if info.MaterializedView != nil {
			baseID = info.MaterializedView.BaseTableIDs[0]
		} else {
			baseID = info.MaterializedViewLog.BaseTableID
		}
		base := p.tables[baseID]
		actualBase, err := dom.InfoSchema().TableByName(ctx, base.DB.Name, base.Info.Name)
		if err != nil {
			return errors.Annotatef(err, "failed to resolve base for materialized object %s.%s", table.DB.Name.O, info.Name.O)
		}
		if mv := info.MaterializedView; mv != nil {
			mv.BaseTableIDs = []int64{actualBase.Meta().ID}
		} else {
			info.MaterializedViewLog.BaseTableID = actualBase.Meta().ID
			info.MaterializedViewLog.DependentMViewIDs = nil
			info.Name = model.MaterializedViewLogTableName(actualBase.Meta().Name)
		}
		if replica := info.TiFlashReplica; replica != nil {
			if tiFlashStoreCount == nil {
				count, err := infoschema.GetTiFlashStoreCount(dom.Store())
				if err != nil {
					return errors.Trace(err)
				}
				tiFlashStoreCount = &count
			}
			if replica.Count > *tiFlashStoreCount {
				log.Warn("skip materialized object TiFlash replica: insufficient stores",
					zap.String("table", table.DB.Name.O+"."+info.Name.O),
					zap.Uint64("replicas", replica.Count), zap.Uint64("stores", *tiFlashStoreCount))
				info.TiFlashReplica = nil
			}
		}
		if info.MaterializedView != nil {
			err = creator.CreateMaterializedViewWithInfo(ctx, table.DB.Name, info)
		} else {
			err = creator.CreateMaterializedViewLogWithInfo(ctx, table.DB.Name, info)
		}
		if err != nil {
			return errors.Annotatef(err, "failed to restore materialized object %s.%s", table.DB.Name.O, info.Name.O)
		}
		if cfg.WaitTiflashReady {
			actual, err := dom.InfoSchema().TableByName(ctx, table.DB.Name, info.Name)
			if err != nil {
				return errors.Trace(err)
			}
			if replica := actual.Meta().TiFlashReplica; replica != nil && replica.Count > 0 {
				tiFlashTableIDs = append(tiFlashTableIDs, actual.Meta().ID)
			}
		}
	}
	// These replicas are created after the ordinary snapshot waiting pipeline.
	// Include reused objects so checkpoint retries honor the same wait option.
	if len(tiFlashTableIDs) > 0 {
		log.Info("waiting for materialized object TiFlash replicas", zap.Int("tables", len(tiFlashTableIDs)))
	}
	return restore.WaitTiFlashReplicaReady(ctx, dom, tiFlashTableIDs...)
}

// repairMaterializedViews runs after data replay, index repair and TiFlash restoration. Native
// DDL builds the data and establishes all target IDs in dependency order.
func repairMaterializedViews(
	ctx context.Context, g glue.Glue, dom *domain.Domain, cfg *RestoreConfig, sr *stream.SchemasReplace,
) error {
	recorder := sr.MaterializedViewRecorder
	tables := make([]*metautil.Table, 0, len(recorder.Tables))
	for _, table := range recorder.Tables {
		db := sr.DbMap[table.DBID]
		if db == nil || !cfg.TableFilter.MatchTable(db.Name, table.Info.Name.O) {
			continue
		}
		actualDB, ok := dom.InfoSchema().SchemaByName(pmodel.NewCIStr(db.Name))
		if !ok || actualDB.ID != db.DbID {
			continue
		}
		if table.Info.MaterializedView == nil && table.Info.MaterializedViewLog == nil {
			replacement := db.TableMap[table.Info.ID]
			if replacement == nil {
				continue
			}
			actual, exists := dom.InfoSchema().TableByID(ctx, replacement.TableID)
			if !exists {
				continue
			}
			// Use the final base name after replay, including any rename.
			info := table.Info.Clone()
			info.Name = actual.Meta().Name
			tables = append(tables, &metautil.Table{DB: actualDB, Info: info})
		} else {
			tables = append(tables, &metautil.Table{DB: actualDB, Info: table.Info})
		}
	}
	slices.SortFunc(tables, func(a, b *metautil.Table) int { return cmp.Compare(a.Info.ID, b.Info.ID) })
	plan, derivedTables := indexMaterializedViewTables(tables)
	if err := plan.collectRestorableObjects(tables, derivedTables); err != nil {
		return err
	}
	return plan.restore(ctx, g, dom, cfg)
}
