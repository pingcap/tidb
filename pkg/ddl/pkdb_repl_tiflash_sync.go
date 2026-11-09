package ddl

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/pingcap/tidb/pkg/ddl/placement"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	pkdbrepl "github.com/pingcap/tidb/pkg/domain/pkdb_repl"
	"github.com/pingcap/tidb/pkg/infoschema"
	infoschemacontext "github.com/pingcap/tidb/pkg/infoschema/context"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

// tiflashPlacementSyncMgr syncs TiFlash placement rules from infoschema to PD
// in the log replication standby cluster. It runs in standby mode where DDL
// workers are blocked, so it directly calls PD APIs instead of submitting DDL jobs.
type tiflashPlacementSyncMgr struct {
	ctx       context.Context
	infoCache *infoschema.InfoCache
	wg        util.WaitGroupWrapper
}

func newTiFlashPlacementSyncMgr(
	ctx context.Context,
	infoCache *infoschema.InfoCache,
) *tiflashPlacementSyncMgr {
	return &tiflashPlacementSyncMgr{
		ctx:       ctx,
		infoCache: infoCache,
	}
}

func (m *tiflashPlacementSyncMgr) start() {
	m.wg.RunWithLog(func() {
		for {
			select {
			case <-m.ctx.Done():
				return
			case <-pkdbrepl.StandbyTiFlashPlacementSyncTickCh:
				m.runOnce()
			}
		}
	})
}

func (m *tiflashPlacementSyncMgr) close() {
	m.wg.Wait()
}

func (m *tiflashPlacementSyncMgr) runOnce() {
	schema := m.infoCache.GetLatest()
	if schema == nil {
		return
	}
	if err := syncTiFlashPlacementRules(m.ctx, schema); err != nil {
		logutil.BgLogger().Warn("standby tiflash placement sync failed", zap.Error(err))
	}
}

// syncTiFlashPlacementRules performs one round of TiFlash placement rule sync.
// It is exported for testing.
func syncTiFlashPlacementRules(ctx context.Context, schema infoschema.InfoSchema) error {
	// Step 1: Ensure TiFlash rule group config is set.
	if err := infosync.SetTiFlashGroupConfig(ctx); err != nil {
		logutil.BgLogger().Warn("standby tiflash sync: SetTiFlashGroupConfig failed", zap.Error(err))
		return err
	}

	// Step 2: Collect all tables with TiFlash replicas.
	type tableEntry struct {
		tblInfo *model.TableInfo
		dbInfo  *model.DBInfo
	}
	var tables []tableEntry
	for _, dbResult := range schema.ListTablesWithSpecialAttribute(infoschemacontext.TiFlashAttribute) {
		db, ok := schema.SchemaByName(dbResult.DBName)
		if !ok {
			continue
		}
		for _, tblInfo := range dbResult.TableInfos {
			if tblInfo.TiFlashReplica == nil || tblInfo.TiFlashReplica.Count == 0 {
				continue
			}
			tables = append(tables, tableEntry{tblInfo: tblInfo, dbInfo: db})
		}
	}

	// Step 3: Sync rules for each table and track known physical IDs.
	knownPhysicalIDs := make(map[int64]struct{})
	for _, entry := range tables {
		syncTiFlashRulesForTable(ctx, entry.tblInfo, knownPhysicalIDs)
	}

	// Step 4: Clean up obsolete rules for live objects that no longer have
	// TiFlash replicas. Dropped/truncated objects are cleaned later by GC so
	// flashback/recover semantics are preserved.
	cleanObsoleteTiFlashRules(ctx, schema, knownPhysicalIDs)

	return nil
}

func syncTiFlashRulesForTable(
	ctx context.Context,
	tblInfo *model.TableInfo,
	knownPhysicalIDs map[int64]struct{},
) {
	count := tblInfo.TiFlashReplica.Count
	labels := tblInfo.TiFlashReplica.LocationLabels

	if pi := tblInfo.GetPartitionInfo(); pi != nil {
		// Partitioned table: sync rules for each partition.
		syncPartitionRules(ctx, pi.Definitions, false, count, labels, tblInfo.ID, knownPhysicalIDs)
		syncPartitionRules(ctx, pi.AddingDefinitions, true, count, labels, tblInfo.ID, knownPhysicalIDs)
	} else {
		// Non-partitioned table.
		knownPhysicalIDs[tblInfo.ID] = struct{}{}
		rule, err := infosync.GetPlacementRule(ctx, tblInfo.ID)
		if err != nil {
			logutil.BgLogger().Warn("standby tiflash sync: get rule failed",
				zap.Int64("tableID", tblInfo.ID), zap.Error(err))
			return
		}
		if rule == nil || len(rule.ID) == 0 || rule.Count != int(count) ||
			!slices.Equal(rule.LocationLabels, labels) {
			if err := infosync.ConfigureTiFlashPDForTable(tblInfo.ID, count, &labels); err != nil {
				logutil.BgLogger().Error("standby tiflash sync: configure table failed",
					zap.Int64("tableID", tblInfo.ID), zap.Error(err))
			} else {
				logutil.BgLogger().Info("standby tiflash sync: configured placement rule for table",
					zap.Int64("tableID", tblInfo.ID), zap.Uint64("count", count))
			}
		}
	}
}

func syncPartitionRules(
	ctx context.Context,
	definitions []model.PartitionDefinition,
	accel bool,
	count uint64,
	labels []string,
	tableID int64,
	knownPhysicalIDs map[int64]struct{},
) {
	var unsyncedDefs []model.PartitionDefinition
	for _, p := range definitions {
		knownPhysicalIDs[p.ID] = struct{}{}
		rule, err := infosync.GetPlacementRule(ctx, p.ID)
		if err != nil {
			logutil.BgLogger().Warn("standby tiflash sync: get partition rule failed",
				zap.Int64("partitionID", p.ID), zap.Error(err))
			continue
		}
		if rule == nil || len(rule.ID) == 0 || rule.Count != int(count) ||
			!slices.Equal(rule.LocationLabels, labels) {
			unsyncedDefs = append(unsyncedDefs, p)
		}
	}
	if len(unsyncedDefs) > 0 {
		if err := infosync.ConfigureTiFlashPDForPartitions(accel, &unsyncedDefs, count, &labels, tableID); err != nil {
			logutil.BgLogger().Error("standby tiflash sync: configure partitions failed",
				zap.Int64("tableID", tableID), zap.Int("numPartitions", len(unsyncedDefs)), zap.Error(err))
		} else {
			logutil.BgLogger().Info("standby tiflash sync: configured placement rules for partitions",
				zap.Int64("tableID", tableID), zap.Int("numPartitions", len(unsyncedDefs)), zap.Uint64("count", count))
		}
	}
}

func cleanObsoleteTiFlashRules(
	ctx context.Context,
	schema infoschema.InfoSchema,
	knownPhysicalIDs map[int64]struct{},
) {
	rules, err := infosync.GetTiFlashGroupRules(ctx, placement.TiFlashRuleGroupID)
	if err != nil {
		logutil.BgLogger().Warn("standby tiflash sync: get group rules failed", zap.Error(err))
		return
	}
	var obsoleteIDs []int64
	for _, rule := range rules {
		physicalID, parseErr := parsePhysicalIDFromRuleID(rule.ID)
		if parseErr != nil {
			continue
		}
		if _, exists := knownPhysicalIDs[physicalID]; exists {
			continue
		}
		if shouldDeleteTiFlashRule(ctx, schema, physicalID) {
			obsoleteIDs = append(obsoleteIDs, physicalID)
		}
	}
	if len(obsoleteIDs) > 0 {
		if err := infosync.DeleteTiFlashPlacementRules(ctx, obsoleteIDs); err != nil {
			logutil.BgLogger().Error("standby tiflash sync: delete obsolete rules failed",
				zap.Int64s("physicalTableIDs", obsoleteIDs), zap.Error(err))
		} else {
			logutil.BgLogger().Info("standby tiflash sync: deleted obsolete rules",
				zap.Int64s("physicalTableIDs", obsoleteIDs))
		}
	}
}

// shouldDeleteTiFlashRule reports whether the sync can delete a TiFlash rule.
// It only deletes rules for live tables or partitions that no longer need
// TiFlash replicas. A physical ID missing from InfoSchema may belong to a
// dropped or truncated object that is still recoverable, so its rule must be
// retained until doGCPlacementRules removes it after delete-range GC.
func shouldDeleteTiFlashRule(ctx context.Context, schema infoschema.InfoSchema, physicalID int64) bool {
	if tbl, ok := schema.TableByID(ctx, physicalID); ok {
		return tbl.Meta().TiFlashReplica == nil || tbl.Meta().TiFlashReplica.Count == 0
	}
	if tableID, ok := schema.TableIDByPartitionID(physicalID); ok {
		if tbl, ok := schema.TableByID(ctx, tableID); ok {
			return tbl.Meta().TiFlashReplica == nil || tbl.Meta().TiFlashReplica.Count == 0
		}
	}
	return false
}

// parsePhysicalIDFromRuleID extracts the physical table ID from a TiFlash
// placement rule ID. The rule ID format is "table-<id>-r" (v1) or
// "keyspace-<ks>-table-<id>-r" (v2).
func parsePhysicalIDFromRuleID(ruleID string) (int64, error) {
	idx := strings.Index(ruleID, "table-")
	if idx < 0 {
		return 0, fmt.Errorf("invalid rule ID format: %s", ruleID)
	}
	suffix := ruleID[idx+len("table-"):]
	if !strings.HasSuffix(suffix, "-r") {
		return 0, fmt.Errorf("invalid rule ID format: %s", ruleID)
	}
	idStr := suffix[:len(suffix)-2]
	return strconv.ParseInt(idStr, 10, 64)
}
