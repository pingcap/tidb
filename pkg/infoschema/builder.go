// Copyright 2016 PingCAP, Inc.
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

package infoschema

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/ngaut/pools"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/autoid"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/util/domainutil"
	"github.com/pingcap/tidb/pkg/util/intest"
)

// Builder builds a new InfoSchema.
type Builder struct {
	enableV2 bool
	infoschemaV2
	// dbInfos do not need to be copied everytime applying a diff, instead,
	// they can be copied only once over the whole lifespan of Builder.
	// This map will indicate which DB has been copied, so that they
	// don't need to be copied again.
	dirtyDB map[string]bool

	// Used by autoid allocators
	autoid.Requirement

	factory func() (pools.Resource, error)
	bundleInfoBuilder
	infoData *Data
}

// ApplyDiff applies SchemaDiff to the new InfoSchema.
// Return the detail updated table IDs that are produced from SchemaDiff and an error.
func (b *Builder) ApplyDiff(m *meta.Meta, diff *model.SchemaDiff) ([]int64, error) {
	b.schemaMetaVersion = diff.Version
	switch diff.Type {
	case model.ActionCreateSchema:
		return nil, applyCreateSchema(b, m, diff)
	case model.ActionDropSchema:
		return applyDropSchema(b, diff), nil
	case model.ActionRecoverSchema:
		return applyRecoverSchema(b, m, diff)
	case model.ActionModifySchemaCharsetAndCollate:
		return nil, applyModifySchemaCharsetAndCollate(b, m, diff)
	case model.ActionModifySchemaDefaultPlacement:
		return nil, applyModifySchemaDefaultPlacement(b, m, diff)
	case model.ActionCreatePlacementPolicy:
		return nil, applyCreatePolicy(b, m, diff)
	case model.ActionDropPlacementPolicy:
		return applyDropPolicy(b, diff.SchemaID), nil
	case model.ActionAlterPlacementPolicy:
		return applyAlterPolicy(b, m, diff)
	case model.ActionCreateResourceGroup:
		return nil, applyCreateOrAlterResourceGroup(b, m, diff)
	case model.ActionAlterResourceGroup:
		return nil, applyCreateOrAlterResourceGroup(b, m, diff)
	case model.ActionDropResourceGroup:
		return applyDropResourceGroup(b, m, diff), nil
	case model.ActionTruncateTablePartition, model.ActionTruncateTable:
		return applyTruncateTableOrPartition(b, m, diff)
	case model.ActionDropTable, model.ActionDropTablePartition:
		return applyDropTableOrPartition(b, m, diff)
	case model.ActionRecoverTable:
		return applyRecoverTable(b, m, diff)
	case model.ActionCreateTables:
		return applyCreateTables(b, m, diff)
	case model.ActionReorganizePartition, model.ActionRemovePartitioning,
		model.ActionAlterTablePartitioning:
		return applyReorganizePartition(b, m, diff)
	case model.ActionExchangeTablePartition:
		return applyExchangeTablePartition(b, m, diff)
	case model.ActionFlashbackCluster:
		return []int64{-1}, nil
	default:
		return applyDefaultAction(b, m, diff)
	}
}

func (b *Builder) applyCreateTables(m *meta.Meta, diff *model.SchemaDiff) ([]int64, error) {
	return b.applyAffectedOpts(m, make([]int64, 0, len(diff.AffectedOpts)), diff, model.ActionCreateTable)
}

func applyTruncateTableOrPartition(b *Builder, m *meta.Meta, diff *model.SchemaDiff) ([]int64, error) {
	tblIDs, err := applyTableUpdate(b, m, diff)
	if err != nil {
		return nil, errors.Trace(err)
	}

	// bundle ops
	if diff.Type == model.ActionTruncateTable {
		b.deleteBundle(b.infoSchema, diff.OldTableID)
		b.markTableBundleShouldUpdate(diff.TableID)
	}

	for _, opt := range diff.AffectedOpts {
		if diff.Type == model.ActionTruncateTablePartition {
			// Reduce the impact on DML when executing partition DDL. eg.
			// While session 1 performs the DML operation associated with partition 1,
			// the TRUNCATE operation of session 2 on partition 2 does not cause the operation of session 1 to fail.
			tblIDs = append(tblIDs, opt.OldTableID)
			b.markPartitionBundleShouldUpdate(opt.TableID)
		}
		b.deleteBundle(b.infoSchema, opt.OldTableID)
	}
	return tblIDs, nil
}

func applyDropTableOrPartition(b *Builder, m *meta.Meta, diff *model.SchemaDiff) ([]int64, error) {
	tblIDs, err := applyTableUpdate(b, m, diff)
	if err != nil {
		return nil, errors.Trace(err)
	}

	// bundle ops
	b.markTableBundleShouldUpdate(diff.TableID)
	for _, opt := range diff.AffectedOpts {
		b.deleteBundle(b.infoSchema, opt.OldTableID)
	}
	return tblIDs, nil
}

func applyReorganizePartition(b *Builder, m *meta.Meta, diff *model.SchemaDiff) ([]int64, error) {
	tblIDs, err := applyTableUpdate(b, m, diff)
	if err != nil {
		return nil, errors.Trace(err)
	}

	// bundle ops
	for _, opt := range diff.AffectedOpts {
		if opt.OldTableID != 0 {
			b.deleteBundle(b.infoSchema, opt.OldTableID)
		}
		if opt.TableID != 0 {
			b.markTableBundleShouldUpdate(opt.TableID)
		}
		// TODO: Should we also check markPartitionBundleShouldUpdate?!?
	}
	return tblIDs, nil
}

func applyExchangeTablePartition(b *Builder, m *meta.Meta, diff *model.SchemaDiff) ([]int64, error) {
	// It is not in StatePublic.
	if diff.OldTableID == diff.TableID && diff.OldSchemaID == diff.SchemaID {
		ntIDs, err := applyTableUpdate(b, m, diff)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if diff.AffectedOpts == nil || diff.AffectedOpts[0].OldSchemaID == 0 {
			return ntIDs, err
		}
		// Reload parition tabe.
		ptSchemaID := diff.AffectedOpts[0].OldSchemaID
		ptID := diff.AffectedOpts[0].TableID
		ptDiff := &model.SchemaDiff{
			Type:        diff.Type,
			Version:     diff.Version,
			TableID:     ptID,
			SchemaID:    ptSchemaID,
			OldTableID:  ptID,
			OldSchemaID: ptSchemaID,
		}
		ptIDs, err := applyTableUpdate(b, m, ptDiff)
		if err != nil {
			return nil, errors.Trace(err)
		}
		return append(ptIDs, ntIDs...), nil
	}
	ntSchemaID := diff.OldSchemaID
	ntID := diff.OldTableID
	ptSchemaID := diff.SchemaID
	ptID := diff.TableID
	partID := diff.TableID
	if len(diff.AffectedOpts) > 0 {
		ptID = diff.AffectedOpts[0].TableID
		if diff.AffectedOpts[0].SchemaID != 0 {
			ptSchemaID = diff.AffectedOpts[0].SchemaID
		}
	}
	// The normal table needs to be updated first:
	// Just update the tables separately
	currDiff := &model.SchemaDiff{
		// This is only for the case since https://github.com/pingcap/tidb/pull/45877
		// Fixed now, by adding back the AffectedOpts
		// to carry the partitioned Table ID.
		Type:     diff.Type,
		Version:  diff.Version,
		TableID:  ntID,
		SchemaID: ntSchemaID,
	}
	if ptID != partID {
		currDiff.TableID = partID
		currDiff.OldTableID = ntID
		currDiff.OldSchemaID = ntSchemaID
	}
	ntIDs, err := applyTableUpdate(b, m, currDiff)
	if err != nil {
		return nil, errors.Trace(err)
	}
	// partID is the new id for the non-partitioned table!
	b.markTableBundleShouldUpdate(partID)
	// Then the partitioned table, will re-read the whole table, including all partitions!
	currDiff.TableID = ptID
	currDiff.SchemaID = ptSchemaID
	currDiff.OldTableID = ptID
	currDiff.OldSchemaID = ptSchemaID
	ptIDs, err := applyTableUpdate(b, m, currDiff)
	if err != nil {
		return nil, errors.Trace(err)
	}
	// ntID is the new id for the partition!
	b.markPartitionBundleShouldUpdate(ntID)
	err = updateAutoIDForExchangePartition(b.Requirement.Store(), ptSchemaID, ptID, ntSchemaID, ntID)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return append(ptIDs, ntIDs...), nil
}

func applyRecoverTable(b *Builder, m *meta.Meta, diff *model.SchemaDiff) ([]int64, error) {
	tblIDs, err := applyTableUpdate(b, m, diff)
	if err != nil {
		return nil, errors.Trace(err)
	}

	// bundle ops
	for _, opt := range diff.AffectedOpts {
		b.markTableBundleShouldUpdate(opt.TableID)
	}
	return tblIDs, nil
}

func updateAutoIDForExchangePartition(store kv.Storage, ptSchemaID, ptID, ntSchemaID, ntID int64) error {
	err := kv.RunInNewTxn(kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL), store, true, func(ctx context.Context, txn kv.Transaction) error {
		t := meta.NewMeta(txn)
		ptAutoIDs, err := t.GetAutoIDAccessors(ptSchemaID, ptID).Get()
		if err != nil {
			return err
		}

		// non-partition table auto IDs.
		ntAutoIDs, err := t.GetAutoIDAccessors(ntSchemaID, ntID).Get()
		if err != nil {
			return err
		}

		// Set both tables to the maximum auto IDs between normal table and partitioned table.
		newAutoIDs := meta.AutoIDGroup{
			RowID:       max(ptAutoIDs.RowID, ntAutoIDs.RowID),
			IncrementID: max(ptAutoIDs.IncrementID, ntAutoIDs.IncrementID),
			RandomID:    max(ptAutoIDs.RandomID, ntAutoIDs.RandomID),
		}
		err = t.GetAutoIDAccessors(ptSchemaID, ptID).Put(newAutoIDs)
		if err != nil {
			return err
		}
		err = t.GetAutoIDAccessors(ntSchemaID, ntID).Put(newAutoIDs)
		if err != nil {
			return err
		}
		return nil
	})

	return err
}

func (b *Builder) applyAffectedOpts(m *meta.Meta, tblIDs []int64, diff *model.SchemaDiff, tp model.ActionType) ([]int64, error) {
	if diff.AffectedOpts != nil {
		for _, opt := range diff.AffectedOpts {
			affectedDiff := &model.SchemaDiff{
				Version:     diff.Version,
				Type:        tp,
				SchemaID:    opt.SchemaID,
				TableID:     opt.TableID,
				OldSchemaID: opt.OldSchemaID,
				OldTableID:  opt.OldTableID,
			}
			affectedIDs, err := b.ApplyDiff(m, affectedDiff)
			if err != nil {
				return nil, errors.Trace(err)
			}
			tblIDs = append(tblIDs, affectedIDs...)
		}
	}
	return tblIDs, nil
}

func applyDefaultAction(b *Builder, m *meta.Meta, diff *model.SchemaDiff) ([]int64, error) {
	tblIDs, err := applyTableUpdate(b, m, diff)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return b.applyAffectedOpts(m, tblIDs, diff, diff.Type)
}

func (b *Builder) getTableIDs(diff *model.SchemaDiff) (oldTableID, newTableID int64) {
	switch diff.Type {
	case model.ActionCreateSequence, model.ActionRecoverTable:
		newTableID = diff.TableID
	case model.ActionCreateTable:
		// WARN: when support create table with foreign key in https://github.com/pingcap/tidb/pull/37148,
		// create table with foreign key requires a multi-step state change(none -> write-only -> public),
		// when the table's state changes from write-only to public, infoSchema need to drop the old table
		// which state is write-only, otherwise, infoSchema.sortedTablesBuckets will contain 2 table both
		// have the same ID, but one state is write-only, another table's state is public, it's unexpected.
		//
		// WARN: this change will break the compatibility if execute create table with foreign key DDL when upgrading TiDB,
		// since old-version TiDB doesn't know to delete the old table.
		// Since the cluster-index feature also has similar problem, we chose to prevent DDL execution during the upgrade process to avoid this issue.
		oldTableID = diff.OldTableID
		newTableID = diff.TableID
	case model.ActionDropTable, model.ActionDropView, model.ActionDropSequence:
		oldTableID = diff.TableID
	case model.ActionTruncateTable, model.ActionCreateView,
		model.ActionExchangeTablePartition, model.ActionAlterTablePartitioning,
		model.ActionRemovePartitioning:
		oldTableID = diff.OldTableID
		newTableID = diff.TableID
	default:
		oldTableID = diff.TableID
		newTableID = diff.TableID
	}
	return
}

func (b *Builder) updateBundleForTableUpdate(diff *model.SchemaDiff, newTableID, oldTableID int64) {
	// handle placement rule cache
	switch diff.Type {
	case model.ActionCreateTable:
		b.markTableBundleShouldUpdate(newTableID)
	case model.ActionDropTable:
		b.deleteBundle(b.infoSchema, oldTableID)
	case model.ActionTruncateTable:
		b.deleteBundle(b.infoSchema, oldTableID)
		b.markTableBundleShouldUpdate(newTableID)
	case model.ActionRecoverTable:
		b.markTableBundleShouldUpdate(newTableID)
	case model.ActionAlterTablePlacement:
		b.markTableBundleShouldUpdate(newTableID)
	}
}

func dropTableForUpdate(b *Builder, newTableID, oldTableID int64, dbInfo *model.DBInfo, diff *model.SchemaDiff) ([]int64, autoid.Allocators, error) {
	tblIDs := make([]int64, 0, 2)
	var keptAllocs autoid.Allocators
	// We try to reuse the old allocator, so the cached auto ID can be reused.
	if tableIDIsValid(oldTableID) {
		if oldTableID == newTableID &&
			// For rename table, keep the old alloc.

			// For repairing table in TiDB cluster, given 2 normal node and 1 repair node.
			// For normal node's information schema, repaired table is existed.
			// For repair node's information schema, repaired table is filtered (couldn't find it in `is`).
			// So here skip to reserve the allocators when repairing table.
			diff.Type != model.ActionRepairTable &&
			// Alter sequence will change the sequence info in the allocator, so the old allocator is not valid any more.
			diff.Type != model.ActionAlterSequence {
			// TODO: Check how this would work with ADD/REMOVE Partitioning,
			// which may have AutoID not connected to tableID
			// TODO: can there be _tidb_rowid AutoID per partition?
			oldAllocs, _ := allocByID(b, oldTableID)
			keptAllocs = getKeptAllocators(diff, oldAllocs)
		}

		tmpIDs := tblIDs
		if (diff.Type == model.ActionRenameTable || diff.Type == model.ActionRenameTables) && diff.OldSchemaID != diff.SchemaID {
			oldDBInfo, ok := oldSchemaInfo(b, diff)
			if !ok {
				return nil, keptAllocs, ErrDatabaseNotExists.GenWithStackByArgs(
					fmt.Sprintf("(Schema ID %d)", diff.OldSchemaID),
				)
			}
			tmpIDs = applyDropTable(b, diff, oldDBInfo, oldTableID, tmpIDs)
		} else {
			tmpIDs = applyDropTable(b, diff, dbInfo, oldTableID, tmpIDs)
		}

		if oldTableID != newTableID {
			// Update tblIDs only when oldTableID != newTableID because applyCreateTable() also updates tblIDs.
			tblIDs = tmpIDs
		}
	}
	return tblIDs, keptAllocs, nil
}

func (b *Builder) applyTableUpdate(m *meta.Meta, diff *model.SchemaDiff) ([]int64, error) {
	roDBInfo, ok := b.infoSchema.SchemaByID(diff.SchemaID)
	if !ok {
		return nil, ErrDatabaseNotExists.GenWithStackByArgs(
			fmt.Sprintf("(Schema ID %d)", diff.SchemaID),
		)
	}
	dbInfo := b.getSchemaAndCopyIfNecessary(roDBInfo.Name.L)
	oldTableID, newTableID := b.getTableIDs(diff)
	b.updateBundleForTableUpdate(diff, newTableID, oldTableID)
	b.copySortedTables(oldTableID, newTableID)

	tblIDs, allocs, err := dropTableForUpdate(b, newTableID, oldTableID, dbInfo, diff)
	if err != nil {
		return nil, err
	}

	if tableIDIsValid(newTableID) {
		// All types except DropTableOrView.
		var err error
		tblIDs, err = applyCreateTable(b, m, dbInfo, newTableID, allocs, diff.Type, tblIDs, diff.Version)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	return tblIDs, nil
}

// getKeptAllocators get allocators that is not changed by the DDL.
func getKeptAllocators(diff *model.SchemaDiff, oldAllocs autoid.Allocators) autoid.Allocators {
	var autoIDChanged, autoRandomChanged bool
	switch diff.Type {
	case model.ActionRebaseAutoID, model.ActionModifyTableAutoIdCache:
		autoIDChanged = true
	case model.ActionRebaseAutoRandomBase:
		autoRandomChanged = true
	case model.ActionMultiSchemaChange:
		for _, t := range diff.SubActionTypes {
			switch t {
			case model.ActionRebaseAutoID, model.ActionModifyTableAutoIdCache:
				autoIDChanged = true
			case model.ActionRebaseAutoRandomBase:
				autoRandomChanged = true
			}
		}
	}
	var newAllocs autoid.Allocators
	switch {
	case autoIDChanged:
		// Only drop auto-increment allocator.
		newAllocs = oldAllocs.Filter(func(a autoid.Allocator) bool {
			tp := a.GetType()
			return tp != autoid.RowIDAllocType && tp != autoid.AutoIncrementType
		})
	case autoRandomChanged:
		// Only drop auto-random allocator.
		newAllocs = oldAllocs.Filter(func(a autoid.Allocator) bool {
			tp := a.GetType()
			return tp != autoid.AutoRandomType
		})
	default:
		// Keep all allocators.
		newAllocs = oldAllocs
	}
	return newAllocs
}

func appendAffectedIDs(affected []int64, tblInfo *model.TableInfo) []int64 {
	affected = append(affected, tblInfo.ID)
	if pi := tblInfo.GetPartitionInfo(); pi != nil {
		for _, def := range pi.Definitions {
			affected = append(affected, def.ID)
		}
	}
	return affected
}

func (b *Builder) applyCreateSchema(m *meta.Meta, diff *model.SchemaDiff) error {
	di, err := m.GetDatabase(diff.SchemaID)
	if err != nil {
		return errors.Trace(err)
	}
	if di == nil {
		// When we apply an old schema diff, the database may has been dropped already, so we need to fall back to
		// full load.
		return ErrDatabaseNotExists.GenWithStackByArgs(
			fmt.Sprintf("(Schema ID %d)", diff.SchemaID),
		)
	}
	b.addDB(diff.Version, di, &schemaTables{dbInfo: di, tables: make(map[string]table.Table)})
	return nil
}

func (b *Builder) applyModifySchemaCharsetAndCollate(m *meta.Meta, diff *model.SchemaDiff) error {
	di, err := m.GetDatabase(diff.SchemaID)
	if err != nil {
		return errors.Trace(err)
	}
	if di == nil {
		// This should never happen.
		return ErrDatabaseNotExists.GenWithStackByArgs(
			fmt.Sprintf("(Schema ID %d)", diff.SchemaID),
		)
	}
	newDbInfo := b.getSchemaAndCopyIfNecessary(di.Name.L)
	newDbInfo.Charset = di.Charset
	newDbInfo.Collate = di.Collate
	return nil
}

func (b *Builder) applyModifySchemaDefaultPlacement(m *meta.Meta, diff *model.SchemaDiff) error {
	di, err := m.GetDatabase(diff.SchemaID)
	if err != nil {
		return errors.Trace(err)
	}
	if di == nil {
		// This should never happen.
		return ErrDatabaseNotExists.GenWithStackByArgs(
			fmt.Sprintf("(Schema ID %d)", diff.SchemaID),
		)
	}
	newDbInfo := b.getSchemaAndCopyIfNecessary(di.Name.L)
	newDbInfo.PlacementPolicyRef = di.PlacementPolicyRef
	return nil
}

func (b *Builder) applyDropSchema(diff *model.SchemaDiff) []int64 {
	di, ok := b.infoSchema.SchemaByID(diff.SchemaID)
	if !ok {
		return nil
	}
	b.infoSchema.delSchema(di)

	// Copy the sortedTables that contain the table we are going to drop.
	tableIDs := make([]int64, 0, len(di.Tables))
	bucketIdxMap := make(map[int]struct{}, len(di.Tables))
	for _, tbl := range di.Tables {
		bucketIdxMap[tableBucketIdx(tbl.ID)] = struct{}{}
		// TODO: If the table ID doesn't exist.
		tableIDs = appendAffectedIDs(tableIDs, tbl)
	}
	for bucketIdx := range bucketIdxMap {
		b.copySortedTablesBucket(bucketIdx)
	}

	di = di.Clone()
	for _, id := range tableIDs {
		b.deleteBundle(b.infoSchema, id)
		b.applyDropTable(diff, di, id, nil)
	}
	return tableIDs
}

func (b *Builder) applyRecoverSchema(m *meta.Meta, diff *model.SchemaDiff) ([]int64, error) {
	if di, ok := b.infoSchema.SchemaByID(diff.SchemaID); ok {
		return nil, ErrDatabaseExists.GenWithStackByArgs(
			fmt.Sprintf("(Schema ID %d)", di.ID),
		)
	}
	di, err := m.GetDatabase(diff.SchemaID)
	if err != nil {
		return nil, errors.Trace(err)
	}
	b.infoSchema.addSchema(&schemaTables{
		dbInfo: di,
		tables: make(map[string]table.Table, len(diff.AffectedOpts)),
	})
	return applyCreateTables(b, m, diff)
}

// copySortedTables copies sortedTables for old table and new table for later modification.
func (b *Builder) copySortedTables(oldTableID, newTableID int64) {
	if tableIDIsValid(oldTableID) {
		b.copySortedTablesBucket(tableBucketIdx(oldTableID))
	}
	if tableIDIsValid(newTableID) && newTableID != oldTableID {
		b.copySortedTablesBucket(tableBucketIdx(newTableID))
	}
}

func (b *Builder) copySortedTablesBucket(bucketIdx int) {
	oldSortedTables := b.infoSchema.sortedTablesBuckets[bucketIdx]
	newSortedTables := make(sortedTables, len(oldSortedTables))
	copy(newSortedTables, oldSortedTables)
	b.infoSchema.sortedTablesBuckets[bucketIdx] = newSortedTables
}

func (b *Builder) updateBundleForCreateTable(tblInfo *model.TableInfo, tp model.ActionType) {
	switch tp {
	case model.ActionDropTablePartition:
	case model.ActionTruncateTablePartition:
	// ReorganizePartition handle the bundles in applyReorganizePartition
	case model.ActionReorganizePartition, model.ActionRemovePartitioning,
		model.ActionAlterTablePartitioning:
	default:
		pi := tblInfo.GetPartitionInfo()
		if pi != nil {
			for _, partition := range pi.Definitions {
				b.markPartitionBundleShouldUpdate(partition.ID)
			}
		}
	}
}

func (b *Builder) buildAllocsForCreateTable(tp model.ActionType, dbInfo *model.DBInfo, tblInfo *model.TableInfo, allocs autoid.Allocators) autoid.Allocators {
	if len(allocs.Allocs) != 0 {
		tblVer := autoid.AllocOptionTableInfoVersion(tblInfo.Version)
		switch tp {
		case model.ActionRebaseAutoID, model.ActionModifyTableAutoIdCache:
			idCacheOpt := autoid.CustomAutoIncCacheOption(tblInfo.AutoIdCache)
			// If the allocator type might be AutoIncrementType, create both AutoIncrementType
			// and RowIDAllocType allocator for it. Because auto id and row id could share the same allocator.
			// Allocate auto id may route to allocate row id, if row id allocator is nil, the program panic!
			for _, tp := range [2]autoid.AllocatorType{autoid.AutoIncrementType, autoid.RowIDAllocType} {
				newAlloc := autoid.NewAllocator(b.Requirement, dbInfo.ID, tblInfo.ID, tblInfo.IsAutoIncColUnsigned(), tp, tblVer, idCacheOpt)
				allocs = allocs.Append(newAlloc)
			}
		case model.ActionRebaseAutoRandomBase:
			newAlloc := autoid.NewAllocator(b.Requirement, dbInfo.ID, tblInfo.ID, tblInfo.IsAutoRandomBitColUnsigned(), autoid.AutoRandomType, tblVer)
			allocs = allocs.Append(newAlloc)
		case model.ActionModifyColumn:
			// Change column attribute from auto_increment to auto_random.
			if tblInfo.ContainsAutoRandomBits() && allocs.Get(autoid.AutoRandomType) == nil {
				// Remove auto_increment allocator.
				allocs = allocs.Filter(func(a autoid.Allocator) bool {
					return a.GetType() != autoid.AutoIncrementType && a.GetType() != autoid.RowIDAllocType
				})
				newAlloc := autoid.NewAllocator(b.Requirement, dbInfo.ID, tblInfo.ID, tblInfo.IsAutoRandomBitColUnsigned(), autoid.AutoRandomType, tblVer)
				allocs = allocs.Append(newAlloc)
			}
		}
		return allocs
	}
	return autoid.NewAllocatorsFromTblInfo(b.Requirement, dbInfo.ID, tblInfo)
}

func applyCreateTable(b *Builder, m *meta.Meta, dbInfo *model.DBInfo, tableID int64, allocs autoid.Allocators, tp model.ActionType, affected []int64, schemaVersion int64) ([]int64, error) {
	tblInfo, err := m.GetTable(dbInfo.ID, tableID)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if tblInfo == nil {
		// When we apply an old schema diff, the table may has been dropped already, so we need to fall back to
		// full load.
		return nil, ErrTableNotExists.FastGenByArgs(
			fmt.Sprintf("(Schema ID %d)", dbInfo.ID),
			fmt.Sprintf("(Table ID %d)", tableID),
		)
	}

	b.updateBundleForCreateTable(tblInfo, tp)

	if tp != model.ActionTruncateTablePartition {
		affected = appendAffectedIDs(affected, tblInfo)
	}

	// Failpoint check whether tableInfo should be added to repairInfo.
	// Typically used in repair table test to load mock `bad` tableInfo into repairInfo.
	failpoint.Inject("repairFetchCreateTable", func(val failpoint.Value) {
		if val.(bool) {
			if domainutil.RepairInfo.InRepairMode() && tp != model.ActionRepairTable && domainutil.RepairInfo.CheckAndFetchRepairedTable(dbInfo, tblInfo) {
				failpoint.Return(nil, nil)
			}
		}
	})

	ConvertCharsetCollateToLowerCaseIfNeed(tblInfo)
	ConvertOldVersionUTF8ToUTF8MB4IfNeed(tblInfo)

	allocs = b.buildAllocsForCreateTable(tp, dbInfo, tblInfo, allocs)

	tbl, err := b.tableFromMeta(allocs, tblInfo)
	if err != nil {
		return nil, errors.Trace(err)
	}

	b.infoSchema.addReferredForeignKeys(dbInfo.Name, tblInfo)

	if !b.enableV2 {
		tableNames := b.infoSchema.schemaMap[dbInfo.Name.L]
		tableNames.tables[tblInfo.Name.L] = tbl
	}
	b.addTable(schemaVersion, dbInfo, tblInfo, tbl)

	bucketIdx := tableBucketIdx(tableID)
	slices.SortFunc(b.infoSchema.sortedTablesBuckets[bucketIdx], func(i, j table.Table) int {
		return cmp.Compare(i.Meta().ID, j.Meta().ID)
	})

	if tblInfo.TempTableType != model.TempTableNone {
		b.addTemporaryTable(tableID)
	}

	newTbl, ok := b.infoSchema.TableByID(tableID)
	if ok {
		dbInfo.Tables = append(dbInfo.Tables, newTbl.Meta())
	}
	return affected, nil
}

// ConvertCharsetCollateToLowerCaseIfNeed convert the charset / collation of table and its columns to lower case,
// if the table's version is prior to TableInfoVersion3.
func ConvertCharsetCollateToLowerCaseIfNeed(tbInfo *model.TableInfo) {
	if tbInfo.Version >= model.TableInfoVersion3 {
		return
	}
	tbInfo.Charset = strings.ToLower(tbInfo.Charset)
	tbInfo.Collate = strings.ToLower(tbInfo.Collate)
	for _, col := range tbInfo.Columns {
		col.SetCharset(strings.ToLower(col.GetCharset()))
		col.SetCollate(strings.ToLower(col.GetCollate()))
	}
}

// ConvertOldVersionUTF8ToUTF8MB4IfNeed convert old version UTF8 to UTF8MB4 if config.TreatOldVersionUTF8AsUTF8MB4 is enable.
func ConvertOldVersionUTF8ToUTF8MB4IfNeed(tbInfo *model.TableInfo) {
	if tbInfo.Version >= model.TableInfoVersion2 || !config.GetGlobalConfig().TreatOldVersionUTF8AsUTF8MB4 {
		return
	}
	if tbInfo.Charset == charset.CharsetUTF8 {
		tbInfo.Charset = charset.CharsetUTF8MB4
		tbInfo.Collate = charset.CollationUTF8MB4
	}
	for _, col := range tbInfo.Columns {
		if col.Version < model.ColumnInfoVersion2 && col.GetCharset() == charset.CharsetUTF8 {
			col.SetCharset(charset.CharsetUTF8MB4)
			col.SetCollate(charset.CollationUTF8MB4)
		}
	}
}

func (b *Builder) applyDropTable(diff *model.SchemaDiff, dbInfo *model.DBInfo, tableID int64, affected []int64) []int64 {
	bucketIdx := tableBucketIdx(tableID)
	sortedTbls := b.infoSchema.sortedTablesBuckets[bucketIdx]
	idx := sortedTbls.searchTable(tableID)
	if idx == -1 {
		return affected
	}
	if tableNames, ok := b.infoSchema.schemaMap[dbInfo.Name.L]; ok {
		tblInfo := sortedTbls[idx].Meta()
		delete(tableNames.tables, tblInfo.Name.L)
		affected = appendAffectedIDs(affected, tblInfo)
	}
	// Remove the table in sorted table slice.
	b.infoSchema.sortedTablesBuckets[bucketIdx] = append(sortedTbls[0:idx], sortedTbls[idx+1:]...)

	// Remove the table in temporaryTables
	if b.infoSchema.temporaryTableIDs != nil {
		delete(b.infoSchema.temporaryTableIDs, tableID)
	}
	// The old DBInfo still holds a reference to old table info, we need to remove it.
	b.deleteReferredForeignKeys(dbInfo, tableID)
	return affected
}

func (b *Builder) deleteReferredForeignKeys(dbInfo *model.DBInfo, tableID int64) {
	for i, tblInfo := range dbInfo.Tables {
		if tblInfo.ID == tableID {
			if i == len(dbInfo.Tables)-1 {
				dbInfo.Tables = dbInfo.Tables[:i]
			} else {
				dbInfo.Tables = append(dbInfo.Tables[:i], dbInfo.Tables[i+1:]...)
			}
			b.infoSchema.deleteReferredForeignKeys(dbInfo.Name, tblInfo)
			break
		}
	}
}

// Build builds and returns the built infoschema.
func (b *Builder) Build(schemaTS uint64) InfoSchema {
	if b.enableV2 {
		b.infoschemaV2.ts = schemaTS
		updateInfoSchemaBundles(b)
		return &b.infoschemaV2
	}
	updateInfoSchemaBundles(b)
	return b.infoSchema
}

// InitWithOldInfoSchema initializes an empty new InfoSchema by copies all the data from old InfoSchema.
func (b *Builder) InitWithOldInfoSchema(oldSchema InfoSchema) (*Builder, error) {
	// Do not mix infoschema v1 and infoschema v2 building, this can simplify the logic.
	// If we want to build infoschema v2, but the old infoschema is v1, just return error to trigger a full load.
	isV2, _ := IsV2(oldSchema)
	if b.enableV2 != isV2 {
		return nil, errors.Errorf("builder's (v2=%v) infoschema mismatch, return error to trigger full reload", b.enableV2)
	}

	if schemaV2, ok := oldSchema.(*infoschemaV2); ok {
		b.infoschemaV2.ts = schemaV2.ts
	}
	oldIS := oldSchema.base()
	b.initBundleInfoBuilder()
	b.infoSchema.schemaMetaVersion = oldIS.schemaMetaVersion
	b.copySchemasMap(oldIS)
	b.copyBundlesMap(oldIS)
	b.copyPoliciesMap(oldIS)
	b.copyResourceGroupMap(oldIS)
	b.copyTemporaryTableIDsMap(oldIS)
	b.copyReferredForeignKeyMap(oldIS)

	copy(b.infoSchema.sortedTablesBuckets, oldIS.sortedTablesBuckets)
	return b, nil
}

func (b *Builder) copySchemasMap(oldIS *infoSchema) {
	for _, v := range oldIS.schemaMap {
		b.infoSchema.addSchema(v)
	}
}

// getSchemaAndCopyIfNecessary creates a new schemaTables instance when a table in the database has changed.
// It also does modifications on the new one because old schemaTables must be read-only.
// And it will only copy the changed database once in the lifespan of the Builder.
// NOTE: please make sure the dbName is in lowercase.
func (b *Builder) getSchemaAndCopyIfNecessary(dbName string) *model.DBInfo {
	if !b.dirtyDB[dbName] {
		b.dirtyDB[dbName] = true
		oldSchemaTables := b.infoSchema.schemaMap[dbName]
		newSchemaTables := &schemaTables{
			dbInfo: oldSchemaTables.dbInfo.Copy(),
			tables: make(map[string]table.Table, len(oldSchemaTables.tables)),
		}
		for k, v := range oldSchemaTables.tables {
			newSchemaTables.tables[k] = v
		}
		b.infoSchema.addSchema(newSchemaTables)
		return newSchemaTables.dbInfo
	}
	return b.infoSchema.schemaMap[dbName].dbInfo
}

func (b *Builder) initVirtualTables(schemaVersion int64) error {
	// Initialize virtual tables.
	for _, driver := range drivers {
		err := b.createSchemaTablesForDB(driver.DBInfo, driver.TableFromMeta, schemaVersion)
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func (b *Builder) sortAllTablesByID() {
	// Sort all tables by `ID`
	for _, v := range b.infoSchema.sortedTablesBuckets {
		slices.SortFunc(v, func(a, b table.Table) int {
			return cmp.Compare(a.Meta().ID, b.Meta().ID)
		})
	}
}

// InitWithDBInfos initializes an empty new InfoSchema with a slice of DBInfo, all placement rules, and schema version.
func (b *Builder) InitWithDBInfos(dbInfos []*model.DBInfo, policies []*model.PolicyInfo, resourceGroups []*model.ResourceGroupInfo, schemaVersion int64) (*Builder, error) {
	info := b.infoSchema
	info.schemaMetaVersion = schemaVersion

	b.initBundleInfoBuilder()

	b.initMisc(dbInfos, policies, resourceGroups)

	for _, di := range dbInfos {
		err := b.createSchemaTablesForDB(di, b.tableFromMeta, schemaVersion)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}

	err := b.initVirtualTables(schemaVersion)
	if err != nil {
		return nil, err
	}

	b.sortAllTablesByID()

	return b, nil
}

func (b *Builder) tableFromMeta(alloc autoid.Allocators, tblInfo *model.TableInfo) (table.Table, error) {
	ret, err := tables.TableFromMeta(alloc, tblInfo)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if t, ok := ret.(table.CachedTable); ok {
		var tmp pools.Resource
		tmp, err = b.factory()
		if err != nil {
			return nil, errors.Trace(err)
		}

		err = t.Init(tmp.(sessionctx.Context).GetSQLExecutor())
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	return ret, nil
}

type tableFromMetaFunc func(alloc autoid.Allocators, tblInfo *model.TableInfo) (table.Table, error)

func (b *Builder) createSchemaTablesForDB(di *model.DBInfo, tableFromMeta tableFromMetaFunc, schemaVersion int64) error {
	schTbls := &schemaTables{
		dbInfo: di,
		tables: make(map[string]table.Table, len(di.Tables)),
	}
	for _, t := range di.Tables {
		allocs := autoid.NewAllocatorsFromTblInfo(b.Requirement, di.ID, t)
		var tbl table.Table
		tbl, err := tableFromMeta(allocs, t)
		if err != nil {
			return errors.Wrap(err, fmt.Sprintf("Build table `%s`.`%s` schema failed", di.Name.O, t.Name.O))
		}

		schTbls.tables[t.Name.L] = tbl
		b.addTable(schemaVersion, di, t, tbl)

		if tblInfo := tbl.Meta(); tblInfo.TempTableType != model.TempTableNone {
			b.addTemporaryTable(tblInfo.ID)
		}
	}
	b.addDB(schemaVersion, di, schTbls)

	return nil
}

func (b *Builder) addDB(schemaVersion int64, di *model.DBInfo, schTbls *schemaTables) {
	if b.enableV2 {
		if isSpecialDB(di.Name.L) {
			b.infoData.addSpecialDB(di, schTbls)
		} else {
			b.infoData.addDB(schemaVersion, di)
		}
	} else {
		b.infoSchema.addSchema(schTbls)
	}
}

func (b *Builder) addTable(schemaVersion int64, di *model.DBInfo, tblInfo *model.TableInfo, tbl table.Table) {
	if b.enableV2 {
		b.infoData.add(tableItem{
			dbName:        di.Name.L,
			dbID:          di.ID,
			tableName:     tblInfo.Name.L,
			tableID:       tblInfo.ID,
			schemaVersion: schemaVersion,
		}, tbl)
	} else {
		sortedTbls := b.infoSchema.sortedTablesBuckets[tableBucketIdx(tblInfo.ID)]
		b.infoSchema.sortedTablesBuckets[tableBucketIdx(tblInfo.ID)] = append(sortedTbls, tbl)
	}
}

type virtualTableDriver struct {
	*model.DBInfo
	TableFromMeta tableFromMetaFunc
}

var drivers []*virtualTableDriver

// RegisterVirtualTable register virtual tables to the builder.
func RegisterVirtualTable(dbInfo *model.DBInfo, tableFromMeta tableFromMetaFunc) {
	drivers = append(drivers, &virtualTableDriver{dbInfo, tableFromMeta})
}

// NewBuilder creates a new Builder with a Handle.
func NewBuilder(r autoid.Requirement, factory func() (pools.Resource, error), infoData *Data) *Builder {
	builder := &Builder{
		Requirement:  r,
		infoschemaV2: NewInfoSchemaV2(r, infoData),
		dirtyDB:      make(map[string]bool),
		factory:      factory,
		infoData:     infoData,
	}
	schemaCacheSize := variable.SchemaCacheSize.Load()
	if schemaCacheSize > 0 {
		infoData.tableCache.SetCapacity(uint64(schemaCacheSize))
		builder.enableV2 = true
	}
	return builder
}

func tableBucketIdx(tableID int64) int {
	intest.Assert(tableID > 0)
	return int(tableID % bucketCount)
}

func tableIDIsValid(tableID int64) bool {
	return tableID > 0
}
