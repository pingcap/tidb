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

package infoschema

import (
	"context"
	"fmt"
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/infoschema/internal"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/stretchr/testify/require"
)

type tableMetadataStore struct {
	kv.Storage
	batchSizes []int
	versions   []uint64
	failBatch  int
	batchErr   error
}

func (s *tableMetadataStore) GetSnapshot(ver kv.Version) kv.Snapshot {
	s.versions = append(s.versions, ver.Ver)
	return &tableMetadataSnapshot{Snapshot: s.Storage.GetSnapshot(ver), store: s}
}

type tableMetadataSnapshot struct {
	kv.Snapshot
	store *tableMetadataStore
}

func (s *tableMetadataSnapshot) BatchGet(ctx context.Context, keys []kv.Key, opts ...kv.BatchGetOption) (map[string]kv.ValueEntry, error) {
	s.store.batchSizes = append(s.store.batchSizes, len(keys))
	if len(s.store.batchSizes) == s.store.failBatch {
		return nil, errors.New("in flashback progress")
	}
	if s.store.batchErr != nil {
		return nil, s.store.batchErr
	}
	return s.Snapshot.BatchGet(ctx, keys, opts...)
}

func TestTableInfosByNames(t *testing.T) {
	r := internal.CreateAutoIDRequirement(t)
	t.Cleanup(func() { require.NoError(t, r.Store().Close()) })
	store := &tableMetadataStore{Storage: r.Store()}
	is := NewInfoSchemaV2(internal.CreateAutoIDRequirementWithStore(t, store), nil, NewData())
	is.schemaMetaVersion = 2
	db := &model.DBInfo{ID: 1, Name: ast.NewCIStr("batch_meta"), State: model.StatePublic}
	is.Data.addDB(1, db)
	names := make([]ast.CIStr, 130)
	infos := make([]*model.TableInfo, len(names))
	for i := range names {
		names[i] = ast.NewCIStr(fmt.Sprintf("T%d", i))
		col := &model.ColumnInfo{ID: 1, Name: ast.NewCIStr("a"), State: model.StatePublic, FieldType: *types.NewFieldType(mysql.TypeVarchar)}
		col.SetCharset("UTF8")
		col.SetCollate("UTF8_BIN")
		infos[i] = &model.TableInfo{
			ID: int64(1000 + i), Name: names[i], DBID: db.ID, State: model.StatePublic,
			Version: model.TableInfoVersion2, Charset: "UTF8", Collate: "UTF8_BIN", Columns: []*model.ColumnInfo{col},
		}
		item := tableItem{dbName: db.Name, dbID: db.ID, tableName: names[i], tableID: infos[i].ID, schemaVersion: 1}
		btreeSet(&is.byName, &item)
		btreeSet(&is.byID, &item)
	}
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL)
	require.NoError(t, kv.RunInNewTxn(ctx, store, false, func(_ context.Context, txn kv.Transaction) error {
		m := meta.NewMutator(txn)
		if err := m.CreateDatabase(db); err != nil {
			return err
		}
		for _, info := range infos {
			if err := m.CreateTableOrView(db.ID, info); err != nil {
				return err
			}
		}
		return nil
	}))
	ver, err := store.CurrentVersion(kv.GlobalTxnScope)
	require.NoError(t, err)
	is.ts = ver.Ver
	warm := infos[0].Clone()
	ConvertCharsetCollateToLowerCaseIfNeed(warm)
	is.tableCache.Set(tableCacheKey{warm.ID, 1}, internal.MockTable(t, store, warm))
	requested := append(append([]ast.CIStr(nil), names...), ast.NewCIStr("missing"))
	for range 2 {
		store.batchSizes = nil
		got, err := TableInfosByNames(ctx, &is, db.Name, requested)
		require.NoError(t, err)
		require.Len(t, got, len(names))
		require.Equal(t, []int{128, 1}, store.batchSizes)
		for _, info := range got {
			require.Equal(t, db.ID, info.DBID)
			require.Equal(t, "utf8", info.Charset)
			require.Equal(t, "utf8_bin", info.Collate)
			require.Equal(t, "utf8", info.Columns[0].GetCharset())
			if info.ID == warm.ID {
				require.Same(t, warm, info)
			} else {
				require.False(t, is.TableIsCached(info.ID))
			}
		}
	}
	store.batchSizes = nil
	got, err := TableInfosByNames(ctx, &is, db.Name, names[:1])
	require.NoError(t, err)
	require.Equal(t, []*model.TableInfo{warm}, got)
	require.Empty(t, store.batchSizes)

	canceled, cancel := context.WithCancel(ctx)
	cancel()
	_, err = TableInfosByNames(canceled, &is, db.Name, names)
	require.ErrorIs(t, err, context.Canceled)
	require.Empty(t, store.batchSizes)
	for _, input := range []struct {
		name         string
		schema       ast.CIStr
		names        []ast.CIStr
		wantCanceled bool
	}{
		{"empty_names", db.Name, nil, false},
		{"warm_cache", db.Name, names[:1], true},
		{"missing_table", db.Name, []ast.CIStr{ast.NewCIStr("missing")}, true},
		{"missing_schema", ast.NewCIStr("missing_schema"), names, false},
	} {
		t.Run(input.name, func(t *testing.T) {
			got, err := TableInfosByNames(canceled, &is, input.schema, input.names)
			require.Empty(t, store.batchSizes)
			if input.wantCanceled {
				require.ErrorIs(t, err, context.Canceled)
			} else {
				require.NoError(t, err)
				require.Empty(t, got)
			}
		})
	}
	store.batchErr = errors.New("batch read failed")
	_, err = TableInfosByNames(ctx, &is, db.Name, names[1:2])
	require.ErrorContains(t, err, "batch read failed")
	store.batchErr = nil

	// A failure after the first batch must discard that attempt's partial results.
	store.batchSizes, store.failBatch = nil, 2
	got, err = TableInfosByNames(ctx, &is, db.Name, names)
	require.NoError(t, err)
	require.Len(t, got, len(names))
	require.Equal(t, []int{128, 1, 128, 1}, store.batchSizes)
	store.failBatch = 0

	// The shared name index may contain newer entries than this infoschema snapshot.
	updated := infos[1].Clone()
	updated.Comment = "new definition"
	require.NoError(t, kv.RunInNewTxn(ctx, store, false, func(_ context.Context, txn kv.Transaction) error {
		return meta.NewMutator(txn).UpdateTable(db.ID, updated)
	}))
	newItem := &tableItem{dbName: db.Name, dbID: db.ID, tableName: updated.Name, tableID: updated.ID, schemaVersion: 3}
	btreeSet(&is.byName, newItem)
	btreeSet(&is.byID, newItem)
	got, err = TableInfosByNames(ctx, &is, db.Name, names[1:2])
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Empty(t, got[0].Comment)
	for _, version := range store.versions {
		require.Equal(t, is.ts, version)
	}
	latest := NewInfoSchemaV2(is.r, is.factory, is.Data)
	latest.schemaMetaVersion = 3
	ver, err = store.CurrentVersion(kv.GlobalTxnScope)
	require.NoError(t, err)
	latest.ts = ver.Ver
	got, err = TableInfosByNames(ctx, &latest, db.Name, names[1:2])
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Equal(t, updated.Comment, got[0].Comment)
	newItem = &tableItem{dbName: db.Name, dbID: db.ID, tableName: updated.Name, tableID: updated.ID, schemaVersion: 4, tomb: true}
	btreeSet(&is.byName, newItem)
	latest.schemaMetaVersion = 4
	store.batchSizes = nil
	got, err = TableInfosByNames(ctx, &latest, db.Name, names[1:2])
	require.NoError(t, err)
	require.Empty(t, got)
	require.Empty(t, store.batchSizes)
}

func TestTableInfosByNamesMetadata(t *testing.T) {
	t.Cleanup(config.RestoreFunc())
	config.UpdateGlobal(func(conf *config.Config) { conf.TreatOldVersionUTF8AsUTF8MB4 = true })
	r := internal.CreateAutoIDRequirement(t)
	t.Cleanup(func() { require.NoError(t, r.Store().Close()) })
	store := &tableMetadataStore{Storage: r.Store()}
	is := NewInfoSchemaV2(internal.CreateAutoIDRequirementWithStore(t, store), nil, NewData())
	is.schemaMetaVersion = 1
	db := &model.DBInfo{ID: 1, Name: ast.NewCIStr("batch_meta"), State: model.StatePublic}
	is.Data.addDB(1, db)
	var infos []*model.TableInfo
	var names []ast.CIStr
	for _, kind := range []string{"legacy_utf8", "generated", "partitioned", "check_constraint"} {
		for _, cache := range []string{"warm", "cold"} {
			col := &model.ColumnInfo{
				ID: 1, Name: ast.NewCIStr("a"), State: model.StatePublic,
				FieldType: *types.NewFieldType(mysql.TypeVarchar),
			}
			col.SetCharset("UTF8")
			col.SetCollate("UTF8_BIN")
			col.SetFlen(32)
			info := &model.TableInfo{
				ID: int64(1000 + len(infos)), Name: ast.NewCIStr(kind + "_" + cache), DBID: db.ID,
				State: model.StatePublic, Version: model.TableInfoVersion2,
				Charset: "UTF8", Collate: "UTF8_BIN", Columns: []*model.ColumnInfo{col},
			}
			switch kind {
			case "legacy_utf8":
				info.Version = model.TableInfoVersion1
				col.Version = model.ColumnInfoVersion1
			case "generated":
				generated := col.Clone()
				generated.ID, generated.Name, generated.Offset = 2, ast.NewCIStr("b"), 1
				generated.GeneratedExprString = "concat(`a`, 'suffix')"
				generated.Dependences = map[string]struct{}{"a": {}}
				info.Columns = append(info.Columns, generated)
			case "partitioned":
				info.Partition = &model.PartitionInfo{
					Type: ast.PartitionTypeKey, Columns: []ast.CIStr{col.Name}, Enable: true, Num: 2,
					Definitions: []model.PartitionDefinition{
						{ID: info.ID + 1000, Name: ast.NewCIStr("p0")},
						{ID: info.ID + 2000, Name: ast.NewCIStr("p1")},
					},
				}
			case "check_constraint":
				info.Constraints = []*model.ConstraintInfo{{
					ID: 1, Name: ast.NewCIStr("chk_a"), Table: info.Name,
					ConstraintCols: []ast.CIStr{col.Name}, Enforced: true,
					ExprString: "length(`a`) > 0", State: model.StatePublic,
				}}
			}
			infos = append(infos, info)
			names = append(names, info.Name)
			item := &tableItem{dbName: db.Name, dbID: db.ID, tableName: info.Name, tableID: info.ID, schemaVersion: 1}
			btreeSet(&is.byName, item)
			btreeSet(&is.byID, item)
		}
	}
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL)
	require.NoError(t, kv.RunInNewTxn(ctx, store, false, func(_ context.Context, txn kv.Transaction) error {
		m := meta.NewMutator(txn)
		if err := m.CreateDatabase(db); err != nil {
			return err
		}
		for _, info := range infos {
			if err := m.CreateTableOrView(db.ID, info); err != nil {
				return err
			}
		}
		return nil
	}))
	ver, err := store.CurrentVersion(kv.GlobalTxnScope)
	require.NoError(t, err)
	is.ts = ver.Ver
	expected := make(map[int64]*model.TableInfo, len(infos))
	for i, info := range infos {
		tbl, err := is.TableByName(WithRefillOption(ctx, false), db.Name, info.Name)
		require.NoError(t, err)
		expected[info.ID] = tbl.Meta()
		if i%2 == 0 {
			is.tableCache.Set(tableCacheKey{info.ID, 1}, tbl)
		}
	}
	for range 2 {
		store.batchSizes = nil
		got, err := TableInfosByNames(ctx, &is, db.Name, names)
		require.NoError(t, err)
		require.Len(t, got, len(infos))
		require.Equal(t, []int{4}, store.batchSizes)
		actual := make(map[int64]*model.TableInfo, len(got))
		for _, info := range got {
			actual[info.ID] = info
			if info.Version < model.TableInfoVersion2 {
				require.Equal(t, "utf8mb4", info.Charset)
				require.Equal(t, "utf8mb4_bin", info.Collate)
				require.Equal(t, "utf8mb4", info.Columns[0].GetCharset())
				require.Equal(t, "utf8mb4_bin", info.Columns[0].GetCollate())
			}
		}
		require.Equal(t, expected, actual)
		for i, info := range infos {
			require.Equal(t, i%2 == 0, is.TableIsCached(info.ID))
		}
	}
}

func TestTableInfosByNamesSessionOverrides(t *testing.T) {
	ctx := context.Background()
	schema := ast.NewCIStr("test")
	global := &model.TableInfo{ID: 1, Name: ast.NewCIStr("t"), State: model.StatePublic}
	base := MockInfoSchema([]*model.TableInfo{global})
	db, ok := base.SchemaByName(schema)
	require.True(t, ok)
	mdl := global.Clone()
	mdl.Comment = "mdl"
	local := global.Clone()
	local.ID = 2
	local.TempTableType = model.TempTableLocal
	extended := &SessionExtendedInfoSchema{InfoSchema: base, LocalTemporaryTables: NewSessionTables(), MdlTables: NewSessionTables()}
	require.NoError(t, extended.LocalTemporaryTables.AddTable(db, table.MockTableFromMeta(local)))
	require.NoError(t, extended.MdlTables.AddTable(db, table.MockTableFromMeta(mdl)))
	names := []ast.CIStr{ast.NewCIStr("T"), ast.NewCIStr("missing")}
	got, err := TableInfosByNames(ctx, extended, schema, names)
	require.NoError(t, err)
	require.Equal(t, []*model.TableInfo{local}, got)
	extended.LocalTemporaryTables = nil
	got, err = TableInfosByNames(ctx, extended, schema, names)
	require.NoError(t, err)
	require.Equal(t, []*model.TableInfo{mdl}, got)
	extended.MdlTables = nil
	got, err = TableInfosByNames(ctx, extended, schema, names)
	require.NoError(t, err)
	require.Equal(t, []*model.TableInfo{global}, got)

	// System schemas are memory-backed even when the underlying infoschema is v2.
	v2 := NewInfoSchemaV2(nil, nil, NewData())
	systemTable := &model.TableInfo{ID: 3, Name: ast.NewCIStr("columns")}
	v2.specials.Store("information_schema", &schemaTables{tables: map[string]table.Table{"columns": table.MockTableFromMeta(systemTable)}})
	got, err = TableInfosByNames(ctx, &v2, ast.NewCIStr("information_schema"), []ast.CIStr{ast.NewCIStr("COLUMNS"), ast.NewCIStr("missing")})
	require.NoError(t, err)
	require.Equal(t, []*model.TableInfo{systemTable}, got)
}
