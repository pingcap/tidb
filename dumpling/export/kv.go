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

package export

import (
	"database/sql"
	"fmt"
	"slices"
	"strings"
	"sync/atomic"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/version"
	tcontext "github.com/pingcap/tidb/dumpling/context"
	"github.com/pingcap/tidb/dumpling/dumpservice"
	"github.com/pingcap/tidb/dumpling/internal/kvsource"
	tidbmeta "github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/mysql"
)

type kvTableMeta struct {
	database string
	table    string
	columns  []*model.ColumnInfo
	types    []string
	names    []string
	selected string
	create   string
}

func newKVTableMeta(database string, table *model.TableInfo, columns []*model.ColumnInfo) *kvTableMeta {
	types := make([]string, 0, len(columns))
	names := make([]string, 0, len(columns))
	for _, column := range columns {
		types = append(types, kvsource.ColumnType(column))
		names = append(names, column.Name.O)
	}
	return &kvTableMeta{
		database: database,
		table:    table.Name.O,
		columns:  columns,
		types:    types,
		names:    names,
		selected: strings.Join(columnNamesToSelectFields(names), ","),
	}
}

func (*kvTableMeta) SpecialComments() StringIter {
	return newStringIter(getSpecialComments(version.ServerTypeDumpService)...)
}

func (m *kvTableMeta) DatabaseName() string    { return m.database }
func (m *kvTableMeta) TableName() string       { return m.table }
func (m *kvTableMeta) ColumnCount() uint       { return uint(len(m.columns)) }
func (m *kvTableMeta) ColumnTypes() []string   { return m.types }
func (m *kvTableMeta) ColumnNames() []string   { return m.names }
func (m *kvTableMeta) SelectedField() string   { return m.selected }
func (m *kvTableMeta) SelectedLen() int        { return len(m.columns) }
func (m *kvTableMeta) ShowCreateTable() string { return m.create }
func (*kvTableMeta) ShowCreateView() string    { return "" }
func (*kvTableMeta) AvgRowLength() uint64      { return 0 }
func (*kvTableMeta) HasImplicitRowID() bool    { return false }

func (m *kvTableMeta) ColumnInfos() []*ColumnInfo {
	infos := make([]*ColumnInfo, len(m.columns))
	for i, column := range m.columns {
		info := &ColumnInfo{
			Name:             m.names[i],
			DatabaseTypeName: m.types[i],
			Nullable:         !mysql.HasNotNullFlag(column.GetFlag()),
		}
		if m.types[i] == "DECIMAL" {
			info.Precision = int64(column.GetFlen())
			info.Scale = int64(column.GetDecimal())
		}
		infos[i] = info
	}
	return infos
}

type kvTableData struct {
	scanner kvsource.Scanner
	table   *model.TableInfo
	columns []*model.ColumnInfo
	ranges  []dumpservice.Range
	iter    *kvRowIter
}

func newKVTableData(
	scanner kvsource.Scanner,
	table *model.TableInfo,
	columns []*model.ColumnInfo,
	ranges []dumpservice.Range,
) *kvTableData {
	return &kvTableData{
		scanner: scanner,
		table:   table,
		columns: columns,
		ranges:  ranges,
	}
}

func (d *kvTableData) Start(tctx *tcontext.Context, _ *sql.Conn) error {
	d.iter = &kvRowIter{
		Rows: kvsource.NewRows(tctx, d.scanner, d.table, d.columns, d.ranges),
		args: make([]any, len(d.columns)),
	}
	return d.iter.Error()
}

func (d *kvTableData) Rows() SQLRowIter { return d.iter }

func (d *kvTableData) Close() error {
	if d.iter != nil {
		return d.iter.Close()
	}
	return nil
}

func (*kvTableData) RawRows() *sql.Rows { return nil }

type kvRowIter struct {
	*kvsource.Rows
	args []any
}

func (i *kvRowIter) Decode(receiver RowReceiver) error {
	if !i.HasNext() {
		return errors.New("KV row iterator has no current row")
	}
	receiver.BindAddress(i.args)
	return i.Rows.Decode(i.args)
}

type kvDatabaseTables struct {
	database *model.DBInfo
	tables   []*model.TableInfo
}

func (d *Dumper) loadKVTables(reader tidbmeta.Reader) ([]kvDatabaseTables, error) {
	databases, err := reader.ListDatabases()
	if err != nil {
		return nil, err
	}
	selected := make([]kvDatabaseTables, 0, len(databases))
	for _, database := range databases {
		if database.State != model.StatePublic || !d.conf.TableFilter.MatchSchema(database.Name.O) {
			continue
		}
		tables, err := reader.ListTables(d.tctx, database.ID)
		if err != nil {
			return nil, err
		}
		tables = slices.DeleteFunc(tables, func(table *model.TableInfo) bool {
			return table.State != model.StatePublic || table.IsView() || table.IsSequence() ||
				!d.conf.TableFilter.MatchTable(database.Name.O, table.Name.O)
		})
		if len(tables) == 0 && !d.conf.DumpEmptyDatabase {
			continue
		}
		slices.SortFunc(tables, func(left, right *model.TableInfo) int {
			return strings.Compare(left.Name.L, right.Name.L)
		})
		selected = append(selected, kvDatabaseTables{database: database, tables: tables})
	}
	slices.SortFunc(selected, func(left, right kvDatabaseTables) int {
		return model.LessDBInfo(left.database, right.database)
	})
	return selected, nil
}

func (d *Dumper) prepareKVTableMeta(reader tidbmeta.Reader, databases []kvDatabaseTables) (map[tableName]*kvTableMeta, error) {
	metas := make(map[tableName]*kvTableMeta)
	filteredTables := make(map[tableName]bool)
	for _, entry := range databases {
		for _, table := range entry.tables {
			columns := kvsource.VisibleColumns(table, d.conf.includeStoredGeneratedColumns())
			names := make([]string, len(columns))
			for i, column := range columns {
				names[i] = column.Name.O
			}
			_, indexes, err := d.conf.columnFilter.applyToColumns(entry.database.Name.O, table.Name.O, names)
			if err != nil {
				return nil, err
			}
			key := tableName{db: entry.database.Name.O, table: table.Name.O}
			if len(indexes) != len(columns) {
				selected := make([]*model.ColumnInfo, len(indexes))
				for i, index := range indexes {
					selected[i] = columns[index]
				}
				columns = selected
				filteredTables[key] = true
			}
			meta := newKVTableMeta(key.db, table, columns)
			if !d.conf.NoSchemas {
				meta.create, err = kvsource.CreateTableSQL(reader, entry.database, table)
				if err != nil {
					return nil, err
				}
			}
			metas[key] = meta
		}
	}
	if d.conf.NoSchemas || len(filteredTables) == 0 {
		return metas, nil
	}

	schemaParser := parser.New()
	schemas := make(projectedTableSchemas, len(metas))
	for _, entry := range databases {
		for _, table := range entry.tables {
			key := tableName{db: entry.database.Name.O, table: table.Name.O}
			meta := metas[key]
			var err error
			if filteredTables[key] {
				schemas[key], err = buildProjectedTableSchema(schemaParser, meta.create, meta.names)
			} else {
				schemas[key], err = parseTableSchema(schemaParser, meta.create)
			}
			if err != nil {
				return nil, errors.Annotatef(err, "failed to analyze schema projection for table `%s`.`%s`", escapeString(key.db), escapeString(key.table))
			}
			if filteredTables[key] {
				meta.create, err = restoreProjectedSchema(schemas[key].createTable)
				if err != nil {
					return nil, errors.Annotatef(err, "failed to restore schema projection for table `%s`.`%s`", escapeString(key.db), escapeString(key.table))
				}
			}
		}
	}
	// Include every parent schema before checking references from child tables.
	for _, entry := range databases {
		for _, table := range entry.tables {
			key := tableName{db: entry.database.Name.O, table: table.Name.O}
			if err := validateForeignKeyParents(key.db, schemas[key], schemas); err != nil {
				return nil, errors.Annotatef(err, "failed to validate schema projection for table `%s`.`%s`", escapeString(key.db), escapeString(key.table))
			}
		}
	}
	return metas, nil
}

func (d *Dumper) prepareKVDump() (_ *dumpSource, prepareErr error) {
	client, err := dumpservice.NewClient(d.conf.DumpService)
	if err != nil {
		return nil, err
	}
	defer func() {
		if prepareErr != nil {
			client.Close()
		}
	}()
	snapshot, err := kvsource.NewMetadataSnapshot(d.tctx, client)
	if err != nil {
		return nil, err
	}
	reader := tidbmeta.NewReader(snapshot)
	databases, err := d.loadKVTables(reader)
	if err != nil {
		return nil, err
	}
	metas, err := d.prepareKVTableMeta(reader, databases)
	if err != nil {
		return nil, err
	}
	tableCount := 0
	for _, database := range databases {
		tableCount += len(database.tables)
	}
	atomic.StoreInt64(&d.totalTables, int64(tableCount))
	policies, err := reader.ListPolicies()
	if err != nil {
		return nil, err
	}
	return &dumpSource{
		newWriter: func(tctx *tcontext.Context, id int64) (*Writer, error) {
			writer := NewWriter(tctx, id, d.conf, nil, d.extStore, d.metrics)
			writer.rebuildConnFn = func(conn *sql.Conn, _ bool) (*sql.Conn, error) { return conn, nil }
			return writer, nil
		},
		produceTasks: func(tctx *tcontext.Context, taskChan chan<- Task) error {
			return d.dumpKVTasks(tctx, client, metas, databases, policies, taskChan)
		},
		close: client.Close,
	}, nil
}

func (d *Dumper) dumpKVTasks(
	tctx *tcontext.Context,
	scanner kvsource.Scanner,
	metas map[tableName]*kvTableMeta,
	databases []kvDatabaseTables,
	policies []*model.PolicyInfo,
	taskChan chan<- Task,
) error {
	send := func(task Task) error {
		if d.sendTaskToChan(tctx, task, taskChan) {
			return tctx.Err()
		}
		return nil
	}
	for _, policy := range policies {
		createSQL := kvsource.CreatePlacementPolicySQL(policy)
		if err := send(NewTaskPolicyMeta(policy.Name.O, fmt.Sprintf("/*T![placement] %s */", createSQL))); err != nil {
			return err
		}
	}
	for _, entry := range databases {
		database := entry.database
		if !d.conf.NoSchemas {
			createSQL, err := kvsource.CreateDatabaseSQL(database)
			if err != nil {
				return err
			}
			if err := send(NewTaskDatabaseMeta(database.Name.O, createSQL)); err != nil {
				return err
			}
		}
		for _, table := range entry.tables {
			meta := metas[tableName{db: database.Name.O, table: table.Name.O}]
			if !d.conf.NoSchemas {
				if err := send(NewTaskTableMeta(database.Name.O, table.Name.O, meta.ShowCreateTable())); err != nil {
					return err
				}
			}
			if d.conf.NoData {
				continue
			}
			ranges, err := kvsource.ShardTableRanges(tctx, scanner, table)
			if err != nil {
				return err
			}
			for chunkIndex, rangeToScan := range ranges {
				data := newKVTableData(scanner, table, meta.columns, []dumpservice.Range{rangeToScan})
				if err := send(d.newTaskTableData(meta, data, chunkIndex, len(ranges))); err != nil {
					return err
				}
			}
		}
	}
	return tctx.Err()
}

func (m *kvTableMeta) String() string {
	return fmt.Sprintf("%s.%s", m.database, m.table)
}
