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
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"slices"
	"strings"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/version"
	tcontext "github.com/pingcap/tidb/dumpling/context"
	"github.com/pingcap/tidb/dumpling/dumpservice"
	"github.com/pingcap/tidb/pkg/executor"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/format/textrow"
	tidbmeta "github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/pingcap/tidb/pkg/util/rowcodec"
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
		types = append(types, kvColumnType(column))
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
	scanner kvScanner
	table   *model.TableInfo
	columns []*model.ColumnInfo
	ranges  []dumpservice.Range
	metrics *metrics
	iter    *kvRowIter
}

type kvScanner interface {
	Shards(ctx context.Context, startKey, endKey []byte) ([]dumpservice.Range, error)
	Scan(ctx context.Context, startKey, endKey []byte) (*dumpservice.Scan, error)
}

func newKVTableData(
	scanner kvScanner,
	table *model.TableInfo,
	columns []*model.ColumnInfo,
	ranges []dumpservice.Range,
	metrics *metrics,
) *kvTableData {
	return &kvTableData{
		scanner: scanner,
		table:   table,
		columns: columns,
		ranges:  ranges,
		metrics: metrics,
	}
}

func (d *kvTableData) Start(tctx *tcontext.Context, _ *sql.Conn) error {
	decoder := newKVRowDecoder(d.table, d.columns)
	iter := &kvRowIter{
		ctx:     tctx,
		scanner: d.scanner,
		ranges:  d.ranges,
		metrics: d.metrics,
		table:   d.table,
		decoder: decoder,
		encoder: textrow.NewResultEncoder(charset.CharsetUTF8MB4),
		args:    make([]any, len(decoder.columns)),
	}
	iter.readNext()
	d.iter = iter
	return iter.err
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
	ctx       context.Context
	scanner   kvScanner
	ranges    []dumpservice.Range
	nextRange int
	scan      *dumpservice.Scan
	metrics   *metrics
	table     *model.TableInfo
	decoder   *kvRowDecoder
	encoder   *textrow.ResultEncoder
	key       []byte
	value     []byte
	args      []any
	err       error
	hasRow    bool
}

func (i *kvRowIter) HasNext() bool { return i.err == nil && i.hasRow }

func (i *kvRowIter) Decode(receiver RowReceiver) error {
	if !i.HasNext() {
		return errors.New("KV row iterator has no current row")
	}
	values, err := i.decoder.decode(i.table, i.key, i.value)
	if err != nil {
		return err
	}
	if values.Len() != len(i.args) {
		return errors.Errorf("KV row has %d values for %d columns", values.Len(), len(i.args))
	}
	receiver.BindAddress(i.args)
	for index, column := range i.decoder.columns {
		destination, ok := i.args[index].(*sql.RawBytes)
		if !ok {
			return errors.Errorf("unsupported Dumpling kv row receiver %T", i.args[index])
		}
		if values.IsNull(index) {
			*destination = nil
			continue
		}
		// Format values with the same text serializer as the server so the KV
		// dump matches the SQL dump, for example MySQL's float formatting.
		columnInfo := textrow.ColumnInfo{
			Table:   i.table.Name.O,
			Charset: uint16(mysql.CharsetNameToID(column.GetCharset())),
			Flag:    uint16(column.GetFlag()),
			Decimal: uint8(column.GetDecimal()),
			Type:    column.GetType(),
		}
		data, err := textrow.FormatValueText(values, index, columnInfo, i.encoder)
		if err != nil {
			return errors.Annotatef(err, "format KV column %d", index)
		}
		*destination = append((*destination)[:0], data...)
		if len(data) == 0 {
			*destination = sql.RawBytes{}
		}
	}
	return nil
}

func (i *kvRowIter) Next() {
	if i.HasNext() {
		i.readNext()
	}
}

func (i *kvRowIter) Error() error { return i.err }

func (i *kvRowIter) Close() error {
	if i.scan == nil {
		return nil
	}
	scan := i.scan
	i.scan = nil
	return scan.Close()
}

func (i *kvRowIter) readNext() {
	for {
		if i.scan == nil {
			if i.nextRange == len(i.ranges) {
				i.hasRow = false
				return
			}
			rangeToScan := i.ranges[i.nextRange]
			i.nextRange++
			scan, err := i.scanner.Scan(
				i.ctx,
				rangeToScan.Start,
				rangeToScan.End,
			)
			if err != nil {
				i.err = err
				i.hasRow = false
				return
			}
			i.scan = scan
		}

		key, value, end, err := i.scan.ReadRow(i.key, i.value)
		if err != nil {
			i.err = err
			i.hasRow = false
			i.scan = nil
			return
		}
		if end {
			i.scan = nil
			continue
		}
		i.key = key
		i.value = value
		i.hasRow = true
		return
	}
}

type kvRowDecoder struct {
	columns []*model.ColumnInfo
	ctx     *mock.Context
	schema  *expression.Schema
	decoder *rowcodec.ChunkDecoder
	chunk   *chunk.Chunk
}

func newKVRowDecoder(table *model.TableInfo, columns []*model.ColumnInfo) *kvRowDecoder {
	schema := expression.NewSchema()
	fieldTypes := make([]*types.FieldType, 0, len(columns))
	for _, column := range columns {
		schema.Append(&expression.Column{ID: column.ID, RetType: &column.FieldType})
		fieldTypes = append(fieldTypes, &column.FieldType)
	}
	ctx := mock.NewContextDeprecated()
	ctx.ResetSessionAndStmtTimeZone(time.UTC)
	return &kvRowDecoder{
		columns: columns,
		ctx:     ctx,
		schema:  schema,
		decoder: executor.NewRowDecoder(ctx, schema, table),
		chunk:   chunk.New(fieldTypes, 1, 1),
	}
}

func (d *kvRowDecoder) decode(table *model.TableInfo, key, value []byte) (chunk.Row, error) {
	handle, err := tablecodec.DecodeRowKey(key)
	if err != nil {
		return chunk.Row{}, errors.Annotatef(err, "decode KV row key %x", key)
	}
	d.chunk.Reset()
	if err := executor.DecodeRowValToChunk(d.ctx, d.schema, table, handle, value, d.chunk, d.decoder); err != nil {
		return chunk.Row{}, errors.Annotatef(err, "decode KV row value at key %x", key)
	}
	return d.chunk.GetRow(0), nil
}

func kvVisibleColumns(table *model.TableInfo, includeStoredGenerated bool) []*model.ColumnInfo {
	columns := make([]*model.ColumnInfo, 0, len(table.Columns))
	for _, column := range table.Cols() {
		if column == nil || column.Hidden || column.IsVirtualGenerated() {
			continue
		}
		if column.IsGenerated() && !includeStoredGenerated {
			continue
		}
		columns = append(columns, column)
	}
	return columns
}

func kvPhysicalTableIDs(table *model.TableInfo) []int64 {
	partition := table.GetPartitionInfo()
	if partition == nil || len(partition.Definitions) == 0 {
		return []int64{table.ID}
	}
	tableIDs := make([]int64, 0, len(partition.Definitions))
	for _, definition := range partition.Definitions {
		tableIDs = append(tableIDs, definition.ID)
	}
	return tableIDs
}

func kvPhysicalTableRanges(table *model.TableInfo) []dumpservice.Range {
	tableIDs := kvPhysicalTableIDs(table)
	ranges := make([]dumpservice.Range, 0, len(tableIDs))
	for _, tableID := range tableIDs {
		start := tablecodec.GenTableRecordPrefix(tableID)
		ranges = append(ranges, dumpservice.Range{Start: start, End: start.PrefixNext()})
	}
	return ranges
}

func shardKVTableRanges(
	ctx context.Context,
	scanner kvScanner,
	table *model.TableInfo,
) ([]dumpservice.Range, error) {
	physicalRanges := kvPhysicalTableRanges(table)
	shards := make([]dumpservice.Range, 0, len(physicalRanges))
	for _, physicalRange := range physicalRanges {
		ranges, err := scanner.Shards(ctx, physicalRange.Start, physicalRange.End)
		if err != nil {
			return nil, err
		}
		shards = append(shards, ranges...)
	}
	return shards, nil
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

func kvCreateDatabaseSQL(database *model.DBInfo) (string, error) {
	var output bytes.Buffer
	if err := executor.ConstructResultOfShowCreateDatabase(newKVContext(), database, true, &output); err != nil {
		return "", errors.Annotatef(err, "build schema for kv database %q", database.Name.O)
	}
	return output.String(), nil
}

func kvCreateTableSQL(reader tidbmeta.Reader, database *model.DBInfo, table *model.TableInfo) (string, error) {
	allocators, err := kvMetadataAllocators(reader, database.ID, table)
	if err != nil {
		return "", errors.Annotatef(err, "read auto ID bases for kv table %q", table.Name.O)
	}
	var output bytes.Buffer
	if err := executor.ConstructResultOfShowCreateTable(newKVContext(), database.Name, table, allocators, &output); err != nil {
		return "", errors.Annotatef(err, "build schema for kv table %q", table.Name.O)
	}
	return output.String(), nil
}

func (d *Dumper) prepareKVTableMeta(reader tidbmeta.Reader, databases []kvDatabaseTables) (map[tableName]*kvTableMeta, error) {
	metas := make(map[tableName]*kvTableMeta)
	filteredTables := make(map[tableName]bool)
	for _, entry := range databases {
		for _, table := range entry.tables {
			columns := kvVisibleColumns(table, d.conf.includeStoredGeneratedColumns())
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
				meta.create, err = kvCreateTableSQL(reader, entry.database, table)
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
	snapshot, err := newKVMetadataSnapshot(d.tctx, client)
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
	scanner kvScanner,
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
		createSQL := executor.ConstructResultOfShowCreatePlacementPolicy(policy)
		if err := send(NewTaskPolicyMeta(policy.Name.O, fmt.Sprintf("/*T![placement] %s */", createSQL))); err != nil {
			return err
		}
	}
	for _, entry := range databases {
		database := entry.database
		if !d.conf.NoSchemas {
			createSQL, err := kvCreateDatabaseSQL(database)
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
			ranges, err := shardKVTableRanges(tctx, scanner, table)
			if err != nil {
				return err
			}
			for chunkIndex, rangeToScan := range ranges {
				data := newKVTableData(scanner, table, meta.columns, []dumpservice.Range{rangeToScan}, d.metrics)
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

func scanKVRange(
	ctx context.Context,
	scanner kvScanner,
	startKey, endKey []byte,
	emit func(key, value []byte) error,
) error {
	scan, err := scanner.Scan(ctx, startKey, endKey)
	if err != nil {
		return err
	}
	defer func() { _ = scan.Close() }()
	var keyBuffer, valueBuffer []byte
	for {
		key, value, end, err := scan.ReadRow(keyBuffer, valueBuffer)
		if err != nil {
			return err
		}
		if end {
			return nil
		}
		if err := emit(key, value); err != nil {
			return err
		}
		keyBuffer = key
		valueBuffer = value
	}
}
