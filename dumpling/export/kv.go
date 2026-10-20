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
	tcontext "github.com/pingcap/tidb/dumpling/context"
	"github.com/pingcap/tidb/dumpling/dumpservice"
	"github.com/pingcap/tidb/pkg/executor"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/format/textrow"
	tidbmeta "github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/pingcap/tidb/pkg/util/rowcodec"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
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

func newKVTableMeta(database string, table *model.TableInfo, createSQL string) *kvTableMeta {
	columns := kvVisibleColumns(table)
	types := make([]string, 0, len(columns))
	names := make([]string, 0, len(columns))
	selected := make([]string, 0, len(columns))
	for _, column := range columns {
		types = append(types, kvColumnType(column))
		names = append(names, column.Name.O)
		selected = append(selected, wrapBackTicks(column.Name.O))
	}
	return &kvTableMeta{
		database: database,
		table:    table.Name.O,
		columns:  columns,
		types:    types,
		names:    names,
		selected: strings.Join(selected, ","),
		create:   createSQL,
	}
}

func (*kvTableMeta) SpecialComments() StringIter { return newStringIter() }
func (m *kvTableMeta) DatabaseName() string      { return m.database }
func (m *kvTableMeta) TableName() string         { return m.table }
func (m *kvTableMeta) ColumnCount() uint         { return uint(len(m.columns)) }
func (m *kvTableMeta) ColumnTypes() []string     { return m.types }
func (m *kvTableMeta) ColumnNames() []string     { return m.names }
func (m *kvTableMeta) SelectedField() string     { return m.selected }
func (m *kvTableMeta) SelectedLen() int          { return len(m.columns) }
func (m *kvTableMeta) ShowCreateTable() string   { return m.create }
func (*kvTableMeta) ShowCreateView() string      { return "" }
func (*kvTableMeta) AvgRowLength() uint64        { return 0 }
func (*kvTableMeta) HasImplicitRowID() bool      { return false }

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
	ranges []dumpservice.Range,
	metrics *metrics,
) *kvTableData {
	return &kvTableData{
		scanner: scanner,
		table:   table,
		ranges:  ranges,
		metrics: metrics,
	}
}

func (d *kvTableData) Start(tctx *tcontext.Context, _ *sql.Conn) error {
	decoder := newKVRowDecoder(d.table)
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
	if i.decoder == nil {
		i.decoder = newKVRowDecoder(i.table)
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

func newKVRowDecoder(table *model.TableInfo) *kvRowDecoder {
	columns := kvVisibleColumns(table)
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

func kvVisibleColumns(table *model.TableInfo) []*model.ColumnInfo {
	columns := make([]*model.ColumnInfo, 0, len(table.Columns))
	for _, column := range table.Cols() {
		if column != nil && !column.Hidden && !column.IsGenerated() {
			columns = append(columns, column)
		}
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
	if err := executor.ConstructResultOfShowCreateDatabase(mock.NewContextDeprecated(), database, true, &output); err != nil {
		return "", errors.Annotatef(err, "build schema for kv database %q", database.Name.O)
	}
	return output.String(), nil
}

func kvCreateTableSQL(reader tidbmeta.Reader, databaseID int64, table *model.TableInfo) (string, error) {
	allocators, err := kvMetadataAllocators(reader, databaseID, table)
	if err != nil {
		return "", errors.Annotatef(err, "read auto ID bases for kv table %q", table.Name.O)
	}
	var output bytes.Buffer
	if err := executor.ConstructResultOfShowCreateTable(mock.NewContextDeprecated(), table, allocators, &output); err != nil {
		return "", errors.Annotatef(err, "build schema for kv table %q", table.Name.O)
	}
	return output.String(), nil
}

func (d *Dumper) dumpFromService() error {
	client, err := dumpservice.NewClient(d.conf.DumpService)
	if err != nil {
		return err
	}
	defer client.Close()
	return d.dumpFromScanner(client)
}

func (d *Dumper) dumpFromScanner(scanner kvScanner) error {
	snapshot, err := newKVMetadataSnapshot(d.tctx, scanner)
	if err != nil {
		return err
	}
	reader := tidbmeta.NewReader(snapshot)
	databases, err := d.loadKVTables(reader)
	if err != nil {
		return err
	}
	policies, err := reader.ListPolicies()
	if err != nil {
		return err
	}

	taskIn, taskOut := infiniteChan[Task]()
	wg, writingCtx := errgroup.WithContext(d.tctx)
	writerCtx := d.tctx.WithContext(writingCtx)
	writers := make([]*Writer, d.conf.Threads)
	for index := range d.conf.Threads {
		writer := NewWriter(writerCtx, int64(index), d.conf, nil, d.extStore, d.metrics)
		writer.rebuildConnFn = func(conn *sql.Conn, _ bool) (*sql.Conn, error) { return conn, nil }
		wg.Go(func() error { return writer.run(taskOut) })
		writers[index] = writer
	}

	start := time.Now()
	tableCount := 0
	send := func(task Task) error {
		if d.sendTaskToChan(writerCtx, task, taskIn) {
			return writerCtx.Err()
		}
		return nil
	}
	for _, policy := range policies {
		createSQL := executor.ConstructResultOfShowCreatePlacementPolicy(policy)
		if err := send(NewTaskPolicyMeta(policy.Name.O, fmt.Sprintf("/*T![placement] %s */", createSQL))); err != nil {
			close(taskIn)
			_ = wg.Wait()
			return err
		}
	}
	for _, entry := range databases {
		database := entry.database
		if !d.conf.NoSchemas {
			createSQL, err := kvCreateDatabaseSQL(database)
			if err != nil {
				close(taskIn)
				_ = wg.Wait()
				return err
			}
			if err := send(NewTaskDatabaseMeta(database.Name.O, createSQL)); err != nil {
				close(taskIn)
				_ = wg.Wait()
				return err
			}
		}
		for _, table := range entry.tables {
			tableCount++
			var createSQL string
			if !d.conf.NoSchemas {
				createSQL, err = kvCreateTableSQL(reader, database.ID, table)
				if err != nil {
					close(taskIn)
					_ = wg.Wait()
					return err
				}
				if err := send(NewTaskTableMeta(database.Name.O, table.Name.O, createSQL)); err != nil {
					close(taskIn)
					_ = wg.Wait()
					return err
				}
			}
			meta := newKVTableMeta(database.Name.O, table, createSQL)
			if !d.conf.NoData {
				ranges, err := shardKVTableRanges(d.tctx, scanner, table)
				if err != nil {
					close(taskIn)
					_ = wg.Wait()
					return err
				}
				for chunkIndex, rangeToScan := range ranges {
					data := newKVTableData(
						scanner,
						table,
						[]dumpservice.Range{rangeToScan},
						d.metrics,
					)
					if err := send(NewTaskTableData(meta, data, chunkIndex, len(ranges))); err != nil {
						close(taskIn)
						_ = wg.Wait()
						return err
					}
				}
			}
		}
	}
	atomic.StoreInt64(&d.totalTables, int64(tableCount))
	close(taskIn)
	if err := wg.Wait(); err != nil {
		return errors.Trace(err)
	}
	d.tctx.L().Info("finished dumping KV",
		zap.Int("tables", tableCount),
		zap.Int("tasks", countTotalTask(writers)),
		zap.Duration("duration", time.Since(start)))
	return nil
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
