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
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	tcontext "github.com/pingcap/tidb/dumpling/context"
	"github.com/pingcap/tidb/pkg/executor"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/expression/exprstatic"
	"github.com/pingcap/tidb/pkg/kv"
	tidbmeta "github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/autoid"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/structure"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/codec"
	formatutil "github.com/pingcap/tidb/pkg/util/format"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/pingcap/tidb/pkg/util/redact"
	"github.com/pingcap/tidb/pkg/util/rowcodec"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

type packedTableMeta struct {
	database string
	table    string
	columns  []*model.ColumnInfo
	types    []string
	names    []string
	selected string
	create   string
}

func newPackedTableMeta(database string, table *model.TableInfo, createSQL string) *packedTableMeta {
	columns := packedVisibleColumns(table)
	types := make([]string, 0, len(columns))
	names := make([]string, 0, len(columns))
	selected := make([]string, 0, len(columns))
	for _, column := range columns {
		types = append(types, packedColumnType(column))
		names = append(names, column.Name.O)
		selected = append(selected, wrapBackTicks(column.Name.O))
	}
	return &packedTableMeta{
		database: database,
		table:    table.Name.O,
		columns:  columns,
		types:    types,
		names:    names,
		selected: strings.Join(selected, ","),
		create:   createSQL,
	}
}

func (*packedTableMeta) SpecialComments() StringIter { return newStringIter() }
func (m *packedTableMeta) DatabaseName() string      { return m.database }
func (m *packedTableMeta) TableName() string         { return m.table }
func (m *packedTableMeta) ColumnCount() uint         { return uint(len(m.columns)) }
func (m *packedTableMeta) ColumnTypes() []string     { return m.types }
func (m *packedTableMeta) ColumnNames() []string     { return m.names }
func (m *packedTableMeta) SelectedField() string     { return m.selected }
func (m *packedTableMeta) SelectedLen() int          { return len(m.columns) }
func (m *packedTableMeta) ShowCreateTable() string   { return m.create }
func (*packedTableMeta) ShowCreateView() string      { return "" }
func (*packedTableMeta) AvgRowLength() uint64        { return 0 }
func (*packedTableMeta) HasImplicitRowID() bool      { return false }

func packedColumnType(column *model.ColumnInfo) string {
	return strings.ToUpper(types.TypeToStr(column.GetType(), column.GetCharset()))
}

type packedTableData struct {
	executable       string
	metadataURL      string
	legacyEncryption bool
	table            *model.TableInfo
	ranges           []packedRange
	observation      *packedExportObservation
	iter             *packedRowIter
}

type packedRange struct {
	start []byte
	end   []byte
}

func newPackedTableData(
	executable, metadataURL string,
	legacyEncryption bool,
	table *model.TableInfo,
	observation *packedExportObservation,
) *packedTableData {
	return &packedTableData{
		executable:       executable,
		metadataURL:      metadataURL,
		legacyEncryption: legacyEncryption,
		table:            table,
		ranges:           packedPhysicalTableRanges(table),
		observation:      observation,
	}
}

func (d *packedTableData) Start(tctx *tcontext.Context, _ *sql.Conn) error {
	decoder, err := newPackedRowDecoder(d.table)
	if err != nil {
		return err
	}
	iter := &packedRowIter{
		ctx:              tctx,
		executable:       d.executable,
		metadataURL:      d.metadataURL,
		legacyEncryption: d.legacyEncryption,
		ranges:           d.ranges,
		observation:      d.observation,
		table:            d.table,
		decoder:          decoder,
		args:             make([]any, len(decoder.columns)),
	}
	iter.readNext()
	d.iter = iter
	return iter.err
}

func (d *packedTableData) Rows() SQLRowIter { return d.iter }

func (d *packedTableData) Close() error {
	if d.iter != nil {
		return d.iter.Close()
	}
	return nil
}

func (*packedTableData) RawRows() *sql.Rows { return nil }

type packedRowIter struct {
	ctx              context.Context
	executable       string
	metadataURL      string
	legacyEncryption bool
	ranges           []packedRange
	nextRange        int
	scan             *cseDumperScan
	observation      *packedExportObservation
	table            *model.TableInfo
	decoder          *packedRowDecoder
	key              []byte
	value            []byte
	args             []any
	err              error
	hasRow           bool
}

func (i *packedRowIter) HasNext() bool { return i.err == nil && i.hasRow }

func (i *packedRowIter) Decode(receiver RowReceiver) error {
	if i.observation == nil {
		return i.decode(receiver)
	}
	return i.observation.decode(func() error { return i.decode(receiver) })
}

func (i *packedRowIter) decode(receiver RowReceiver) error {
	if !i.HasNext() {
		return errors.New("packed backup row iterator has no current row")
	}
	if i.decoder == nil {
		decoder, err := newPackedRowDecoder(i.table)
		if err != nil {
			return err
		}
		i.decoder = decoder
	}
	values, err := i.decoder.decode(i.key, i.value)
	if err != nil {
		return err
	}
	if len(values) != len(i.args) {
		return errors.Errorf("packed backup row has %d values for %d columns", len(values), len(i.args))
	}
	receiver.BindAddress(i.args)
	for index := range values {
		destination, ok := i.args[index].(*sql.RawBytes)
		if !ok {
			return errors.Errorf("unsupported Dumpling packed row receiver %T", i.args[index])
		}
		if values[index].IsNull() {
			*destination = nil
			continue
		}
		data, err := packedDatumBytes(i.decoder.columns[index], &values[index])
		if err != nil {
			return errors.Annotatef(err, "format packed backup column %d", index)
		}
		*destination = append((*destination)[:0], data...)
		if len(data) == 0 {
			*destination = sql.RawBytes{}
		}
	}
	return nil
}

func packedDatumBytes(column *model.ColumnInfo, value *types.Datum) ([]byte, error) {
	switch column.GetType() {
	case mysql.TypeFloat:
		return formatutil.AppendFormatFloat(nil, float64(value.GetFloat32()), types.UnspecifiedLength, 32), nil
	case mysql.TypeDouble:
		return formatutil.AppendFormatFloat(nil, value.GetFloat64(), types.UnspecifiedLength, 64), nil
	case mysql.TypeYear:
		return formatutil.AppendFormatYear(nil, value.GetInt64()), nil
	default:
		return value.ToBytes()
	}
}

func (i *packedRowIter) Next() {
	if i.HasNext() {
		i.readNext()
	}
}

func (i *packedRowIter) Error() error { return i.err }

func (i *packedRowIter) Close() error {
	if i.scan == nil {
		return nil
	}
	scan := i.scan
	i.scan = nil
	return scan.close()
}

func (i *packedRowIter) readNext() {
	for {
		if i.scan == nil {
			if i.nextRange == len(i.ranges) {
				i.hasRow = false
				return
			}
			rangeToScan := i.ranges[i.nextRange]
			i.nextRange++
			scan, err := startCSEDumperScan(
				i.ctx,
				i.executable,
				i.metadataURL,
				i.legacyEncryption,
				rangeToScan.start,
				rangeToScan.end,
				i.observation,
			)
			if err != nil {
				i.err = err
				i.hasRow = false
				return
			}
			i.scan = scan
		}

		key, value, end, err := i.scan.readRow(i.key, i.value)
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

type packedRowDecoder struct {
	columns     []*model.ColumnInfo
	fieldTypes  []*types.FieldType
	schema      *expression.Schema
	table       *model.TableInfo
	decodeCtx   expression.BuildContext
	rowDecoder  *rowcodec.ChunkDecoder
	decodeChunk *chunk.Chunk
}

func newPackedRowDecoder(table *model.TableInfo) (*packedRowDecoder, error) {
	if err := validatePackedCommonHandle(table); err != nil {
		return nil, err
	}
	columns := packedVisibleColumns(table)
	fieldTypes := make([]*types.FieldType, len(columns))
	schemaColumns := make([]*expression.Column, len(columns))
	for index, column := range columns {
		fieldTypes[index] = &column.FieldType
		schemaColumns[index] = &expression.Column{ID: column.ID, RetType: &column.FieldType}
	}
	schema := expression.NewSchema(schemaColumns...)
	decodeCtx := exprstatic.NewExprContext()
	return &packedRowDecoder{
		columns:     columns,
		fieldTypes:  fieldTypes,
		schema:      schema,
		table:       table,
		decodeCtx:   decodeCtx,
		rowDecoder:  executor.NewRowDecoderWithBuildContext(decodeCtx, time.UTC, schema, table),
		decodeChunk: chunk.New(fieldTypes, 1, 1),
	}, nil
}

func validatePackedCommonHandle(table *model.TableInfo) error {
	if !table.IsCommonHandle {
		return nil
	}
	primary := table.GetPrimaryKey()
	if primary == nil || !primary.Primary {
		return errors.Errorf("packed backup table %q has a common handle without a primary index", table.Name.O)
	}
	for _, indexColumn := range primary.Columns {
		if indexColumn.Offset < 0 || indexColumn.Offset >= len(table.Columns) {
			return errors.Errorf("packed backup table %q has invalid primary column offset %d", table.Name.O, indexColumn.Offset)
		}
	}
	return nil
}

func (d *packedRowDecoder) decode(
	key, value []byte,
) ([]types.Datum, error) {
	handle, err := tablecodec.DecodeRowKey(key)
	if err != nil {
		return nil, errors.Annotatef(err, "decode packed backup row key %s", redact.Key(key))
	}
	d.decodeChunk.Reset()
	if err := executor.DecodeRowValToChunkWithBuildContext(
		d.decodeCtx,
		time.UTC,
		d.schema,
		d.table,
		handle,
		value,
		d.decodeChunk,
		d.rowDecoder,
	); err != nil {
		return nil, errors.Annotatef(err, "decode packed backup row value at key %s", redact.Key(key))
	}
	return d.decodeChunk.GetRow(0).GetDatumRow(d.fieldTypes), nil
}

func packedVisibleColumns(table *model.TableInfo) []*model.ColumnInfo {
	columns := make([]*model.ColumnInfo, 0, len(table.Columns))
	for _, column := range table.Cols() {
		if column != nil && !column.Hidden && !column.IsGenerated() {
			columns = append(columns, column)
		}
	}
	return columns
}

func packedPhysicalTableIDs(table *model.TableInfo) []int64 {
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

func packedPhysicalTableRanges(table *model.TableInfo) []packedRange {
	tableIDs := packedPhysicalTableIDs(table)
	ranges := make([]packedRange, 0, len(tableIDs))
	for _, tableID := range tableIDs {
		start := tablecodec.GenTableRecordPrefix(tableID)
		ranges = append(ranges, packedRange{start: start, end: start.PrefixNext()})
	}
	return ranges
}

type packedRangeScanner func(
	ctx context.Context,
	startKey, endKey []byte,
	emit func(key, value []byte) error,
) error

func newCSEDumperRangeScanner(
	executable, metadataURL string,
	legacyEncryption bool,
	observation *packedExportObservation,
) packedRangeScanner {
	return func(
		ctx context.Context,
		startKey, endKey []byte,
		emit func(key, value []byte) error,
	) error {
		return scanCSEDumperRange(
			ctx,
			executable,
			metadataURL,
			legacyEncryption,
			startKey,
			endKey,
			emit,
			observation,
		)
	}
}

func packedHashDataPrefix(hashKey []byte) kv.Key {
	prefix := []byte{'m'}
	prefix = codec.EncodeBytes(prefix, hashKey)
	return codec.EncodeUint(prefix, uint64(structure.HashData))
}

func scanPackedHash(
	ctx context.Context,
	scan packedRangeScanner,
	hashKey []byte,
	emit func(field, value []byte) error,
) error {
	prefix := packedHashDataPrefix(hashKey)
	return scan(ctx, prefix, prefix.PrefixNext(), func(key, value []byte) error {
		if !bytes.HasPrefix(key, prefix) {
			return errors.Errorf("packed metadata key %x is outside hash prefix %x", key, prefix)
		}
		remaining, field, err := codec.DecodeBytes(key[len(prefix):], nil)
		if err != nil {
			return errors.Annotatef(err, "decode packed metadata hash key %x", key)
		}
		if len(remaining) != 0 {
			return errors.Errorf("packed metadata hash key %x has %d trailing bytes", key, len(remaining))
		}
		return emit(field, value)
	})
}

func loadPackedDatabases(ctx context.Context, scan packedRangeScanner) ([]*model.DBInfo, error) {
	var databases []*model.DBInfo
	if err := scanPackedHash(ctx, scan, []byte("DBs"), func(field, value []byte) error {
		if !tidbmeta.IsDBkey(field) {
			return nil
		}
		database := &model.DBInfo{}
		if err := json.Unmarshal(value, database); err != nil {
			return errors.Annotatef(err, "decode packed database schema at field %q", field)
		}
		if database.State == model.StatePublic {
			databases = append(databases, database)
		}
		return nil
	}); err != nil {
		return nil, err
	}

	for _, database := range databases {
		if err := scanPackedHash(ctx, scan, tidbmeta.DBkey(database.ID), func(field, value []byte) error {
			if !tidbmeta.IsTableKey(field) {
				return nil
			}
			table := &model.TableInfo{}
			if err := json.Unmarshal(value, table); err != nil {
				return errors.Annotatef(err, "decode packed table schema in database %q", database.Name.O)
			}
			if table.State == model.StatePublic {
				database.Deprecated.Tables = append(database.Deprecated.Tables, table)
			}
			return nil
		}); err != nil {
			return nil, err
		}
		slices.SortFunc(database.Deprecated.Tables, func(left, right *model.TableInfo) int {
			return strings.Compare(left.Name.L, right.Name.L)
		})
	}
	slices.SortFunc(databases, model.LessDBInfo)
	return databases, nil
}

func packedCreateDatabaseSQL(database *model.DBInfo) (string, error) {
	var output bytes.Buffer
	if err := executor.ConstructResultOfShowCreateDatabase(mock.NewContextDeprecated(), database, true, &output); err != nil {
		return "", errors.Annotatef(err, "build schema for packed database %q", database.Name.O)
	}
	return output.String(), nil
}

func packedCreateTableSQL(database ast.CIStr, table *model.TableInfo) (string, error) {
	var output bytes.Buffer
	if err := executor.ConstructResultOfShowCreateTableWithDB(
		mock.NewContextDeprecated(), database, table, autoid.Allocators{}, &output,
	); err != nil {
		return "", errors.Annotatef(err, "build schema for packed table %q", table.Name.O)
	}
	return output.String(), nil
}

func (d *Dumper) dumpPacked() (resultErr error) {
	observation := newPackedExportObservation(d.tctx)
	defer func() {
		observation.finish(resultErr)
	}()
	databases, err := loadPackedDatabases(
		d.tctx,
		newCSEDumperRangeScanner(
			d.conf.CSEExecutable,
			d.conf.PackedBackup,
			d.conf.CSELegacyEncryption,
			observation,
		),
	)
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
	for _, database := range databases {
		if !d.conf.TableFilter.MatchSchema(database.Name.O) {
			continue
		}
		if !d.conf.NoSchemas {
			createSQL, err := packedCreateDatabaseSQL(database)
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
		for _, table := range database.Deprecated.Tables {
			if table.IsView() || table.IsSequence() || !d.conf.TableFilter.MatchTable(database.Name.O, table.Name.O) {
				continue
			}
			tableCount++
			var createSQL string
			if !d.conf.NoSchemas {
				createSQL, err = packedCreateTableSQL(database.Name, table)
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
			meta := newPackedTableMeta(database.Name.O, table, createSQL)
			if !d.conf.NoData {
				data := newPackedTableData(
					d.conf.CSEExecutable,
					d.conf.PackedBackup,
					d.conf.CSELegacyEncryption,
					table,
					observation,
				)
				if err := send(NewTaskTableData(meta, data, 0, 1)); err != nil {
					close(taskIn)
					_ = wg.Wait()
					return err
				}
			}
		}
	}
	atomic.StoreInt64(&d.totalTables, int64(tableCount))
	close(taskIn)
	if err := wg.Wait(); err != nil {
		return errors.Trace(err)
	}
	d.tctx.L().Info("finished dumping packed backup",
		zap.Int("tables", tableCount),
		zap.Int("tasks", countTotalTask(writers)),
		zap.Duration("duration", time.Since(start)))
	return nil
}

func (m *packedTableMeta) String() string {
	return fmt.Sprintf("%s.%s", m.database, m.table)
}
