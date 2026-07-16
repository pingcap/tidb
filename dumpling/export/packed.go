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
	"github.com/pingcap/tidb/pkg/meta/autoid"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	tidbtable "github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/mock"
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
	if column.GetCharset() == charset.CharsetBin {
		switch column.GetType() {
		case mysql.TypeString, mysql.TypeVarchar, mysql.TypeVarString,
			mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob, mysql.TypeBlob:
			return "BLOB"
		}
	}
	switch column.GetType() {
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong:
		return "BIGINT"
	case mysql.TypeFloat:
		return "FLOAT"
	case mysql.TypeDouble:
		return "DOUBLE"
	case mysql.TypeNewDecimal:
		return "DECIMAL"
	case mysql.TypeBit:
		return "BIT"
	case mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob, mysql.TypeBlob, mysql.TypeGeometry:
		return "BLOB"
	case mysql.TypeDate, mysql.TypeNewDate:
		return "DATE"
	case mysql.TypeDatetime:
		return "DATETIME"
	case mysql.TypeTimestamp:
		return "TIMESTAMP"
	case mysql.TypeDuration:
		return "TIME"
	case mysql.TypeYear:
		return "YEAR"
	case mysql.TypeEnum:
		return "ENUM"
	case mysql.TypeSet:
		return "SET"
	case mysql.TypeJSON:
		return "JSON"
	default:
		return "VARCHAR"
	}
}

type packedTableData struct {
	pool     *cseDumperPool
	table    *model.TableInfo
	tableIDs []int64
	iter     *packedRowIter
}

func newPackedTableData(pool *cseDumperPool, table *model.TableInfo) *packedTableData {
	return &packedTableData{
		pool:     pool,
		table:    table,
		tableIDs: packedPhysicalTableIDs(table),
	}
}

func (d *packedTableData) Start(tctx *tcontext.Context, _ *sql.Conn) error {
	decoder, err := newPackedRowDecoder(d.table)
	if err != nil {
		return err
	}
	client, err := d.pool.acquire(tctx)
	if err != nil {
		return errors.Trace(err)
	}
	if err := client.startScan(d.tableIDs); err != nil {
		_ = d.pool.release(client, false)
		return err
	}
	iter := &packedRowIter{
		ctx:     tctx,
		pool:    d.pool,
		client:  client,
		table:   d.table,
		decoder: decoder,
		args:    make([]any, len(decoder.columns)),
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
	ctx      context.Context
	pool     *cseDumperPool
	client   *cseDumperClient
	table    *model.TableInfo
	decoder  *packedRowDecoder
	key      []byte
	value    []byte
	args     []any
	defaults expression.BuildContext
	err      error
	hasRow   bool
	finished bool
}

func (i *packedRowIter) HasNext() bool { return i.err == nil && i.hasRow }

func (i *packedRowIter) Decode(receiver RowReceiver) error {
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
	if i.defaults == nil {
		i.defaults = exprstatic.NewExprContext()
	}
	values, err := i.decoder.decode(i.table, i.key, i.value, i.defaults)
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
		data, err := values[index].ToBytes()
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

func (i *packedRowIter) Next() {
	if i.HasNext() {
		i.readNext()
	}
}

func (i *packedRowIter) Error() error { return i.err }

func (i *packedRowIter) Close() error {
	if i.client == nil {
		return nil
	}
	client := i.client
	i.client = nil
	return i.pool.release(client, i.finished && i.err == nil)
}

func (i *packedRowIter) readNext() {
	if i.client == nil {
		i.hasRow = false
		return
	}
	key, value, end, err := i.client.readRow(i.key, i.value)
	if err != nil {
		i.err = err
		i.hasRow = false
		client := i.client
		i.client = nil
		if releaseErr := i.pool.release(client, false); releaseErr != nil {
			i.err = errors.Annotatef(i.err, "replace failed CSE dumper: %v", releaseErr)
		}
		return
	}
	if end {
		i.hasRow = false
		i.finished = true
		client := i.client
		i.client = nil
		if err := i.pool.release(client, true); err != nil {
			i.err = err
		}
		return
	}
	i.key = key
	i.value = value
	i.hasRow = true
}

type packedRowDecoder struct {
	columns             []*model.ColumnInfo
	columnTypes         map[int64]*types.FieldType
	commonHandleOffsets map[int64]int
}

func newPackedRowDecoder(table *model.TableInfo) (*packedRowDecoder, error) {
	columns := packedVisibleColumns(table)
	columnTypes := make(map[int64]*types.FieldType, len(columns))
	for _, column := range columns {
		if !table.PKIsHandle || !mysql.HasPriKeyFlag(column.GetFlag()) {
			columnTypes[column.ID] = &column.FieldType
		}
	}
	commonHandleOffsets, err := packedCommonHandleColumnOffsets(table)
	if err != nil {
		return nil, err
	}
	return &packedRowDecoder{
		columns:             columns,
		columnTypes:         columnTypes,
		commonHandleOffsets: commonHandleOffsets,
	}, nil
}

func (d *packedRowDecoder) decode(
	table *model.TableInfo,
	key, value []byte,
	defaults expression.BuildContext,
) ([]types.Datum, error) {
	handle, err := tablecodec.DecodeRowKey(key)
	if err != nil {
		return nil, errors.Annotatef(err, "decode packed backup row key %x", key)
	}
	rowMap, err := tablecodec.DecodeRowToDatumMap(value, d.columnTypes, time.UTC)
	if err != nil {
		return nil, errors.Annotatef(err, "decode packed backup row value at key %x", key)
	}
	values := make([]types.Datum, 0, len(d.columns))
	for _, column := range d.columns {
		decoded, err := decodePackedColumn(table, column, handle, rowMap, d.commonHandleOffsets, defaults)
		if err != nil {
			return nil, errors.Annotatef(err, "decode column %q at packed backup key %x", column.Name.O, key)
		}
		values = append(values, decoded)
	}
	return values, nil
}

func decodePackedColumn(
	table *model.TableInfo,
	column *model.ColumnInfo,
	handle kv.Handle,
	rowMap map[int64]types.Datum,
	commonHandleOffsets map[int64]int,
	defaults expression.BuildContext,
) (types.Datum, error) {
	if table.PKIsHandle && mysql.HasPriKeyFlag(column.GetFlag()) {
		var value types.Datum
		if mysql.HasUnsignedFlag(column.GetFlag()) {
			value.SetUint64(uint64(handle.IntValue()))
		} else {
			value.SetInt64(handle.IntValue())
		}
		return value, nil
	}
	if handleOffset, ok := commonHandleOffsets[column.ID]; ok {
		if handleOffset >= handle.NumCols() {
			return types.Datum{}, errors.Errorf("common handle has %d columns, need offset %d", handle.NumCols(), handleOffset)
		}
		_, value, err := codec.DecodeOne(handle.EncodedCol(handleOffset))
		if err != nil {
			return types.Datum{}, err
		}
		return tablecodec.Unflatten(value, &column.FieldType, time.UTC)
	}
	if value, ok := rowMap[column.ID]; ok {
		return value, nil
	}
	return tidbtable.GetColOriginDefaultValue(defaults, column)
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

func packedCommonHandleColumnOffsets(table *model.TableInfo) (map[int64]int, error) {
	offsets := make(map[int64]int)
	if !table.IsCommonHandle {
		return offsets, nil
	}
	primary := table.GetPrimaryKey()
	if primary == nil || !primary.Primary {
		return nil, errors.Errorf("packed backup table %q has a common handle without a primary index", table.Name.O)
	}
	for handleOffset, indexColumn := range primary.Columns {
		if indexColumn.Offset < 0 || indexColumn.Offset >= len(table.Columns) {
			return nil, errors.Errorf("packed backup table %q has invalid primary column offset %d", table.Name.O, indexColumn.Offset)
		}
		offsets[table.Columns[indexColumn.Offset].ID] = handleOffset
	}
	return offsets, nil
}

func decodePackedManifest(manifest *packedManifest) ([]*model.DBInfo, error) {
	databases := make([]*model.DBInfo, 0, len(manifest.Databases))
	for _, encoded := range manifest.Databases {
		database := &model.DBInfo{}
		if err := json.Unmarshal(encoded.Database, database); err != nil {
			return nil, errors.Annotate(err, "decode CSE database schema")
		}
		for _, encodedTable := range encoded.Tables {
			table := &model.TableInfo{}
			if err := json.Unmarshal(encodedTable, table); err != nil {
				return nil, errors.Annotatef(err, "decode CSE table schema in database %q", database.Name.O)
			}
			database.Deprecated.Tables = append(database.Deprecated.Tables, table)
		}
		slices.SortFunc(database.Deprecated.Tables, func(left, right *model.TableInfo) int {
			return strings.Compare(left.Name.L, right.Name.L)
		})
		databases = append(databases, database)
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

func packedCreateTableSQL(table *model.TableInfo) (string, error) {
	var output bytes.Buffer
	if err := executor.ConstructResultOfShowCreateTable(mock.NewContextDeprecated(), table, autoid.Allocators{}, &output); err != nil {
		return "", errors.Annotatef(err, "build schema for packed table %q", table.Name.O)
	}
	return output.String(), nil
}

func openPackedBackup(d *Dumper) error {
	pool, err := newCSEDumperPool(d.tctx, d.conf.Threads, d.conf.CSEExecutable, d.conf.PackedBackup)
	if err != nil {
		return err
	}
	d.packedPool = pool
	return nil
}

func (d *Dumper) dumpPacked() error {
	manifest, err := d.packedPool.schema(d.tctx)
	if err != nil {
		return err
	}
	databases, err := decodePackedManifest(manifest)
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
				createSQL, err = packedCreateTableSQL(table)
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
				if err := send(NewTaskTableData(meta, newPackedTableData(d.packedPool, table), 0, 1)); err != nil {
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
		zap.Uint64("cluster ID", manifest.ClusterID),
		zap.Uint32("keyspace ID", manifest.KeyspaceID),
		zap.Uint64("read timestamp", manifest.ReadTS),
		zap.Int("tables", tableCount),
		zap.Int("tasks", countTotalTask(writers)),
		zap.Duration("duration", time.Since(start)))
	return nil
}

func (m *packedTableMeta) String() string {
	return fmt.Sprintf("%s.%s", m.database, m.table)
}
