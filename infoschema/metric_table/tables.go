package metric_table

import (
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/types"
	"github.com/prometheus/client_golang/api/prometheus/v1"
)

// metricSchemaTable stands for the fake table all its data is in the memory.
type metricSchemaTable struct {
	infoschema.VirtualTable
	meta *model.TableInfo
	cols []*table.Column
}

func tableFromMeta(alloc autoid.Allocator, meta *model.TableInfo) (table.Table, error) {
	return createmetricSchemaTable(meta), nil
}

// createmetricSchemaTable creates all metricSchemaTables
func createmetricSchemaTable(meta *model.TableInfo) *metricSchemaTable {
	columns := make([]*table.Column, 0, len(meta.Columns))
	for _, colInfo := range meta.Columns {
		col := table.ToColumn(colInfo)
		columns = append(columns, col)
	}
	t := &metricSchemaTable{
		meta: meta,
		cols: columns,
	}
	return t
}

// Cols implements table.Table Type interface.
func (vt *metricSchemaTable) Cols() []*table.Column {
	return vt.cols
}

// WritableCols implements table.Table Type interface.
func (vt *metricSchemaTable) WritableCols() []*table.Column {
	return vt.cols
}

// GetID implements table.Table GetID interface.
func (vt *metricSchemaTable) GetPhysicalID() int64 {
	return vt.meta.ID
}

// Meta implements table.Table Type interface.
func (vt *metricSchemaTable) Meta() *model.TableInfo {
	return vt.meta
}

type promQLQueryRange = v1.Range

func (vt *metricSchemaTable) getRows(ctx sessionctx.Context, cols []*table.Column) (fullRows [][]types.Datum, err error) {
	tblDef, ok := metricTableMap[vt.meta.Name.L]
	if !ok {
		return nil, errors.Errorf("can not find metric table: %v", vt.meta.Name.L)
	}

	metricAddr := getMetricAddr()
	// TODO:
	queryRange := getDefaultQueryRange()
	queryValue, err := queryMetric(metricAddr, tblDef, queryRange)
	if err != nil {
		return nil, err
	}

	fullRows = tblDef.genRows(queryValue, queryRange)
	if len(cols) == len(vt.cols) {
		return
	}
	rows := make([][]types.Datum, len(fullRows))
	for i, fullRow := range fullRows {
		row := make([]types.Datum, len(cols))
		for j, col := range cols {
			row[j] = fullRow[col.Offset]
		}
		rows[i] = row
	}
	return rows, nil
}

func getDefaultQueryRange() promQLQueryRange {
	return promQLQueryRange{Start: time.Now(), End: time.Now(), Step: time.Minute}
}

// IterRecords implements table.Table IterRecords interface.
func (vt *metricSchemaTable) IterRecords(ctx sessionctx.Context, startKey kv.Key, cols []*table.Column,
	fn table.RecordIterFunc) error {
	if len(startKey) != 0 {
		return table.ErrUnsupportedOp
	}
	rows, err := vt.getRows(ctx, cols)
	if err != nil {
		return err
	}
	for i, row := range rows {
		more, err := fn(int64(i), row, cols)
		if err != nil {
			return err
		}
		if !more {
			break
		}
	}
	return nil
}
