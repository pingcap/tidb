// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package tables

import (
	"bytes"
	"fmt"
	"sort"
	"strings"

	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

// Both Partition and PartitionedTable implement the table.Table interface.
var _ table.Table = &Partition{}
var _ table.Table = &PartitionedTable{}

// PartitionedTable implements the table.PartitionedTable interface.
var _ table.PartitionedTable = &PartitionedTable{}

// Partition is a feature from MySQL:
// See https://dev.mysql.com/doc/refman/8.0/en/partitioning.html
// A partition table may contain many partitions, each partition has a unique partition
// id. The underlying representation of a partition and a normal table (a table with no
// partitions) is basically the same.
// Partition also implements the table.Table interface.
type Partition struct {
	tableCommon
}

// PartitionedTable implements the table.PartitionedTable interface.
// PartitionedTable is a table, it contains many Partitions.
type PartitionedTable struct {
	Table
	partitionExpr *PartitionExpr
	partitions    map[int64]*Partition
}

func newPartitionedTable(tbl *Table, tblInfo *model.TableInfo) (table.Table, error) {
	partitionExpr, err := generatePartitionExpr(tblInfo)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	partitions := make(map[int64]*Partition)
	pi := tblInfo.GetPartitionInfo()
	for _, p := range pi.Definitions {
		var t Partition
		err = initTableCommonWithIndices(&t.tableCommon, tblInfo, p.ID, tbl.Columns, tbl.alloc)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		partitions[p.ID] = &t
	}

	return &PartitionedTable{
		Table:         *tbl,
		partitionExpr: partitionExpr,
		partitions:    partitions,
	}, nil
}

// PartitionExpr is the partition definition expressions.
// There are two expressions exist, because Locate use binary search, which requires:
// Given a compare function, for any partition range i, if cmp[i] > 0, then cmp[i+1] > 0.
// While partition prune must use the accurate range to do prunning.
// partition by range (x)
//   (partition
//      p1 values less than (y1)
//      p2 values less than (y2)
//      p3 values less than (y3))
// Ranges: (x < y1); (y1 <= x < y2); (y2 <= x < y3)
// UpperBounds: (x < y1); (x < y2); (x < y3)
type PartitionExpr struct {
	Ranges      []expression.Expression
	UpperBounds []expression.Expression
}

func generatePartitionExpr(tblInfo *model.TableInfo) (*PartitionExpr, error) {
	// The caller should assure partition info is not nil.
	pi := tblInfo.GetPartitionInfo()
	ctx := mock.NewContext()
	partitionPruneExprs := make([]expression.Expression, 0, len(pi.Definitions))
	locateExprs := make([]expression.Expression, 0, len(pi.Definitions))
	var buf bytes.Buffer
	for i := 0; i < len(pi.Definitions); i++ {
		if strings.EqualFold(pi.Definitions[i].LessThan[0], "MAXVALUE") {
			// Expr less than maxvalue is always true.
			fmt.Fprintf(&buf, "true")
		} else {
			fmt.Fprintf(&buf, "((%s) < (%s))", pi.Expr, pi.Definitions[i].LessThan[0])
		}
		expr, err := expression.ParseSimpleExpr(ctx, buf.String(), tblInfo)
		if err != nil {
			// If it got an error here, ddl may hang forever, so this error log is important.
			log.Error("wrong table partition expression:", fmt.Sprintf("%+v", err), buf.String())
			return nil, errors.WithStack(err)
		}
		locateExprs = append(locateExprs, expr)

		if i > 0 {
			fmt.Fprintf(&buf, " and ((%s) >= (%s))", pi.Expr, pi.Definitions[i-1].LessThan[0])
		}

		expr, err = expression.ParseSimpleExpr(ctx, buf.String(), tblInfo)
		if err != nil {
			// If it got an error here, ddl may hang forever, so this error log is important.
			log.Error("wrong table partition expression:", fmt.Sprintf("%+v", err), buf.String())
			return nil, errors.WithStack(err)
		}
		partitionPruneExprs = append(partitionPruneExprs, expr)
		buf.Reset()
	}
	return &PartitionExpr{
		Ranges:      partitionPruneExprs,
		UpperBounds: locateExprs,
	}, nil
}

// PartitionExpr returns the partition expression.
func (t *PartitionedTable) PartitionExpr() *PartitionExpr {
	return t.partitionExpr
}

func partitionRecordKey(pid int64, handle int64) kv.Key {
	recordPrefix := tablecodec.GenTableRecordPrefix(pid)
	return tablecodec.EncodeRecordKey(recordPrefix, handle)
}

// locatePartition returns the partition ID of the input record.
func (t *PartitionedTable) locatePartition(ctx sessionctx.Context, pi *model.PartitionInfo, r []types.Datum) (int64, error) {
	var err error
	partitionExprs := t.partitionExpr.UpperBounds
	idx := sort.Search(len(partitionExprs), func(i int) bool {
		var ret int64
		ret, _, err = partitionExprs[i].EvalInt(ctx, types.DatumRow(r))
		if err != nil {
			return true // Break the search.
		}
		return ret > 0
	})
	if err != nil {
		return 0, errors.WithStack(err)
	}
	if idx < 0 || idx >= len(partitionExprs) {
		// The data does not belong to any of the partition?
		return 0, errors.WithStack(table.ErrTrgInvalidCreationCtx)
	}
	return pi.Definitions[idx].ID, nil
}

// GetPartition returns a Table, which is actually a Partition.
func (t *PartitionedTable) GetPartition(pid int64) table.Table {
	return t.partitions[pid]
}

// AddRecord implements the AddRecord method for the table.Table interface.
func (t *PartitionedTable) AddRecord(ctx sessionctx.Context, r []types.Datum, skipHandleCheck bool) (recordID int64, err error) {
	partitionInfo := t.meta.GetPartitionInfo()
	pid, err := t.locatePartition(ctx, partitionInfo, r)
	if err != nil {
		return 0, errors.WithStack(err)
	}

	tbl := t.GetPartition(pid)
	return tbl.AddRecord(ctx, r, skipHandleCheck)
}
