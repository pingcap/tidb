// Copyright 2022 PingCAP, Inc.
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

package dbutil

import (
	"context"
	"fmt"
	"sort"
	"strconv"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/parser/model"
)

// IndexInfo contains information of table index.
type IndexInfo struct {
	Table       string
	NoneUnique  bool
	KeyName     string
	SeqInIndex  int
	ColumnName  string
	Cardinality int
}

// ShowIndex returns result of executing `show index`
func ShowIndex(ctx context.Context, db QueryExecutor, schemaName string, table string) ([]*IndexInfo, error) {
	/*
		show index example result:
		mysql> show index from test;
		+-------+------------+----------+--------------+-------------+-----------+-------------+----------+--------+------+------------+---------+---------------+
		| Table | Non_unique | Key_name | Seq_in_index | Column_name | Collation | Cardinality | Sub_part | Packed | Null | Index_type | Comment | Index_comment |
		+-------+------------+----------+--------------+-------------+-----------+-------------+----------+--------+------+------------+---------+---------------+
		| test  | 0          | PRIMARY  | 1            | id          | A         | 0           | NULL     | NULL   |      | BTREE      |         |               |
		| test  | 0          | aid      | 1            | aid         | A         | 0           | NULL     | NULL   | YES  | BTREE      |         |               |
		+-------+------------+----------+--------------+-------------+-----------+-------------+----------+--------+------+------------+---------+---------------+
	*/
	indices := make([]*IndexInfo, 0, 3)
	query := fmt.Sprintf("SHOW INDEX FROM %s", TableName(schemaName, table))
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer rows.Close()

	for rows.Next() {
		fields, err1 := ScanRow(rows)
		if err1 != nil {
			return nil, errors.Trace(err1)
		}
		seqInIndex, err1 := strconv.Atoi(string(fields["Seq_in_index"].Data))
		if err != nil {
			return nil, errors.Trace(err1)
		}
		cardinality, err1 := strconv.Atoi(string(fields["Cardinality"].Data))
		if err != nil {
			return nil, errors.Trace(err1)
		}
		index := &IndexInfo{
			Table:       string(fields["Table"].Data),
			NoneUnique:  string(fields["Non_unique"].Data) == "1",
			KeyName:     string(fields["Key_name"].Data),
			ColumnName:  string(fields["Column_name"].Data),
			SeqInIndex:  seqInIndex,
			Cardinality: cardinality,
		}
		indices = append(indices, index)
	}

	return indices, nil
}

// FindSuitableColumnWithIndex returns first column of a suitable index.
// The priority is
// * primary key
// * unique key
// * normal index which has max cardinality
func FindSuitableColumnWithIndex(ctx context.Context, db QueryExecutor, schemaName string, tableInfo *model.TableInfo) (*model.ColumnInfo, error) {
	// find primary key
	for _, index := range tableInfo.Indices {
		if index.Primary {
			return FindColumnByName(tableInfo.Columns, index.Columns[0].Name.O), nil
		}
	}

	// no primary key found, seek unique index
	for _, index := range tableInfo.Indices {
		if index.Unique {
			return FindColumnByName(tableInfo.Columns, index.Columns[0].Name.O), nil
		}
	}

	// no unique index found, seek index with max cardinality
	indices, err := ShowIndex(ctx, db, schemaName, tableInfo.Name.O)
	if err != nil {
		return nil, errors.Trace(err)
	}
	var c *model.ColumnInfo
	var maxCardinality int
	for _, indexInfo := range indices {
		// just use the first column in the index, otherwise can't hit the index when select
		if indexInfo.SeqInIndex != 1 {
			continue
		}

		if indexInfo.Cardinality > maxCardinality {
			column := FindColumnByName(tableInfo.Columns, indexInfo.ColumnName)
			if column == nil {
				return nil, errors.NotFoundf("column %s in %s.%s", indexInfo.ColumnName, schemaName, tableInfo.Name.O)
			}
			maxCardinality = indexInfo.Cardinality
			c = column
		}
	}

	return c, nil
}

// FindAllIndex returns all index, order is pk, uk, and normal index.
func FindAllIndex(tableInfo *model.TableInfo) []*model.IndexInfo {
	indices := make([]*model.IndexInfo, len(tableInfo.Indices))
	copy(indices, tableInfo.Indices)
	sort.SliceStable(indices, func(i, j int) bool {
		a := indices[i]
		b := indices[j]
		switch {
		case b.Primary:
			return false
		case a.Primary:
			return true
		case b.Unique:
			return false
		case a.Unique:
			return true
		default:
			return false
		}
	})
	return indices
}

// FindAllColumnWithIndex returns columns with index, order is pk, uk and normal index.
func FindAllColumnWithIndex(tableInfo *model.TableInfo) []*model.ColumnInfo {
	colsMap := make(map[string]interface{})
	cols := make([]*model.ColumnInfo, 0, 2)

	for _, index := range FindAllIndex(tableInfo) {
		// index will be guaranteed to be visited in order PK -> UK -> IK
		for _, indexCol := range index.Columns {
			col := FindColumnByName(tableInfo.Columns, indexCol.Name.O)
			if _, ok := colsMap[col.Name.O]; ok {
				continue
			}
			colsMap[col.Name.O] = struct{}{}
			cols = append(cols, col)
		}
	}

	return cols
}

// SelectUniqueOrderKey returns some columns for order by condition.
func SelectUniqueOrderKey(tbInfo *model.TableInfo) ([]string, []*model.ColumnInfo) {
	keys := make([]string, 0, 2)
	keyCols := make([]*model.ColumnInfo, 0, 2)

	for _, index := range tbInfo.Indices {
		if index.Primary {
			keys = keys[:0]
			keyCols = keyCols[:0]
			for _, indexCol := range index.Columns {
				keys = append(keys, indexCol.Name.O)
				keyCols = append(keyCols, tbInfo.Columns[indexCol.Offset])
			}
			break
		}
		if index.Unique {
			keys = keys[:0]
			keyCols = keyCols[:0]
			for _, indexCol := range index.Columns {
				keys = append(keys, indexCol.Name.O)
				keyCols = append(keyCols, tbInfo.Columns[indexCol.Offset])
			}
		}
	}

	if len(keys) != 0 {
		return keys, keyCols
	}

	// no primary key or unique found, use all fields as order by key
	for _, col := range tbInfo.Columns {
		keys = append(keys, col.Name.O)
		keyCols = append(keyCols, col)
	}

	return keys, keyCols
}
