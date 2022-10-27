// Copyright 2022 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ttl

import (
	"fmt"
	"strings"

	"github.com/pingcap/errors"

	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/format"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/types"
)

type ttlTable struct {
	*model.TableInfo
	Schema model.CIStr
}

func newTTLTable(schema model.CIStr, tblInfo *model.TableInfo) (ttlTable, error) {
	if !isTTLTable(tblInfo) {
		return ttlTable{}, errors.New("table is not ttl table")
	}

	return ttlTable{
		Schema:    schema,
		TableInfo: tblInfo,
	}, nil
}

func (t *ttlTable) FormatQuerySQL(rangeStart, rangeEnd []types.Datum, expire types.Datum, limit int) (string, error) {
	handleColumns := t.GetHandleColumns()
	handleColumnNameText := formatColumnName(handleColumns...)
	operandHandleColumnNameText := formatOperandColumnName(handleColumns...)

	conditions := make([]string, 0, 2)
	if len(rangeStart) > 0 {
		text, err := formatDatums(rangeStart, handleColumns)
		if err != nil {
			return "", err
		}
		conditions = append(conditions, fmt.Sprintf("%s > %s", operandHandleColumnNameText, text))
	}

	if len(rangeEnd) > 0 {
		text, err := formatDatums(rangeEnd, handleColumns)
		if err != nil {
			return "", err
		}
		conditions = append(conditions, fmt.Sprintf("%s < %s", operandHandleColumnNameText, text))
	}

	ttlColumn := t.GetTTLColumn()
	expireText, err := formatDatum(expire, ttlColumn)
	if err != nil {
		return "", err
	}
	conditions = append(conditions, fmt.Sprintf("%s < %s", formatOperandColumnName(ttlColumn), expireText))

	querySQL := fmt.Sprintf(
		"SELECT LOW_PRIORITY %s FROM `%s`.`%s` WHERE %s ORDER BY %s ASC",
		handleColumnNameText,
		t.Schema.O,
		t.Name.O,
		strings.Join(conditions, " AND "),
		handleColumnNameText,
	)

	if limit > 0 {
		querySQL = querySQL + fmt.Sprintf(" LIMIT %d", limit)
	}
	return querySQL, nil
}

func (t *ttlTable) FormatDeleteSQL(keys [][]types.Datum, expire types.Datum) (string, error) {
	handleColumns := t.GetHandleColumns()
	keyTexts := make([]string, 0, len(keys))
	for _, datums := range keys {
		text, err := formatDatums(datums, handleColumns)
		if err != nil {
			return "", err
		}
		keyTexts = append(keyTexts, text)
	}

	ttlColumn := t.GetTTLColumn()
	ttlText, err := formatDatum(expire, ttlColumn)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf(
		"DELETE LOW_PRIORITY FROM `%s`.`%s` WHERE %s IN (%s) AND %s < %s",
		t.Schema.O,
		t.Name.O,
		formatOperandColumnName(handleColumns...),
		strings.Join(keyTexts, ", "),
		formatOperandColumnName(ttlColumn),
		ttlText,
	), nil
}

func (t *ttlTable) GetTTLColumn() *model.ColumnInfo {
	for _, column := range t.Columns {
		if column.Name.L == "expire" {
			return column
		}
	}
	return nil
}

func (t *ttlTable) GetHandleColumns() []*model.ColumnInfo {
	if t.PKIsHandle {
		for i, col := range t.Columns {
			if mysql.HasPriKeyFlag(col.GetFlag()) {
				return []*model.ColumnInfo{t.Columns[i]}
			}
		}
	}

	if t.IsCommonHandle {
		idxInfo := tables.FindPrimaryIndex(t.TableInfo)
		columns := make([]*model.ColumnInfo, len(idxInfo.Columns))
		for i, idxCol := range idxInfo.Columns {
			columns[i] = t.Columns[idxCol.Offset]
		}
		return columns
	}

	return []*model.ColumnInfo{model.NewExtraHandleColInfo()}
}

func formatDatum(datum types.Datum, column *model.ColumnInfo) (string, error) {
	expr := ast.NewValueExpr(datum.GetValue(), column.FieldType.GetCharset(), column.FieldType.GetCollate())

	var res strings.Builder
	ctx := format.NewRestoreCtx(format.DefaultRestoreFlags, &res)
	if err := expr.Restore(ctx); err != nil {
		return "", err
	}
	return res.String(), nil
}

func formatDatums(datums []types.Datum, columns []*model.ColumnInfo) (string, error) {
	datumTexts := make([]string, 0, len(datums))
	for i, datum := range datums {
		text, err := formatDatum(datum, columns[i])
		if err != nil {
			return "", err
		}
		datumTexts = append(datumTexts, text)
	}
	text := strings.Join(datumTexts, ", ")
	if len(datums) > 1 {
		text = fmt.Sprintf("(%s)", text)
	}
	return text, nil
}

func formatColumnName(columns ...*model.ColumnInfo) string {
	names := make([]string, 0, len(columns))
	for _, col := range columns {
		names = append(names, fmt.Sprintf("`%s`", col.Name.O))
	}
	return strings.Join(names, ", ")
}

func formatOperandColumnName(columns ...*model.ColumnInfo) string {
	text := formatColumnName(columns...)
	if len(columns) > 1 {
		text = fmt.Sprintf("(%s)", text)
	}
	return text
}

func isTTLTable(tbl *model.TableInfo) bool {
	for _, column := range tbl.Columns {
		if column.Name.L == "expire" {
			switch column.GetType() {
			case mysql.TypeDatetime, mysql.TypeTimestamp:
				return true
			}
		}
	}
	return false
}
