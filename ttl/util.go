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
	"context"
	"encoding/hex"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/format"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/sqlexec"
)

type sqlBuilderState int

const (
	writeBegin sqlBuilderState = iota
	writeSelOrDel
	writeWhere
	writeOrderBy
	writeLimit
	writeDone
)

type ttlSQLBuilder struct {
	tbl   *ttlTable
	sb    strings.Builder
	state sqlBuilderState
}

func newSQLBuilder(tbl *ttlTable) *ttlSQLBuilder {
	return &ttlSQLBuilder{tbl: tbl, state: writeBegin}
}

func (b *ttlSQLBuilder) Build() (string, error) {
	if b.state == writeBegin {
		return "", errors.Errorf("invalid state: %v", b.state)
	}
	b.state = writeDone
	return b.sb.String(), nil
}

func (b *ttlSQLBuilder) WriteSelect() error {
	if b.state != writeBegin {
		return errors.Errorf("invalid state: %v", b.state)
	}
	b.sb.WriteString("SELECT LOW_PRIORITY ")
	b.writeColNames(b.tbl.KeyColumns, false)
	b.sb.WriteString(" FROM ")
	b.writeTblName()
	b.state = writeSelOrDel
	return nil
}

func (b *ttlSQLBuilder) WriteDelete() error {
	if b.state != writeBegin {
		return errors.Errorf("invalid state: %v", b.state)
	}
	b.sb.WriteString("DELETE LOW_PRIORITY FROM ")
	b.writeTblName()
	b.state = writeSelOrDel
	return nil
}

func (b *ttlSQLBuilder) WriteCommonCondition(cols []*model.ColumnInfo, op string, value rowKey) error {
	switch b.state {
	case writeSelOrDel:
		b.sb.WriteString(" WHERE ")
		b.state = writeWhere
	case writeWhere:
		b.sb.WriteString(" AND ")
	default:
		return errors.Errorf("invalid state: %v", b.state)
	}

	b.writeColNames(cols, len(cols) > 1)
	b.sb.WriteRune(' ')
	b.sb.WriteString(op)
	b.sb.WriteRune(' ')
	if err := b.writeValue(cols, value); err != nil {
		return err
	}
	return nil
}

func (b *ttlSQLBuilder) WriteExpireCondition(expire time.Time) error {
	switch b.state {
	case writeSelOrDel:
		b.sb.WriteString(" WHERE ")
		b.state = writeWhere
	case writeWhere:
		b.sb.WriteString(" AND ")
	default:
		return errors.Errorf("invalid state: %v", b.state)
	}

	b.writeColNames([]*model.ColumnInfo{b.tbl.TimeColumn}, false)
	b.sb.WriteString(" < ")
	b.sb.WriteString("'")
	b.sb.WriteString(expire.Format("2006-01-02 15:04:05.999999"))
	b.sb.WriteString("'")
	return nil
}

func (b *ttlSQLBuilder) WriteInCondition(cols []*model.ColumnInfo, values []rowKey) error {
	switch b.state {
	case writeSelOrDel:
		b.sb.WriteString(" WHERE ")
		b.state = writeWhere
	case writeWhere:
		b.sb.WriteString(" AND ")
	default:
		return errors.Errorf("invalid state: %v", b.state)
	}

	b.writeColNames(cols, len(cols) > 1)
	b.sb.WriteString(" IN ")
	b.sb.WriteRune('(')
	first := true
	for _, v := range values {
		if first {
			first = false
		} else {
			b.sb.WriteString(", ")
		}
		if err := b.writeValue(cols, v); err != nil {
			return err
		}
	}
	b.sb.WriteRune(')')
	return nil
}

func (b *ttlSQLBuilder) WriteOrderBy(cols []*model.ColumnInfo, desc bool) error {
	if b.state != writeSelOrDel && b.state != writeWhere {
		return errors.Errorf("invalid state: %v", b.state)
	}
	b.state = writeOrderBy
	b.sb.WriteString(" ORDER BY ")
	b.writeColNames(cols, false)
	if desc {
		b.sb.WriteString(" DESC")
	} else {
		b.sb.WriteString(" ASC")
	}
	return nil
}

func (b *ttlSQLBuilder) WriteLimit(n int) error {
	if b.state != writeSelOrDel && b.state != writeWhere && b.state != writeOrderBy {
		return errors.Errorf("invalid state: %v", b.state)
	}
	b.state = writeLimit
	b.sb.WriteString(" LIMIT ")
	b.sb.WriteString(strconv.Itoa(n))
	return nil
}

func (b *ttlSQLBuilder) writeTblName() {
	b.sb.WriteRune('`')
	b.sb.WriteString(b.tbl.Schema.O)
	b.sb.WriteRune('`')
	b.sb.WriteRune('.')
	b.sb.WriteRune('`')
	b.sb.WriteString(b.tbl.Name.O)
	b.sb.WriteRune('`')
}

func (b *ttlSQLBuilder) writeColName(col *model.ColumnInfo) {
	b.sb.WriteRune('`')
	b.sb.WriteString(col.Name.O)
	b.sb.WriteRune('`')
}

func (b *ttlSQLBuilder) writeColNames(cols []*model.ColumnInfo, writeBrackets bool) {
	if writeBrackets {
		b.sb.WriteRune('(')
	}

	first := true
	for _, col := range cols {
		if first {
			first = false
		} else {
			b.sb.WriteString(", ")
		}
		b.writeColName(col)
	}

	if writeBrackets {
		b.sb.WriteRune(')')
	}
}

func (b *ttlSQLBuilder) writeValue(cols []*model.ColumnInfo, values rowKey) error {
	writeBrackets := len(cols) > 1
	if len(cols) != len(values) {
		return errors.Errorf("col count not match %d != %d", len(cols), len(values))
	}

	if writeBrackets {
		b.sb.WriteRune('(')
	}

	first := true
	for i, d := range values {
		if first {
			first = false
		} else {
			b.sb.WriteString(", ")
		}
		if err := b.writeDatum(d, &cols[i].FieldType); err != nil {
			return err
		}
	}

	if writeBrackets {
		b.sb.WriteRune(')')
	}

	return nil
}

func (b *ttlSQLBuilder) writeDatum(d types.Datum, ft *types.FieldType) error {
	if ft.GetType() == mysql.TypeString && mysql.HasBinaryFlag(ft.GetFlag()) {
		b.sb.WriteString("x'")
		b.sb.WriteString(hex.EncodeToString(d.GetBytes()))
		b.sb.WriteString("'")
		return nil
	}

	ctx := format.NewRestoreCtx(format.DefaultRestoreFlags, &b.sb)
	expr := ast.NewValueExpr(d.GetValue(), ft.GetCharset(), ft.GetCollate())
	if err := expr.Restore(ctx); err != nil {
		return err
	}

	return nil
}

func executeSQL(ctx context.Context, se *session, sql string, args ...interface{}) ([]chunk.Row, error) {
	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnTTL)
	rs, err := se.ExecuteInternal(ctx, sql, args...)
	if err != nil {
		return nil, err
	}

	if rs == nil {
		return nil, nil
	}

	defer func() {
		terror.Log(rs.Close())
	}()

	return sqlexec.DrainRecordSet(ctx, rs, 8)
}
