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

package sqlbuilder

import (
	"encoding/hex"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/format"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/ttl/cache"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/sqlexec"
	"github.com/pkg/errors"
)

func writeHex(in io.Writer, d types.Datum) error {
	_, err := fmt.Fprintf(in, "x'%s'", hex.EncodeToString(d.GetBytes()))
	return err
}

func writeDatum(restoreCtx *format.RestoreCtx, d types.Datum, ft *types.FieldType) error {
	switch ft.GetType() {
	case mysql.TypeBit, mysql.TypeBlob, mysql.TypeLongBlob, mysql.TypeTinyBlob:
		return writeHex(restoreCtx.In, d)
	case mysql.TypeString, mysql.TypeVarString, mysql.TypeVarchar, mysql.TypeEnum, mysql.TypeSet:
		if mysql.HasBinaryFlag(ft.GetFlag()) {
			return writeHex(restoreCtx.In, d)
		}
		_, err := fmt.Fprintf(restoreCtx.In, "'%s'", sqlexec.EscapeString(d.GetString()))
		return err
	}
	expr := ast.NewValueExpr(d.GetValue(), ft.GetCharset(), ft.GetCollate())
	return expr.Restore(restoreCtx)
}

// FormatSQLDatum formats the datum to a value string in sql
func FormatSQLDatum(d types.Datum, ft *types.FieldType) (string, error) {
	var sb strings.Builder
	ctx := format.NewRestoreCtx(format.DefaultRestoreFlags, &sb)
	if err := writeDatum(ctx, d, ft); err != nil {
		return "", err
	}
	return sb.String(), nil
}

type sqlBuilderState int

const (
	writeBegin sqlBuilderState = iota
	writeSelOrDel
	writeWhere
	writeOrderBy
	writeLimit
	writeDone
)

// SQLBuilder is used to build SQLs for TTL
type SQLBuilder struct {
	tbl        *cache.PhysicalTable
	sb         strings.Builder
	restoreCtx *format.RestoreCtx
	state      sqlBuilderState

	isReadOnly         bool
	hasWriteExpireCond bool
}

// NewSQLBuilder creates a new TTLSQLBuilder
func NewSQLBuilder(tbl *cache.PhysicalTable) *SQLBuilder {
	b := &SQLBuilder{tbl: tbl, state: writeBegin}
	b.restoreCtx = format.NewRestoreCtx(format.DefaultRestoreFlags, &b.sb)
	return b
}

// Build builds the final sql
func (b *SQLBuilder) Build() (string, error) {
	if b.state == writeBegin {
		return "", errors.Errorf("invalid state: %v", b.state)
	}

	if !b.isReadOnly && !b.hasWriteExpireCond {
		// check whether the `timeRow < expire_time` condition has been written to make sure this SQL is safe.
		return "", errors.New("expire condition not write")
	}

	if b.state != writeDone {
		b.state = writeDone
	}

	return b.sb.String(), nil
}

// WriteSelect writes a select statement to select key columns without any condition
func (b *SQLBuilder) WriteSelect() error {
	if b.state != writeBegin {
		return errors.Errorf("invalid state: %v", b.state)
	}
	b.restoreCtx.WritePlain("SELECT LOW_PRIORITY ")
	b.writeColNames(b.tbl.KeyColumns, false)
	b.restoreCtx.WritePlain(" FROM ")
	if err := b.writeTblName(); err != nil {
		return err
	}
	if par := b.tbl.PartitionDef; par != nil {
		b.restoreCtx.WritePlain(" PARTITION(")
		b.restoreCtx.WriteName(par.Name.O)
		b.restoreCtx.WritePlain(")")
	}
	b.state = writeSelOrDel
	b.isReadOnly = true
	return nil
}

// WriteDelete writes a delete statement without any condition
func (b *SQLBuilder) WriteDelete() error {
	if b.state != writeBegin {
		return errors.Errorf("invalid state: %v", b.state)
	}
	b.restoreCtx.WritePlain("DELETE LOW_PRIORITY FROM ")
	if err := b.writeTblName(); err != nil {
		return err
	}
	if par := b.tbl.PartitionDef; par != nil {
		b.restoreCtx.WritePlain(" PARTITION(")
		b.restoreCtx.WriteName(par.Name.O)
		b.restoreCtx.WritePlain(")")
	}
	b.state = writeSelOrDel
	return nil
}

// WriteCommonCondition writes a new condition
func (b *SQLBuilder) WriteCommonCondition(cols []*model.ColumnInfo, op string, dp []types.Datum) error {
	switch b.state {
	case writeSelOrDel:
		b.restoreCtx.WritePlain(" WHERE ")
		b.state = writeWhere
	case writeWhere:
		b.restoreCtx.WritePlain(" AND ")
	default:
		return errors.Errorf("invalid state: %v", b.state)
	}

	b.writeColNames(cols, len(cols) > 1)
	b.restoreCtx.WritePlain(" ")
	b.restoreCtx.WritePlain(op)
	b.restoreCtx.WritePlain(" ")
	return b.writeDataPoint(cols, dp)
}

// WriteExpireCondition writes a condition with the time column
func (b *SQLBuilder) WriteExpireCondition(expire time.Time) error {
	switch b.state {
	case writeSelOrDel:
		b.restoreCtx.WritePlain(" WHERE ")
		b.state = writeWhere
	case writeWhere:
		b.restoreCtx.WritePlain(" AND ")
	default:
		return errors.Errorf("invalid state: %v", b.state)
	}

	b.writeColNames([]*model.ColumnInfo{b.tbl.TimeColumn}, false)
	b.restoreCtx.WritePlain(" < ")
	b.restoreCtx.WritePlain("FROM_UNIXTIME(")
	b.restoreCtx.WritePlain(strconv.FormatInt(expire.Unix(), 10))
	b.restoreCtx.WritePlain(")")
	b.hasWriteExpireCond = true
	return nil
}

// WriteInCondition writes an IN condition
func (b *SQLBuilder) WriteInCondition(cols []*model.ColumnInfo, dps ...[]types.Datum) error {
	switch b.state {
	case writeSelOrDel:
		b.restoreCtx.WritePlain(" WHERE ")
		b.state = writeWhere
	case writeWhere:
		b.restoreCtx.WritePlain(" AND ")
	default:
		return errors.Errorf("invalid state: %v", b.state)
	}

	b.writeColNames(cols, len(cols) > 1)
	b.restoreCtx.WritePlain(" IN ")
	b.restoreCtx.WritePlain("(")
	first := true
	for _, v := range dps {
		if first {
			first = false
		} else {
			b.restoreCtx.WritePlain(", ")
		}
		if err := b.writeDataPoint(cols, v); err != nil {
			return err
		}
	}
	b.restoreCtx.WritePlain(")")
	return nil
}

// WriteOrderBy writes order by
func (b *SQLBuilder) WriteOrderBy(cols []*model.ColumnInfo, desc bool) error {
	if b.state != writeSelOrDel && b.state != writeWhere {
		return errors.Errorf("invalid state: %v", b.state)
	}
	b.state = writeOrderBy
	b.restoreCtx.WritePlain(" ORDER BY ")
	b.writeColNames(cols, false)
	if desc {
		b.restoreCtx.WritePlain(" DESC")
	} else {
		b.restoreCtx.WritePlain(" ASC")
	}
	return nil
}

// WriteLimit writes the limit
func (b *SQLBuilder) WriteLimit(n int) error {
	if b.state != writeSelOrDel && b.state != writeWhere && b.state != writeOrderBy {
		return errors.Errorf("invalid state: %v", b.state)
	}
	b.state = writeLimit
	b.restoreCtx.WritePlain(" LIMIT ")
	b.restoreCtx.WritePlain(strconv.Itoa(n))
	return nil
}

func (b *SQLBuilder) writeTblName() error {
	tn := ast.TableName{Schema: b.tbl.Schema, Name: b.tbl.Name}
	return tn.Restore(b.restoreCtx)
}

func (b *SQLBuilder) writeColName(col *model.ColumnInfo) {
	b.restoreCtx.WriteName(col.Name.O)
}

func (b *SQLBuilder) writeColNames(cols []*model.ColumnInfo, writeBrackets bool) {
	if writeBrackets {
		b.restoreCtx.WritePlain("(")
	}

	first := true
	for _, col := range cols {
		if first {
			first = false
		} else {
			b.restoreCtx.WritePlain(", ")
		}
		b.writeColName(col)
	}

	if writeBrackets {
		b.restoreCtx.WritePlain(")")
	}
}

func (b *SQLBuilder) writeDataPoint(cols []*model.ColumnInfo, dp []types.Datum) error {
	writeBrackets := len(cols) > 1
	if len(cols) != len(dp) {
		return errors.Errorf("col count not match %d != %d", len(cols), len(dp))
	}

	if writeBrackets {
		b.restoreCtx.WritePlain("(")
	}

	first := true
	for i, d := range dp {
		if first {
			first = false
		} else {
			b.restoreCtx.WritePlain(", ")
		}
		if err := writeDatum(b.restoreCtx, d, &cols[i].FieldType); err != nil {
			return err
		}
	}

	if writeBrackets {
		b.restoreCtx.WritePlain(")")
	}

	return nil
}

// ScanQueryGenerator generates SQLs for scan task
type ScanQueryGenerator struct {
	tbl           *cache.PhysicalTable
	expire        time.Time
	keyRangeStart []types.Datum
	keyRangeEnd   []types.Datum
	stack         [][]types.Datum
	limit         int
	firstBuild    bool
	exhausted     bool
}

// NewScanQueryGenerator creates a new ScanQueryGenerator
func NewScanQueryGenerator(tbl *cache.PhysicalTable, expire time.Time, rangeStart []types.Datum, rangeEnd []types.Datum) (*ScanQueryGenerator, error) {
	if err := tbl.ValidateKeyPrefix(rangeStart); err != nil {
		return nil, err
	}

	if err := tbl.ValidateKeyPrefix(rangeEnd); err != nil {
		return nil, err
	}

	return &ScanQueryGenerator{
		tbl:           tbl,
		expire:        expire,
		keyRangeStart: rangeStart,
		keyRangeEnd:   rangeEnd,
		firstBuild:    true,
	}, nil
}

// NextSQL creates next sql of the scan task
func (g *ScanQueryGenerator) NextSQL(continueFromResult [][]types.Datum, nextLimit int) (string, error) {
	if g.exhausted {
		return "", errors.New("generator is exhausted")
	}

	if nextLimit <= 0 {
		return "", errors.Errorf("invalid limit '%d'", nextLimit)
	}

	defer func() {
		g.firstBuild = false
	}()

	if g.stack == nil {
		g.stack = make([][]types.Datum, 0, len(g.tbl.KeyColumns))
	}

	if len(continueFromResult) >= g.limit {
		var continueFromKey []types.Datum
		if cnt := len(continueFromResult); cnt > 0 {
			continueFromKey = continueFromResult[cnt-1]
		}
		if err := g.setStack(continueFromKey); err != nil {
			return "", err
		}
	} else {
		if l := len(g.stack); l > 0 {
			g.stack = g.stack[:l-1]
		}
		if len(g.stack) == 0 {
			g.exhausted = true
		}
	}
	g.limit = nextLimit
	return g.buildSQL()
}

// IsExhausted returns whether the generator is exhausted
func (g *ScanQueryGenerator) IsExhausted() bool {
	return g.exhausted
}

func (g *ScanQueryGenerator) setStack(key []types.Datum) error {
	if key == nil {
		key = g.keyRangeStart
	}

	if key == nil {
		g.stack = g.stack[:0]
		return nil
	}

	if err := g.tbl.ValidateKeyPrefix(key); err != nil {
		return err
	}

	g.stack = g.stack[:len(key)]
	for i := 0; i < len(key); i++ {
		g.stack[i] = key[0 : i+1]
	}
	return nil
}

func (g *ScanQueryGenerator) buildSQL() (string, error) {
	if g.limit <= 0 {
		return "", errors.Errorf("invalid limit '%d'", g.limit)
	}

	if g.exhausted {
		return "", nil
	}

	b := NewSQLBuilder(g.tbl)
	if err := b.WriteSelect(); err != nil {
		return "", err
	}
	if len(g.stack) > 0 {
		for i, d := range g.stack[len(g.stack)-1] {
			col := []*model.ColumnInfo{g.tbl.KeyColumns[i]}
			val := []types.Datum{d}
			var err error
			if i < len(g.stack)-1 {
				err = b.WriteCommonCondition(col, "=", val)
			} else if g.firstBuild {
				// When `g.firstBuild == true`, that means we are querying rows after range start, because range is defined
				// as [start, end), we should use ">=" to find the rows including start key.
				err = b.WriteCommonCondition(col, ">=", val)
			} else {
				// Otherwise when `g.firstBuild != true`, that means we are continuing with the previous result, we should use
				// ">" to exclude the previous row.
				err = b.WriteCommonCondition(col, ">", val)
			}
			if err != nil {
				return "", err
			}
		}
	}

	if len(g.keyRangeEnd) > 0 {
		if err := b.WriteCommonCondition(g.tbl.KeyColumns[0:len(g.keyRangeEnd)], "<", g.keyRangeEnd); err != nil {
			return "", err
		}
	}

	if err := b.WriteExpireCondition(g.expire); err != nil {
		return "", err
	}

	if err := b.WriteOrderBy(g.tbl.KeyColumns, false); err != nil {
		return "", err
	}

	if err := b.WriteLimit(g.limit); err != nil {
		return "", err
	}

	return b.Build()
}

// BuildDeleteSQL builds a delete SQL
func BuildDeleteSQL(tbl *cache.PhysicalTable, rows [][]types.Datum, expire time.Time) (string, error) {
	if len(rows) == 0 {
		return "", errors.New("Cannot build delete SQL with empty rows")
	}

	b := NewSQLBuilder(tbl)
	if err := b.WriteDelete(); err != nil {
		return "", err
	}

	if err := b.WriteInCondition(tbl.KeyColumns, rows...); err != nil {
		return "", err
	}

	if err := b.WriteExpireCondition(expire); err != nil {
		return "", err
	}

	if err := b.WriteLimit(len(rows)); err != nil {
		return "", err
	}

	return b.Build()
}
