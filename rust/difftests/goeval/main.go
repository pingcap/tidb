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
// See the License for the specific language governing permissions and
// limitations under the License.

// Command goeval evaluates constant SQL scalar expressions with TiDB's
// production expression engine and prints each result, so the Rust tidb-expr
// constant-folding evaluator can be checked for semantic parity (the design's
// "result ring").
//
// It reads one expression per line from stdin and writes, per line:
//
//	INT:<v>    a signed integer result
//	UINT:<v>   an unsigned integer result
//	STR:<v>    a valid UTF-8 string result, preserving its exact bytes including
//	           embedded NUL — also used for a date/datetime result (MysqlTime's
//	           canonical string form), since tidb-expr has no dedicated date
//	           value domain and represents dates as plain strings throughout.
//	STR_HEX:<uppercase hex>
//	           an invalid-UTF-8 string/bytes result, encoded reversibly rather
//	           than writing malformed text into the checked golden corpus.
//	DEC:<v>    a decimal result (MyDecimal's canonical string form)
//	FLOAT:<v>  a FLOAT/DOUBLE result, rendered via strconv.FormatFloat(v, 'f',
//	           -1, 64) — the same algorithm Datum.ToString() uses, and one
//	           that (confirmed by direct comparison, not assumed) produces
//	           byte-identical output to Rust's own f64 Display across every
//	           value tried, including subnormals and float64::MAX
//	NULL       a NULL result
//	ERR        the expression failed to parse or evaluate
//	SKIP:<k>   a result kind outside the current scope (.../...)
//
// A line may be prefixed by one of the following context labels and a tab:
//
//	STRICT
//	ALLOW_ZERO_DATE
//	IGNORE_ZERO_IN_DATE
//
// Unprefixed lines retain the historical default context. The prefixed form
// exists for source tests whose observable result is owned by StatementContext
// rather than by the builtin function alone.
package main

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	// Registers expression.BuildSimpleExpr / EvalSimpleAst (core_init.go init).
	_ "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/types"
	_ "github.com/pingcap/tidb/pkg/types/parser_driver"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/mock"
)

func evalOne(ctx *mock.Context, s string) string {
	// *mock.Context implements both expression.BuildContext and EvalContext.
	expr, err := expression.ParseSimpleExpr(ctx, s)
	if err != nil {
		return "ERR"
	}
	d, err := expr.Eval(ctx, chunk.Row{})
	if err != nil {
		return "ERR"
	}
	return labelDatum(d)
}

func evalSourceFunction(ctx *mock.Context, name, arg string) string {
	expr, err := expression.NewFunctionBase(
		ctx,
		strings.ToLower(name),
		types.NewFieldType(mysql.TypeUnspecified),
		expression.NewStrConst(arg),
	)
	if err != nil {
		return "ERR"
	}
	d, err := expr.Eval(ctx, chunk.Row{})
	if err != nil {
		return "ERR"
	}
	return labelDatum(d)
}

func labelDatum(d types.Datum) string {
	if d.IsNull() {
		return "NULL"
	}
	switch d.Kind() {
	case types.KindInt64:
		return fmt.Sprintf("INT:%d", d.GetInt64())
	case types.KindUint64:
		return fmt.Sprintf("UINT:%d", d.GetUint64())
	case types.KindString, types.KindBytes:
		return labelString(d.GetString())
	case types.KindMysqlDecimal:
		return "DEC:" + d.GetMysqlDecimal().String()
	case types.KindMysqlTime:
		return "STR:" + d.GetMysqlTime().String()
	case types.KindFloat64:
		return "FLOAT:" + strconv.FormatFloat(d.GetFloat64(), 'f', -1, 64)
	case types.KindFloat32:
		return "FLOAT:" + strconv.FormatFloat(float64(d.GetFloat32()), 'f', -1, 32)
	default:
		return fmt.Sprintf("SKIP:%d", d.Kind())
	}
}

// labelString is the Go side of tidb_datatype::Datum::label's shared byte
// contract. strings.ToValidUTF8 with an empty replacement changes a string if
// and only if it contains malformed UTF-8; valid control bytes such as NUL are
// left untouched. Invalid payloads use fmt's uppercase hexadecimal encoding.
func labelString(value string) string {
	if strings.ToValidUTF8(value, "") == value {
		return "STR:" + value
	}
	return fmt.Sprintf("STR_HEX:%X", []byte(value))
}

// newContext mirrors pkg/expression's test `createContext`: a mock session
// with the default (strict) SQL mode, a fixed time zone, truncate-as-warning
// type flags, and max_allowed_packet — the setup the expression builder needs.
func newContext() *mock.Context {
	ctx := mock.NewContext()
	sqlMode, _ := mysql.GetSQLMode(mysql.DefaultSQLMode)
	ctx.GetSessionVars().SQLMode = sqlMode
	ctx.ResetSessionAndStmtTimeZone(time.FixedZone("UTC+11", 11*3600))
	sc := ctx.GetSessionVars().StmtCtx
	sc.SetTypeFlags(sc.TypeFlags().WithTruncateAsWarning(true))
	_ = ctx.GetSessionVars().SetSystemVar(
		"max_allowed_packet", strconv.FormatUint(config.GetMaxAllowedPacket(), 10))
	ctx.GetSessionVars().PlanColumnID.Store(0)
	return ctx
}

func main() {
	ctx := newContext()
	in := bufio.NewScanner(os.Stdin)
	in.Buffer(make([]byte, 1024*1024), 8*1024*1024)
	out := bufio.NewWriter(os.Stdout)
	defer out.Flush()
	for in.Scan() {
		line := in.Text()
		if strings.TrimSpace(line) == "" || strings.HasPrefix(line, "##") {
			continue
		}
		evalCtx, fields := contextForLine(ctx, line)
		if evalCtx == nil {
			fmt.Fprintln(out, "ERR")
			continue
		}
		if len(fields) == 2 {
			fmt.Fprintln(out, evalSourceFunction(evalCtx, fields[0], fields[1]))
		} else {
			fmt.Fprintln(out, evalOne(evalCtx, fields[0]))
		}
	}
}

func contextForLine(defaultCtx *mock.Context, line string) (*mock.Context, []string) {
	fields := strings.Split(line, "\t")
	if len(fields) == 1 {
		return defaultCtx, fields
	}
	label := fields[0]
	ctx := newContext()
	switch label {
	case "STRICT":
	case "ALLOW_ZERO_DATE":
		ctx.GetSessionVars().SQLMode = mysql.DelSQLMode(ctx.GetSessionVars().SQLMode, mysql.ModeNoZeroDate)
	case "IGNORE_ZERO_IN_DATE":
		sc := ctx.GetSessionVars().StmtCtx
		sc.SetTypeFlags(sc.TypeFlags().WithIgnoreZeroInDate(true))
	default:
		return nil, fields[1:]
	}
	return ctx, fields[1:]
}
