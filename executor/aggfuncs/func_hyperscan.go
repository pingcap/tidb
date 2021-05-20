// Copyright 2021 PingCAP, Inc.
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

// +build hyperscan

package aggfuncs

import (
	"encoding/base64"
	"strconv"
	"unsafe"

	hs "github.com/blacktear23/gohs/hyperscan"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/expression/aggregation"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/dbterror"
	"github.com/pingcap/tidb/util/stringutil"
)

var (
	errIncorrectParameterCount = dbterror.ClassExpression.NewStd(mysql.ErrWrongParamcountToNativeFct)

	// All the AggFunc implementations for "HS_BUILDDB" are listed here
	_ AggFunc = (*hsBuildDbAgg)(nil)
)

const (
	// DefPartialResult4HsBuildDbAgg is the size of partialResult4HsBuildDbAgg
	DefPartialResult4HsBuildDbAgg = int64(unsafe.Sizeof(partialResult4HsBuildDbAgg{}))
	// DefHsPatternStructSize is the size of hs.Pattern
	DefHsPatternStructSize = int64(unsafe.Sizeof(hs.Pattern{}))
)

func init() {
	extensionAggFuncBuilders[ast.AggFuncHSBuildDB] = buildHsBuildDb
}

func buildHsBuildDb(ctx sessionctx.Context, aggFuncDesc *aggregation.AggFuncDesc, ordinal int) AggFunc {
	base := baseAggFunc{
		args:    aggFuncDesc.Args,
		ordinal: ordinal,
	}
	switch aggFuncDesc.Mode {
	case aggregation.DedupMode:
		return nil
	default:
		return &hsBuildDbAgg{base}
	}
}

type hsBuildDbAgg struct {
	baseAggFunc
}

type partialResult4HsBuildDbAgg struct {
	patterns []*hs.Pattern
}

func (e *hsBuildDbAgg) AllocPartialResult() (pr PartialResult, memDelta int64) {
	p := partialResult4HsBuildDbAgg{}
	p.patterns = make([]*hs.Pattern, 0)
	return PartialResult(&p), DefPartialResult4HsBuildDbAgg
}

func (e *hsBuildDbAgg) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4HsBuildDbAgg)(pr)
	p.patterns = make([]*hs.Pattern, 0)
}

func (e *hsBuildDbAgg) AppendFinalResult2Chunk(sctx sessionctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4HsBuildDbAgg)(pr)
	if len(p.patterns) == 0 {
		chk.AppendNull(e.ordinal)
		return nil
	}
	builder := hs.DatabaseBuilder{
		Patterns: p.patterns,
		Mode:     hs.BlockMode,
		Platform: hs.PopulatePlatform(),
	}
	db, err := builder.Build()
	if err != nil {
		return errors.Trace(err)
	}
	defer db.Close()
	data, err := db.Marshal()
	if err != nil {
		return errors.Trace(err)
	}
	b64data := base64.StdEncoding.EncodeToString(data)
	chk.AppendString(e.ordinal, b64data)
	return nil
}

func (e *hsBuildDbAgg) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4HsBuildDbAgg)(pr)
	for _, row := range rowsInGroup {
		var (
			pid        = 0
			patternStr string
			err        error
		)
		if len(e.args) == 1 {
			pattern, err := e.args[0].Eval(row)
			if err != nil {
				return 0, errors.Trace(err)
			}
			patternStr, err = pattern.ToString()
			if err != nil {
				return 0, errors.Trace(err)
			}

		} else if len(e.args) == 2 {
			id, err := e.args[0].Eval(row)
			if err != nil {
				return 0, errors.Trace(err)
			}

			if !id.IsNull() {
				if idStr, err := id.ToString(); err == nil {
					npid, err := strconv.ParseInt(idStr, 10, 64)
					if err == nil {
						pid = int(npid)
					}
				}
			}

			pattern, err := e.args[1].Eval(row)
			if err != nil {
				return 0, errors.Trace(err)
			}

			patternStr, err = pattern.ToString()
			if err != nil {
				return 0, errors.Trace(err)
			}
		} else {
			return 0, errIncorrectParameterCount.GenWithStackByArgs(ast.AggFuncHSBuildDB)
		}
		patternStr = stringutil.Copy(patternStr)
		pat, err := hs.ParsePattern(patternStr)
		if err != nil {
			return 0, errors.Trace(err)
		}
		pat.Id = pid
		p.patterns = append(p.patterns, pat)
		memDelta += (int64(len(patternStr)) + DefHsPatternStructSize)
	}
	return memDelta, nil
}

func (e *hsBuildDbAgg) MergePartialResult(sctx sessionctx.Context, src, dst PartialResult) (memDelta int64, err error) {
	p1, p2 := (*partialResult4HsBuildDbAgg)(src), (*partialResult4HsBuildDbAgg)(dst)
	for _, pat := range p1.patterns {
		p2.patterns = append(p2.patterns, pat)
	}
	return 0, nil
}
