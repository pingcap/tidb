// Copyright 2016 PingCAP, Inc.
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

package executor

import (
	"bytes"
	"compress/gzip"
	"compress/zlib"
	"context"
	"encoding/base64"
	"fmt"
	"github.com/golang/snappy"
	"io"

	"github.com/cznic/mathutil"
	"github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/util/chunk"
)

// ExplainExec represents an explain executor.
type ExplainExec struct {
	baseExecutor

	explain     *core.Explain
	analyzeExec Executor
	rows        [][]string
	cursor      int
}

// Open implements the Executor Open interface.
func (e *ExplainExec) Open(ctx context.Context) error {
	if e.analyzeExec != nil {
		return e.analyzeExec.Open(ctx)
	}
	return nil
}

// Close implements the Executor Close interface.
func (e *ExplainExec) Close() error {
	e.rows = nil
	return nil
}

// Next implements the Executor Next interface.
func (e *ExplainExec) Next(ctx context.Context, req *chunk.Chunk) error {
	if e.rows == nil {
		var err error
		e.rows, err = e.generateExplainInfo(ctx)
		if err != nil {
			return err
		}
	}

	req.GrowAndReset(e.maxChunkSize)
	if e.cursor >= len(e.rows) {
		return nil
	}

	numCurRows := mathutil.Min(req.Capacity(), len(e.rows)-e.cursor)
	for i := e.cursor; i < e.cursor+numCurRows; i++ {
		for j := range e.rows[i] {
			req.AppendString(j, e.rows[i][j])
		}
	}
	e.cursor += numCurRows
	return nil
}

func (e *ExplainExec) generateExplainInfo(ctx context.Context) ([][]string, error) {
	if e.analyzeExec != nil {
		chk := newFirstChunk(e.analyzeExec)
		for {
			err := Next(ctx, e.analyzeExec, chk)
			if err != nil {
				return nil, err
			}
			if chk.NumRows() == 0 {
				break
			}
		}
		if err := e.analyzeExec.Close(); err != nil {
			return nil, err
		}
	}
	if err := e.explain.RenderResult(); err != nil {
		return nil, err
	}
	if e.analyzeExec != nil {
		e.ctx.GetSessionVars().StmtCtx.RuntimeStatsColl = nil
	}
	genExplainRows(e.explain.Rows)
	genPlanNormalizeString(e.explain.StmtPlan)
	return e.explain.Rows, nil
}

func genPlanNormalizeString(p core.Plan) {
	pn := core.PlanNormalizer{}
	str := pn.NormalizePlanTreeString(p.(core.PhysicalPlan))
	fmt.Printf("\n-----------------------\n%v\n----------------------------\n", str)

	decodePlan, err := core.DecodeNormalizePlanTreeString(str)
	fmt.Printf("decode plan \n-----------------------\n%v\n-----------err: %v -----------------\n", decodePlan, err)
}

func genExplainRows(rows [][]string) {
	var buf bytes.Buffer
	for _, row := range rows {
		for _, value := range row {
			buf.WriteString(value)
			buf.WriteString("\t")
		}
		buf.WriteString("\n")
	}
	fmt.Printf("gen explain: %v\n%v\n", buf.Len(), buf.String())
	compressZlib(buf)

	//compressGzip(buf)
	//compressSnappy(buf)
}

func compressZlib(buf bytes.Buffer) {
	var in bytes.Buffer
	w, _ := zlib.NewWriterLevel(&in, zlib.BestCompression)
	w.Write(buf.Bytes())
	w.Close()
	encodedStr := base64.StdEncoding.EncodeToString(in.Bytes())
	fmt.Printf("compress len: %v\n%v\nencode len: %v\n%v\n", in.Len(), in.String(), len(encodedStr), encodedStr)

	decodeBytes, err := base64.StdEncoding.DecodeString(encodedStr)
	if err != nil {
		fmt.Printf("decode err: %v\n\n")
	}
	reader := bytes.NewReader(decodeBytes)
	out, err := zlib.NewReader(reader)
	if err != nil {
		fmt.Printf("decode err: %v\n\n")
	}
	var outbuf bytes.Buffer
	io.Copy(&outbuf, out)
	fmt.Printf("decode plan: \n%v\n\n", outbuf.String())
}

func compressGzip(buf bytes.Buffer) {
	var in bytes.Buffer
	w, _ := gzip.NewWriterLevel(&in, gzip.BestCompression)
	w.Write(buf.Bytes())
	w.Close()
	fmt.Printf("compress len: %v\n", in.Len())
}

func compressSnappy(buf bytes.Buffer) {
	re := make([]byte, 0, buf.Len())
	re = snappy.Encode(re, buf.Bytes())
	fmt.Printf("compress len: %v\n", len(re))
}
