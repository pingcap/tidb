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

package executor

import (
	"context"
	"errors"
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/coprocessor"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/tikvpb"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/metadef"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/stretchr/testify/require"
)

type coprocessorTestStream struct {
	tikvpb.Tikv_CoprocessorStreamServer
	responses []*coprocessor.Response
	sendErr   error
}

func (s *coprocessorTestStream) Send(resp *coprocessor.Response) error {
	s.responses = append(s.responses, resp)
	return s.sendErr
}

func TestCoprocessorExecutorClose(t *testing.T) {
	ctx := context.Background()
	builder := infoschema.NewBuilder(nil, 0, nil, infoschema.NewData(), false)
	require.NoError(t, builder.InitWithDBInfos(nil, nil, nil, nil, 0))
	is := builder.Build(0)
	tbl, err := is.TableByName(ctx, metadef.InformationSchemaName, ast.NewCIStr(infoschema.ClusterTableSlowLog))
	require.NoError(t, err)
	openErr := errors.New("coprocessor open failed")
	nextErr := errors.New("coprocessor next failed")
	closeErr := errors.New("coprocessor close failed")
	sendErr := errors.New("coprocessor send failed")

	for _, streaming := range []bool{false, true} {
		mode := "unary"
		if streaming {
			mode = "streaming"
		}
		t.Run(mode, func(t *testing.T) {
			for _, tt := range []struct {
				name       string
				rows       []int64
				openErr    error
				nextErr    error
				closeErr   error
				sendErr    error
				encodeType tipb.EncodeType
				wantError  string
				buildError bool
			}{
				{name: "empty"},
				{name: "rows", rows: []int64{1, 2}},
				{name: "chunk encoding", rows: []int64{1, 2}, encodeType: tipb.EncodeType_TypeChunk},
				{name: "open error", openErr: openErr, closeErr: closeErr, wantError: openErr.Error()},
				{name: "next error", nextErr: nextErr, closeErr: closeErr, wantError: nextErr.Error()},
				{name: "canceled", nextErr: context.Canceled, closeErr: closeErr, wantError: context.Canceled.Error()},
				{name: "encoding error", rows: []int64{1}, encodeType: tipb.EncodeType(100), closeErr: closeErr, wantError: "unknown DAG encode type"},
				{name: "close error", closeErr: closeErr},
				{name: "send error", rows: []int64{1}, sendErr: sendErr, closeErr: closeErr},
				{name: "error response send error", nextErr: nextErr, sendErr: sendErr, closeErr: closeErr, wantError: nextErr.Error()},
				{name: "build error", buildError: true, wantError: "unsupported request type"},
			} {
				if !streaming && tt.sendErr != nil {
					continue
				}
				t.Run(tt.name, func(t *testing.T) {
					sctx := mock.NewContext()
					sctx.SetInfoSchema(is)
					e := newFoundRowsTestExecutor(sctx, tt.rows...)
					e.openErr, e.nextErr, e.closeErr = tt.openErr, tt.nextErr, tt.closeErr
					const fp = "github.com/pingcap/tidb/pkg/executor/mockCoprocessorDAGExecutor"
					require.NoError(t, failpoint.EnableCall(fp, func(executor *exec.Executor) {
						*executor = e
					}))
					t.Cleanup(func() { require.NoError(t, failpoint.Disable(fp)) })
					dag := &tipb.DAGRequest{
						Executors: []*tipb.Executor{{
							Tp: tipb.ExecType_TypeTableScan,
							TblScan: &tipb.TableScan{
								TableId: tbl.Meta().ID,
								Columns: util.ColumnsToProto(tbl.Meta().Columns, tbl.Meta().PKIsHandle, false, false),
							},
						}},
						OutputOffsets: []uint32{0},
						EncodeType:    tt.encodeType,
					}
					data, err := dag.Marshal()
					require.NoError(t, err)
					req := &coprocessor.Request{Tp: kv.ReqTypeDAG, Data: data, Context: &kvrpcpb.Context{}}
					if tt.buildError {
						req.Tp = -1
					}
					h := NewCoprocessorDAGHandler(sctx)
					var responses []*coprocessor.Response
					if streaming {
						stream := &coprocessorTestStream{sendErr: tt.sendErr}
						err := h.HandleStreamRequest(ctx, req, stream)
						if tt.sendErr != nil {
							require.ErrorIs(t, err, tt.sendErr)
						} else {
							require.NoError(t, err)
						}
						responses = stream.responses
					} else {
						responses = []*coprocessor.Response{h.HandleRequest(ctx, req)}
					}
					if tt.buildError {
						require.Zero(t, e.opens)
						require.Zero(t, e.closes)
					} else {
						require.Equal(t, 1, e.opens)
						require.Equal(t, 1, e.closes, "every opened executor must be closed exactly once")
					}
					wantError := tt.wantError
					if !streaming && tt.name == "close error" {
						wantError = closeErr.Error()
					}
					if wantError != "" {
						require.NotEmpty(t, responses)
						require.Contains(t, responses[0].OtherError, wantError)
						if wantError != closeErr.Error() {
							require.NotContains(t, responses[0].OtherError, closeErr.Error())
						}
					} else if tt.sendErr == nil {
						for _, resp := range responses {
							require.Empty(t, resp.OtherError)
						}
					}
				})
			}
		})
	}
}
