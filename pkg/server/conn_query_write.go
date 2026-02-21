// Copyright 2015 PingCAP, Inc.
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

package server

import (
	"context"
	"runtime/trace"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/server/internal/column"
	"github.com/pingcap/tidb/pkg/server/internal/dump"
	"github.com/pingcap/tidb/pkg/server/internal/resultset"
	storeerr "github.com/pingcap/tidb/pkg/store/driver/error"
	"github.com/pingcap/tidb/pkg/util/execdetails"
)

func (cc *clientConn) writeColumnInfo(columns []*column.Info) error {
	data := cc.alloc.AllocWithLen(4, 1024)
	data = dump.LengthEncodedInt(data, uint64(len(columns)))
	if err := cc.writePacket(data); err != nil {
		return err
	}
	for _, v := range columns {
		data = data[0:4]
		data = v.Dump(data, cc.rsEncoder)
		if err := cc.writePacket(data); err != nil {
			return err
		}
	}
	return nil
}

// writeChunks writes data from a Chunk, which filled data by a ResultSet, into a connection.
// binary specifies the way to dump data. It throws any error while dumping data.
// serverStatus, a flag bit represents server information
// The first return value indicates whether error occurs at the first call of ResultSet.Next.
func (cc *clientConn) writeChunks(ctx context.Context, rs resultset.ResultSet, binary bool, serverStatus uint16) (bool, error) {
	data := cc.alloc.AllocWithLen(4, 1024)
	req := rs.NewChunk(cc.ctx.GetSessionVars().GetChunkAllocator())
	gotColumnInfo := false
	firstNext := true
	validNextCount := 0
	var start time.Time
	var stmtDetail *execdetails.StmtExecDetails
	stmtDetailRaw := ctx.Value(execdetails.StmtExecDetailKey)
	if stmtDetailRaw != nil {
		//nolint:forcetypeassert
		stmtDetail = stmtDetailRaw.(*execdetails.StmtExecDetails)
	}
	for {
		failpoint.Inject("fetchNextErr", func(value failpoint.Value) {
			//nolint:forcetypeassert
			switch value.(string) {
			case "firstNext":
				failpoint.Return(firstNext, storeerr.ErrTiFlashServerTimeout)
			case "secondNext":
				if !firstNext {
					failpoint.Return(firstNext, storeerr.ErrTiFlashServerTimeout)
				}
			case "secondNextAndRetConflict":
				if !firstNext && validNextCount > 1 {
					failpoint.Return(firstNext, kv.ErrWriteConflict)
				}
			}
		})
		// Here server.tidbResultSet implements Next method.
		err := rs.Next(ctx, req)
		if err != nil {
			return firstNext, err
		}
		if !gotColumnInfo {
			// We need to call Next before we get columns.
			// Otherwise, we will get incorrect columns info.
			columns := rs.Columns()
			if stmtDetail != nil {
				start = time.Now()
			}
			if err = cc.writeColumnInfo(columns); err != nil {
				return false, err
			}
			if cc.capability&mysql.ClientDeprecateEOF == 0 {
				// metadata only needs EOF marker for old clients without ClientDeprecateEOF
				if err = cc.writeEOF(ctx, serverStatus); err != nil {
					return false, err
				}
			}
			if stmtDetail != nil {
				stmtDetail.WriteSQLRespDuration += time.Since(start)
			}
			gotColumnInfo = true
		}
		rowCount := req.NumRows()
		if rowCount == 0 {
			break
		}
		validNextCount++
		firstNext = false
		reg := trace.StartRegion(ctx, "WriteClientConn")
		if stmtDetail != nil {
			start = time.Now()
		}
		for i := range rowCount {
			data = data[0:4]
			if binary {
				data, err = column.DumpBinaryRow(data, rs.Columns(), req.GetRow(i), cc.rsEncoder)
			} else {
				data, err = column.DumpTextRow(data, rs.Columns(), req.GetRow(i), cc.rsEncoder)
			}
			if err != nil {
				reg.End()
				return false, err
			}
			if err = cc.writePacket(data); err != nil {
				reg.End()
				return false, err
			}
		}
		reg.End()
		if stmtDetail != nil {
			stmtDetail.WriteSQLRespDuration += time.Since(start)
		}
	}
	if err := rs.Finish(); err != nil {
		return false, err
	}

	if stmtDetail != nil {
		start = time.Now()
	}

	err := cc.writeEOF(ctx, serverStatus)
	if stmtDetail != nil {
		stmtDetail.WriteSQLRespDuration += time.Since(start)
	}
	return false, err
}

// writeChunksWithFetchSize writes data from a Chunk, which filled data by a ResultSet, into a connection.
// binary specifies the way to dump data. It throws any error while dumping data.
// serverStatus, a flag bit represents server information.
// fetchSize, the desired number of rows to be fetched each time when client uses cursor.
func (cc *clientConn) writeChunksWithFetchSize(ctx context.Context, rs resultset.CursorResultSet, serverStatus uint16, fetchSize int) error {
	var (
		stmtDetail *execdetails.StmtExecDetails
		err        error
		start      time.Time
	)
	data := cc.alloc.AllocWithLen(4, 1024)
	stmtDetailRaw := ctx.Value(execdetails.StmtExecDetailKey)
	if stmtDetailRaw != nil {
		//nolint:forcetypeassert
		stmtDetail = stmtDetailRaw.(*execdetails.StmtExecDetails)
	}
	if stmtDetail != nil {
		start = time.Now()
	}

	iter := rs.GetRowIterator()
	// send the rows to the client according to fetchSize.
	for i := 0; i < fetchSize && iter.Current(ctx) != iter.End(); i++ {
		row := iter.Current(ctx)

		data = data[0:4]
		data, err = column.DumpBinaryRow(data, rs.Columns(), row, cc.rsEncoder)
		if err != nil {
			return err
		}
		if err = cc.writePacket(data); err != nil {
			return err
		}

		iter.Next(ctx)
	}
	if iter.Error() != nil {
		return iter.Error()
	}

	// tell the client COM_STMT_FETCH has finished by setting proper serverStatus,
	// and close ResultSet.
	if iter.Current(ctx) == iter.End() {
		serverStatus &^= mysql.ServerStatusCursorExists
		serverStatus |= mysql.ServerStatusLastRowSend
	}

	// don't include the time consumed by `cl.OnFetchReturned()` in the `WriteSQLRespDuration`
	if stmtDetail != nil {
		stmtDetail.WriteSQLRespDuration += time.Since(start)
	}

	if cl, ok := rs.(resultset.FetchNotifier); ok {
		cl.OnFetchReturned()
	}

	start = time.Now()
	err = cc.writeEOF(ctx, serverStatus)
	if stmtDetail != nil {
		stmtDetail.WriteSQLRespDuration += time.Since(start)
	}
	return err
}
