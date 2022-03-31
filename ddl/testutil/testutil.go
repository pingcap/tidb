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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package testutil

import (
	"context"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/types"
	"github.com/tikv/client-go/v2/tikvrpc"
)

// SessionExecInGoroutine export for testing.
func SessionExecInGoroutine(s kv.Storage, dbName, sql string, done chan error) {
	ExecMultiSQLInGoroutine(s, dbName, []string{sql}, done)
}

// ExecMultiSQLInGoroutine exports for testing.
func ExecMultiSQLInGoroutine(s kv.Storage, dbName string, multiSQL []string, done chan error) {
	go func() {
		se, err := session.CreateSession4Test(s)
		if err != nil {
			done <- errors.Trace(err)
			return
		}
		defer se.Close()
		_, err = se.Execute(context.Background(), "use "+dbName)
		if err != nil {
			done <- errors.Trace(err)
			return
		}
		for _, sql := range multiSQL {
			rs, err := se.Execute(context.Background(), sql)
			if err != nil {
				done <- errors.Trace(err)
				return
			}
			if rs != nil {
				done <- errors.Errorf("RecordSet should be empty")
				return
			}
			done <- nil
		}
	}()
}

// ExtractAllTableHandles extracts all handles of a given table.
func ExtractAllTableHandles(se session.Session, dbName, tbName string) ([]int64, error) {
	dom := domain.GetDomain(se)
	tbl, err := dom.InfoSchema().TableByName(model.NewCIStr(dbName), model.NewCIStr(tbName))
	if err != nil {
		return nil, err
	}
	err = se.NewTxn(context.Background())
	if err != nil {
		return nil, err
	}

	var allHandles []int64
	err = tables.IterRecords(tbl, se, nil,
		func(h kv.Handle, _ []types.Datum, _ []*table.Column) (more bool, err error) {
			allHandles = append(allHandles, h.IntValue())
			return true, nil
		})
	return allHandles, err
}

func GetReqStartKeyAndTxnTs(req *tikvrpc.Request) ([]byte, uint64, error) {
	var startKey []byte
	var ts uint64
	switch req.Type {
	case tikvrpc.CmdGet:
		request := req.Get()
		startKey = request.Key
		ts = request.Version
	case tikvrpc.CmdScan:
		request := req.Scan()
		startKey = request.StartKey
		ts = request.Version
	case tikvrpc.CmdPrewrite:
		request := req.Prewrite()
		startKey = request.Mutations[0].Key
		ts = request.StartVersion
	case tikvrpc.CmdCommit:
		request := req.Commit()
		startKey = request.Keys[0]
		ts = request.StartVersion
	case tikvrpc.CmdCleanup:
		request := req.Cleanup()
		startKey = request.Key
		ts = request.StartVersion
	case tikvrpc.CmdBatchGet:
		request := req.BatchGet()
		startKey = request.Keys[0]
		ts = request.Version
	case tikvrpc.CmdBatchRollback:
		request := req.BatchRollback()
		startKey = request.Keys[0]
		ts = request.StartVersion
	case tikvrpc.CmdScanLock:
		request := req.ScanLock()
		startKey = request.StartKey
		ts = request.MaxVersion
	case tikvrpc.CmdPessimisticLock:
		request := req.PessimisticLock()
		startKey = request.PrimaryLock
		ts = request.StartVersion
	case tikvrpc.CmdPessimisticRollback:
		request := req.PessimisticRollback()
		startKey = request.Keys[0]
		ts = request.StartVersion
	case tikvrpc.CmdCheckSecondaryLocks:
		request := req.CheckSecondaryLocks()
		startKey = request.Keys[0]
		ts = request.StartVersion
	case tikvrpc.CmdCop, tikvrpc.CmdCopStream:
		request := req.Cop()
		startKey = request.Ranges[0].Start
		ts = request.StartTs
	case tikvrpc.CmdGC, tikvrpc.CmdDeleteRange, tikvrpc.CmdTxnHeartBeat, tikvrpc.CmdRawGet,
		tikvrpc.CmdRawBatchGet, tikvrpc.CmdRawPut, tikvrpc.CmdRawBatchPut, tikvrpc.CmdRawDelete, tikvrpc.CmdRawBatchDelete, tikvrpc.CmdRawDeleteRange,
		tikvrpc.CmdRawScan, tikvrpc.CmdGetKeyTTL, tikvrpc.CmdRawCompareAndSwap, tikvrpc.CmdUnsafeDestroyRange, tikvrpc.CmdRegisterLockObserver,
		tikvrpc.CmdCheckLockObserver, tikvrpc.CmdRemoveLockObserver, tikvrpc.CmdPhysicalScanLock, tikvrpc.CmdStoreSafeTS,
		tikvrpc.CmdLockWaitInfo, tikvrpc.CmdMvccGetByKey, tikvrpc.CmdMvccGetByStartTs, tikvrpc.CmdSplitRegion,
		tikvrpc.CmdDebugGetRegionProperties, tikvrpc.CmdEmpty:
		// Ignore those requests since now, since it is no business with TopSQL.
	case tikvrpc.CmdBatchCop, tikvrpc.CmdMPPTask, tikvrpc.CmdMPPConn, tikvrpc.CmdMPPCancel, tikvrpc.CmdMPPAlive:
		// Ignore mpp requests.
	case tikvrpc.CmdResolveLock, tikvrpc.CmdCheckTxnStatus:
		// TODO: add resource tag for those request.
	default:
		return nil, 0, errors.New("unknown request, check the new type RPC request here")
	}
	return startKey, ts, nil
}
