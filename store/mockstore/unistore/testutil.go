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

package unistore

import (
	"errors"
	"fmt"
	"runtime"

	"github.com/pingcap/tidb/tablecodec"
	topsqlstate "github.com/pingcap/tidb/util/topsql/state"
	"github.com/tikv/client-go/v2/tikvrpc"
)

func checkResourceTagForTopSQL(req *tikvrpc.Request) error {
	if !topsqlstate.TopSQLEnabled() {
		return nil
	}
	tag := req.GetResourceGroupTag()
	if len(tag) > 0 {
		return nil
	}

	startKey, err := getReqStartKey(req)
	if err != nil {
		return err
	}
	var tid int64
	if tablecodec.IsRecordKey(startKey) {
		tid, _, _ = tablecodec.DecodeRecordKey(startKey)
	}
	if tablecodec.IsIndexKey(startKey) {
		tid, _, _, _ = tablecodec.DecodeIndexKey(startKey)
	}
	// since the error maybe "invalid record key", should just ignore check resource tag for this request.
	if tid > 0 {
		stack := getStack()
		return fmt.Errorf("%v req does not set the resource tag, tid: %v, stack: %v",
			req.Type.String(), tid, string(stack))
	}
	return nil
}

func getReqStartKey(req *tikvrpc.Request) ([]byte, error) {
	switch req.Type {
	case tikvrpc.CmdGet:
		request := req.Get()
		return request.Key, nil
	case tikvrpc.CmdScan:
		request := req.Scan()
		return request.StartKey, nil
	case tikvrpc.CmdPrewrite:
		request := req.Prewrite()
		return request.Mutations[0].Key, nil
	case tikvrpc.CmdCommit:
		request := req.Commit()
		return request.Keys[0], nil
	case tikvrpc.CmdCleanup:
		request := req.Cleanup()
		return request.Key, nil
	case tikvrpc.CmdBatchGet:
		request := req.BatchGet()
		return request.Keys[0], nil
	case tikvrpc.CmdBatchRollback:
		request := req.BatchRollback()
		return request.Keys[0], nil
	case tikvrpc.CmdScanLock:
		request := req.ScanLock()
		return request.StartKey, nil
	case tikvrpc.CmdPessimisticLock:
		request := req.PessimisticLock()
		return request.PrimaryLock, nil
	case tikvrpc.CmdCheckSecondaryLocks:
		request := req.CheckSecondaryLocks()
		return request.Keys[0], nil
	case tikvrpc.CmdCop, tikvrpc.CmdCopStream:
		request := req.Cop()
		return request.Ranges[0].Start, nil
	case tikvrpc.CmdGC, tikvrpc.CmdDeleteRange, tikvrpc.CmdTxnHeartBeat, tikvrpc.CmdRawGet,
		tikvrpc.CmdRawBatchGet, tikvrpc.CmdRawPut, tikvrpc.CmdRawBatchPut, tikvrpc.CmdRawDelete, tikvrpc.CmdRawBatchDelete, tikvrpc.CmdRawDeleteRange,
		tikvrpc.CmdRawScan, tikvrpc.CmdGetKeyTTL, tikvrpc.CmdRawCompareAndSwap, tikvrpc.CmdUnsafeDestroyRange, tikvrpc.CmdRegisterLockObserver,
		tikvrpc.CmdCheckLockObserver, tikvrpc.CmdRemoveLockObserver, tikvrpc.CmdPhysicalScanLock, tikvrpc.CmdStoreSafeTS,
		tikvrpc.CmdLockWaitInfo, tikvrpc.CmdMvccGetByKey, tikvrpc.CmdMvccGetByStartTs, tikvrpc.CmdSplitRegion,
		tikvrpc.CmdDebugGetRegionProperties, tikvrpc.CmdEmpty:
		// Ignore those requests since now, since it is no business with TopSQL.
		return nil, nil
	case tikvrpc.CmdBatchCop, tikvrpc.CmdMPPTask, tikvrpc.CmdMPPConn, tikvrpc.CmdMPPCancel, tikvrpc.CmdMPPAlive:
		// Ignore mpp requests.
		return nil, nil
	case tikvrpc.CmdResolveLock, tikvrpc.CmdCheckTxnStatus, tikvrpc.CmdPessimisticRollback:
		// TODO: add resource tag for those request. https://github.com/pingcap/tidb/issues/33621
		return nil, nil
	default:
		return nil, errors.New("unknown request, check the new type RPC request here")
	}
}

func getStack() []byte {
	const size = 1024 * 64
	buf := make([]byte, size)
	stackSize := runtime.Stack(buf, false)
	buf = buf[:stackSize]
	return buf
}
