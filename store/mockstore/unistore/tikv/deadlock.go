// Copyright 2019-present PingCAP, Inc.
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

package tikv

import (
	"context"
	"errors"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"

	deadlockPb "github.com/pingcap/kvproto/pkg/deadlock"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/store/mockstore/unistore/pd"
	"github.com/pingcap/tidb/store/mockstore/unistore/tikv/kverrors"
	"github.com/pingcap/tidb/store/mockstore/unistore/util/lockwaiter"
)

// Follower will send detection rpc to Leader
const (
	Follower = iota
	Leader
)

// DetectorServer represents a detector server.
type DetectorServer struct {
	Detector *Detector
	role     int32
}

// Detect detects deadlock.
func (ds *DetectorServer) Detect(req *deadlockPb.DeadlockRequest) *deadlockPb.DeadlockResponse {
	switch req.Tp {
	case deadlockPb.DeadlockRequestType_Detect:
		err := ds.Detector.Detect(req.Entry.Txn, req.Entry.WaitForTxn, req.Entry.KeyHash, diagnosticContext{
			key:              req.Entry.Key,
			resourceGroupTag: req.Entry.ResourceGroupTag,
		})
		if err != nil {
			resp := convertErrToResp(err, req.Entry.Txn, req.Entry.WaitForTxn, req.Entry.KeyHash)
			return resp
		}
	case deadlockPb.DeadlockRequestType_CleanUpWaitFor:
		ds.Detector.CleanUpWaitFor(req.Entry.Txn, req.Entry.WaitForTxn, req.Entry.KeyHash)
	case deadlockPb.DeadlockRequestType_CleanUp:
		ds.Detector.CleanUp(req.Entry.Txn)
	}
	return nil
}

// DetectorClient represents a detector client.
type DetectorClient struct {
	pdClient     pd.Client
	sendCh       chan *deadlockPb.DeadlockRequest
	waitMgr      *lockwaiter.Manager
	streamCli    deadlockPb.Deadlock_DetectClient
	streamCancel context.CancelFunc
	streamConn   *grpc.ClientConn
}

// getLeaderAddr will send request to pd to find out the
// current leader node for the first region
func (dt *DetectorClient) getLeaderAddr() (string, error) {
	// find first region from pd, get the first region leader
	ctx := context.Background()
	region, err := dt.pdClient.GetRegion(ctx, []byte{})
	if err != nil {
		log.Error("get first region failed", zap.Error(err))
		return "", err
	}
	if region.Leader == nil {
		return "", errors.New("no leader")
	}
	leaderStoreMeta, err := dt.pdClient.GetStore(ctx, region.Leader.GetStoreId())
	if err != nil {
		log.Error("get store failed", zap.Uint64("id", region.Leader.GetStoreId()), zap.Error(err))
		return "", err
	}
	log.Warn("getLeaderAddr", zap.Stringer("leader peer", region.Leader), zap.String("addr", leaderStoreMeta.GetAddress()))
	return leaderStoreMeta.GetAddress(), nil
}

// rebuildStreamClient builds connection to the first region leader,
// it's not thread safe and should be called only by `DetectorClient.Start` or `DetectorClient.SendReqLoop`
func (dt *DetectorClient) rebuildStreamClient() error {
	leaderAddr, err := dt.getLeaderAddr()
	if err != nil {
		return err
	}
	cc, err := grpc.Dial(leaderAddr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	if dt.streamConn != nil {
		err = dt.streamConn.Close()
		if err != nil {
			return err
		}
	}
	dt.streamConn = cc
	ctx, cancel := context.WithCancel(context.Background())
	stream, err := deadlockPb.NewDeadlockClient(cc).Detect(ctx)
	if err != nil {
		cancel()
		return err
	}
	log.Info("build stream client successfully", zap.String("leader addr", leaderAddr))
	dt.streamCli = stream
	dt.streamCancel = cancel
	go dt.recvLoop(dt.streamCli)
	return nil
}

// NewDetectorClient will create a new detector util, entryTTL is used for
// recycling the lock wait edge in detector wait wap. chSize is the pending
// detection sending task size(used on non leader node)
func NewDetectorClient(waiterMgr *lockwaiter.Manager, pdClient pd.Client) *DetectorClient {
	chSize := 10000
	newDetector := &DetectorClient{
		sendCh:   make(chan *deadlockPb.DeadlockRequest, chSize),
		waitMgr:  waiterMgr,
		pdClient: pdClient,
	}
	return newDetector
}

// sendReqLoop will send detection request to leader, stream connection will be rebuilt and
// a new recv goroutine using the same stream client will be created
func (dt *DetectorClient) sendReqLoop() {
	var (
		err        error
		rebuildErr error
		req        *deadlockPb.DeadlockRequest
	)
	for {
		if dt.streamCli == nil {
			rebuildErr = dt.rebuildStreamClient()
			if rebuildErr != nil {
				log.Error("rebuild connection to first region failed", zap.Error(rebuildErr))
				time.Sleep(3 * time.Second)
				continue
			}
		}
		req = <-dt.sendCh
		err = dt.streamCli.Send(req)
		if err != nil {
			log.Warn("send failed, invalid current stream and try to rebuild connection", zap.Error(err))
			dt.streamCancel()
			dt.streamCli = nil
		}
	}
}

// recvLoop tries to recv response(current only deadlock error) from leader, break loop if errors happen
func (dt *DetectorClient) recvLoop(streamCli deadlockPb.Deadlock_DetectClient) {
	var (
		err  error
		resp *deadlockPb.DeadlockResponse
	)
	for {
		resp, err = streamCli.Recv()
		if err != nil {
			log.Warn("recv from failed, stop receive", zap.Error(err))
			break
		}
		// here only detection request will get response from leader
		dt.waitMgr.WakeUpForDeadlock(resp)
	}
}

func (dt *DetectorClient) handleRemoteTask(requestType deadlockPb.DeadlockRequestType,
	txnTs uint64, waitForTxnTs uint64, keyHash uint64, diagCtx diagnosticContext) {
	detectReq := &deadlockPb.DeadlockRequest{}
	detectReq.Tp = requestType
	detectReq.Entry.Txn = txnTs
	detectReq.Entry.WaitForTxn = waitForTxnTs
	detectReq.Entry.KeyHash = keyHash
	detectReq.Entry.Key = diagCtx.key
	detectReq.Entry.ResourceGroupTag = diagCtx.resourceGroupTag
	dt.sendCh <- detectReq
}

// CleanUp processes cleaup task on local detector
// user interfaces
func (dt *DetectorClient) CleanUp(startTs uint64) {
	dt.handleRemoteTask(deadlockPb.DeadlockRequestType_CleanUp, startTs, 0, 0, diagnosticContext{})
}

// CleanUpWaitFor cleans up the specific wait edge in detector's wait map
func (dt *DetectorClient) CleanUpWaitFor(txnTs, waitForTxn, keyHash uint64) {
	dt.handleRemoteTask(deadlockPb.DeadlockRequestType_CleanUpWaitFor, txnTs, waitForTxn, keyHash, diagnosticContext{})
}

// Detect post the detection request to local deadlock detector or remote first region leader,
// the caller should use `waiter.ch` to receive possible deadlock response
func (dt *DetectorClient) Detect(txnTs uint64, waitForTxnTs uint64, keyHash uint64, key []byte, resourceGroupTag []byte) {
	dt.handleRemoteTask(deadlockPb.DeadlockRequestType_Detect, txnTs, waitForTxnTs, keyHash, diagnosticContext{
		key:              key,
		resourceGroupTag: resourceGroupTag,
	})
}

// convertErrToResp converts `ErrDeadlock` to `DeadlockResponse` proto type
func convertErrToResp(errDeadlock *kverrors.ErrDeadlock, txnTs, waitForTxnTs, keyHash uint64) *deadlockPb.DeadlockResponse {
	entry := deadlockPb.WaitForEntry{}
	entry.Txn = txnTs
	entry.WaitForTxn = waitForTxnTs
	entry.KeyHash = keyHash
	resp := &deadlockPb.DeadlockResponse{}
	resp.Entry = entry
	resp.DeadlockKeyHash = errDeadlock.DeadlockKeyHash

	resp.WaitChain = make([]*deadlockPb.WaitForEntry, 0, len(errDeadlock.WaitChain))
	for _, item := range errDeadlock.WaitChain {
		resp.WaitChain = append(resp.WaitChain, &deadlockPb.WaitForEntry{
			Txn:              item.Txn,
			WaitForTxn:       item.WaitForTxn,
			KeyHash:          item.KeyHash,
			Key:              item.Key,
			ResourceGroupTag: item.ResourceGroupTag,
		})
	}

	return resp
}

// NewDetectorServer creates local detector used by RPC detection handler
func NewDetectorServer() *DetectorServer {
	entryTTL := 3 * time.Second
	urgentSize := uint64(100000)
	exipreInterval := 3600 * time.Second
	svr := &DetectorServer{
		Detector: NewDetector(entryTTL, urgentSize, exipreInterval),
	}
	return svr
}

// IsLeader returns whether the server is leader or not.
func (ds *DetectorServer) IsLeader() bool {
	return atomic.LoadInt32(&ds.role) == Leader
}

// ChangeRole changes the server role.
func (ds *DetectorServer) ChangeRole(newRole int32) {
	atomic.StoreInt32(&ds.role, newRole)
}
