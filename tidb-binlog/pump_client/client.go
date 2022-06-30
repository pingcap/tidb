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

package client

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/tidb-binlog/node"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/etcd"
	pb "github.com/pingcap/tipb/go-binlog"
	pd "github.com/tikv/pd/client"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	// DefaultEtcdTimeout is the default timeout config for etcd.
	DefaultEtcdTimeout = 5 * time.Second

	// DefaultRetryTime is the default retry time for each pump.
	DefaultRetryTime = 10

	// DefaultBinlogWriteTimeout is the default max time binlog can use to write to pump.
	DefaultBinlogWriteTimeout = 15 * time.Second

	// CheckInterval is the default interval for check unavailable pumps.
	CheckInterval = 30 * time.Second
)

var (
	// ErrNoAvaliablePump means no available pump to write binlog.
	ErrNoAvaliablePump = errors.New("no available pump to write binlog")

	// CommitBinlogTimeout is the max retry duration time for write commit/rollback binlog.
	CommitBinlogTimeout = 10 * time.Minute

	// RetryInterval is the interval of retrying to write binlog.
	RetryInterval = 100 * time.Millisecond
)

// PumpInfos saves pumps' information in pumps client.
type PumpInfos struct {
	// Pumps saves the map of pump's nodeID and pump status.
	Pumps map[string]*PumpStatus

	// AvliablePumps saves the whole available pumps' status.
	AvaliablePumps map[string]*PumpStatus

	// UnAvaliablePumps saves the unavailable pumps.
	// And only pump with Online state in this map need check is it available.
	UnAvaliablePumps map[string]*PumpStatus
}

// NewPumpInfos returns a PumpInfos.
func NewPumpInfos() *PumpInfos {
	return &PumpInfos{
		Pumps:            make(map[string]*PumpStatus),
		AvaliablePumps:   make(map[string]*PumpStatus),
		UnAvaliablePumps: make(map[string]*PumpStatus),
	}
}

// PumpsClient is the client of pumps.
type PumpsClient struct {
	sync.RWMutex

	ctx context.Context

	cancel context.CancelFunc

	wg sync.WaitGroup

	// ClusterID is the cluster ID of this tidb cluster.
	ClusterID uint64

	// the registry of etcd.
	EtcdRegistry *node.EtcdRegistry

	// Pumps saves the pumps' information.
	Pumps *PumpInfos

	// Selector will select a suitable pump.
	Selector PumpSelector

	// the max retry time if write binlog failed, obsolete now.
	RetryTime int

	// BinlogWriteTimeout is the max time binlog can use to write to pump.
	BinlogWriteTimeout time.Duration

	// Security is the security config
	Security *tls.Config

	// binlog socket file path, for compatible with kafka version pump.
	binlogSocket string

	nodePath string
}

// NewPumpsClient returns a PumpsClient.
// TODO: get strategy from etcd, and can update strategy in real-time. Use Range as default now.
func NewPumpsClient(etcdURLs, strategy string, timeout time.Duration, securityOpt pd.SecurityOption) (*PumpsClient, error) {
	ectdEndpoints, err := util.ParseHostPortAddr(etcdURLs)
	if err != nil {
		return nil, errors.Trace(err)
	}

	// get clusterid
	pdCli, err := pd.NewClient(ectdEndpoints, securityOpt)
	if err != nil {
		return nil, errors.Trace(err)
	}
	clusterID := pdCli.GetClusterID(context.Background())
	pdCli.Close()

	security, err := util.ToTLSConfig(securityOpt.CAPath, securityOpt.CertPath, securityOpt.KeyPath)
	if err != nil {
		return nil, errors.Trace(err)
	}

	cli, err := etcd.NewClientFromCfg(ectdEndpoints, DefaultEtcdTimeout, node.DefaultRootPath, security)
	if err != nil {
		return nil, errors.Trace(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	newPumpsClient := &PumpsClient{
		ctx:                ctx,
		cancel:             cancel,
		ClusterID:          clusterID,
		EtcdRegistry:       node.NewEtcdRegistry(cli, DefaultEtcdTimeout),
		Pumps:              NewPumpInfos(),
		Selector:           NewSelector(strategy),
		BinlogWriteTimeout: timeout,
		Security:           security,
		nodePath:           path.Join(node.DefaultRootPath, node.NodePrefix[node.PumpNode]),
	}

	revision, err := newPumpsClient.getPumpStatus(ctx)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if len(newPumpsClient.Pumps.Pumps) == 0 {
		return nil, errors.New("no pump found in pd")
	}
	newPumpsClient.Selector.SetPumps(copyPumps(newPumpsClient.Pumps.AvaliablePumps))

	newPumpsClient.wg.Add(2)
	go func() {
		newPumpsClient.watchStatus(revision)
		newPumpsClient.wg.Done()
	}()

	go func() {
		newPumpsClient.detect()
		newPumpsClient.wg.Done()
	}()

	return newPumpsClient, nil
}

// NewLocalPumpsClient returns a PumpsClient, this PumpsClient will write binlog by socket file. For compatible with kafka version pump.
func NewLocalPumpsClient(etcdURLs, binlogSocket string, timeout time.Duration, securityOpt pd.SecurityOption) (*PumpsClient, error) {
	ectdEndpoints, err := util.ParseHostPortAddr(etcdURLs)
	if err != nil {
		return nil, errors.Trace(err)
	}

	// get clusterid
	pdCli, err := pd.NewClient(ectdEndpoints, securityOpt)
	if err != nil {
		return nil, errors.Trace(err)
	}
	clusterID := pdCli.GetClusterID(context.Background())
	pdCli.Close()

	security, err := util.ToTLSConfig(securityOpt.CAPath, securityOpt.CertPath, securityOpt.KeyPath)
	if err != nil {
		return nil, errors.Trace(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	newPumpsClient := &PumpsClient{
		ctx:                ctx,
		cancel:             cancel,
		ClusterID:          clusterID,
		Pumps:              NewPumpInfos(),
		Selector:           NewSelector(LocalUnix),
		BinlogWriteTimeout: timeout,
		Security:           security,
		binlogSocket:       binlogSocket,
	}
	newPumpsClient.getLocalPumpStatus(ctx)

	return newPumpsClient, nil
}

// getLocalPumpStatus gets the local pump. For compatible with kafka version tidb-binlog.
func (c *PumpsClient) getLocalPumpStatus(pctx context.Context) {
	nodeStatus := &node.Status{
		NodeID:  localPump,
		Addr:    c.binlogSocket,
		IsAlive: true,
		State:   node.Online,
	}
	c.addPump(NewPumpStatus(nodeStatus, c.Security), true)
}

// getPumpStatus gets all the pumps status in the etcd.
func (c *PumpsClient) getPumpStatus(pctx context.Context) (revision int64, err error) {
	nodesStatus, revision, err := c.EtcdRegistry.Nodes(pctx, node.NodePrefix[node.PumpNode])
	if err != nil {
		return -1, errors.Trace(err)
	}

	for _, status := range nodesStatus {
		log.Debug("[pumps client] get pump from pd", zap.Stringer("pump", status))
		c.addPump(NewPumpStatus(status, c.Security), false)
	}

	return revision, nil
}

// WriteBinlog writes binlog to a suitable pump. Tips: will never return error for commit/rollback binlog.
func (c *PumpsClient) WriteBinlog(binlog *pb.Binlog) error {
	c.RLock()
	pumpNum := len(c.Pumps.AvaliablePumps)
	selector := c.Selector
	c.RUnlock()

	var choosePump *PumpStatus
	meetError := false
	defer func() {
		if meetError {
			c.checkPumpAvaliable()
		}

		selector.Feedback(binlog.StartTs, binlog.Tp, choosePump)
	}()

	commitData, err := binlog.Marshal()
	if err != nil {
		return errors.Trace(err)
	}
	req := &pb.WriteBinlogReq{ClusterID: c.ClusterID, Payload: commitData}

	retryTime := 0
	var pump *PumpStatus
	var resp *pb.WriteBinlogResp
	startTime := time.Now()

	for {
		if pump == nil || binlog.Tp == pb.BinlogType_Prewrite {
			pump = selector.Select(binlog, retryTime)
		}
		if pump == nil {
			err = ErrNoAvaliablePump
			break
		}

		resp, err = pump.WriteBinlog(req, c.BinlogWriteTimeout)
		if err == nil && resp.Errmsg != "" {
			err = errors.New(resp.Errmsg)
		}
		if err == nil {
			choosePump = pump
			return nil
		}

		meetError = true
		log.Warn("[pumps client] write binlog to pump failed", zap.String("NodeID", pump.NodeID), zap.Stringer("binlog type", binlog.Tp), zap.Int64("start ts", binlog.StartTs), zap.Int64("commit ts", binlog.CommitTs), zap.Int("length", len(commitData)), zap.Error(err))

		if binlog.Tp != pb.BinlogType_Prewrite {
			// only use one pump to write commit/rollback binlog, util write success or blocked for ten minutes. And will not return error to tidb.
			if time.Since(startTime) > CommitBinlogTimeout {
				break
			}
		} else {
			if !isRetryableError(err) {
				// this kind of error is not retryable, return directly.
				return errors.Trace(err)
			}

			// make sure already retry every available pump.
			if time.Since(startTime) > c.BinlogWriteTimeout && retryTime > pumpNum {
				break
			}

			if isConnUnAvliable(err) {
				// this kind of error indicate that the grpc connection is not available, may be created the connection again can write success.
				pump.ResetGrpcClient()
			}

			retryTime++
		}

		if binlog.Tp != pb.BinlogType_Prewrite {
			time.Sleep(RetryInterval * 10)
		} else {
			time.Sleep(RetryInterval)
		}
	}

	log.Info("[pumps client] write binlog to available pumps all failed, will try unavailable pumps")
	pump, err1 := c.backoffWriteBinlog(req, binlog.Tp, binlog.StartTs)
	if err1 == nil {
		choosePump = pump
		return nil
	}

	return errors.Errorf("write binlog failed, the last error %v", err)
}

// Return directly for non p-binlog.
// Try every online pump for p-binlog.
func (c *PumpsClient) backoffWriteBinlog(req *pb.WriteBinlogReq, binlogType pb.BinlogType, startTS int64) (pump *PumpStatus, err error) {
	if binlogType != pb.BinlogType_Prewrite {
		// never return error for commit/rollback binlog.
		return nil, nil
	}

	c.RLock()
	allPumps := copyPumps(c.Pumps.Pumps)
	c.RUnlock()

	var resp *pb.WriteBinlogResp
	// send binlog to unavailable pumps to retry again.
	for _, pump := range allPumps {
		if !pump.ShouldBeUsable() {
			continue
		}

		pump.ResetGrpcClient()

		resp, err = pump.WriteBinlog(req, c.BinlogWriteTimeout)
		if err == nil && resp.Errmsg != "" {
			err = errors.New(resp.Errmsg)
		}

		if err != nil {
			log.Warn("[pumps client] try write binlog failed",
				zap.String("error", err.Error()),
				zap.String("NodeID", pump.NodeID))
			continue
		}

		// if this pump can write binlog success, set this pump to available.
		log.Info("[pumps client] write binlog to pump success, set this pump to available", zap.String("NodeID", pump.NodeID))
		c.setPumpAvailable(pump, true)
		return pump, nil
	}

	return nil, errors.New("write binlog to unavailable pump failed")
}

func (c *PumpsClient) checkPumpAvaliable() {
	c.RLock()
	allPumps := copyPumps(c.Pumps.Pumps)
	c.RUnlock()

	for _, pump := range allPumps {
		if !pump.IsUsable() {
			c.setPumpAvailable(pump, false)
		}
	}
}

// setPumpAvailable set pump's isAvailable, and modify UnAvailablePumps or AvailablePumps.
func (c *PumpsClient) setPumpAvailable(pump *PumpStatus, available bool) {
	c.Lock()
	defer c.Unlock()

	log.Info("[pumps client] set pump available", zap.String("NodeID", pump.NodeID), zap.Bool("available", available))

	pump.Reset()

	if available {
		delete(c.Pumps.UnAvaliablePumps, pump.NodeID)
		if _, ok := c.Pumps.Pumps[pump.NodeID]; ok {
			c.Pumps.AvaliablePumps[pump.NodeID] = pump
		}
	} else {
		delete(c.Pumps.AvaliablePumps, pump.NodeID)
		if _, ok := c.Pumps.Pumps[pump.NodeID]; ok {
			c.Pumps.UnAvaliablePumps[pump.NodeID] = pump
		}
	}

	c.Selector.SetPumps(copyPumps(c.Pumps.AvaliablePumps))
}

// addPump add a new pump.
func (c *PumpsClient) addPump(pump *PumpStatus, updateSelector bool) {
	c.Lock()
	defer c.Unlock()

	if pump.ShouldBeUsable() {
		c.Pumps.AvaliablePumps[pump.NodeID] = pump
	} else {
		c.Pumps.UnAvaliablePumps[pump.NodeID] = pump
	}
	c.Pumps.Pumps[pump.NodeID] = pump

	if updateSelector {
		c.Selector.SetPumps(copyPumps(c.Pumps.AvaliablePumps))
	}

}

// SetSelectStrategy sets the selector's strategy, strategy should be 'range' or 'hash' now.
func (c *PumpsClient) SetSelectStrategy(strategy string) error {
	if strategy != Range && strategy != Hash {
		return errors.Errorf("strategy %s is not support", strategy)
	}

	c.Lock()
	c.Selector = NewSelector(strategy)
	c.Selector.SetPumps(copyPumps(c.Pumps.AvaliablePumps))
	c.Unlock()
	return nil
}

// updatePump update pump's status, and return whether pump's IsAvailable should be changed.
func (c *PumpsClient) updatePump(status *node.Status) (pump *PumpStatus, availableChanged, available bool) {
	var ok bool
	c.Lock()
	if pump, ok = c.Pumps.Pumps[status.NodeID]; ok {
		if pump.Status.State != status.State {
			if status.State == node.Online {
				availableChanged = true
				available = true
			} else if pump.IsUsable() {
				availableChanged = true
				available = false
			}
		}
		pump.Status = *status
	}
	c.Unlock()

	return
}

// removePump removes a pump, used when pump is offline.
func (c *PumpsClient) removePump(nodeID string) {
	c.Lock()
	if pump, ok := c.Pumps.Pumps[nodeID]; ok {
		pump.Reset()
	}
	delete(c.Pumps.Pumps, nodeID)
	delete(c.Pumps.UnAvaliablePumps, nodeID)
	delete(c.Pumps.AvaliablePumps, nodeID)
	c.Selector.SetPumps(copyPumps(c.Pumps.AvaliablePumps))
	c.Unlock()
}

// exist returns true if pumps client has pump matched this nodeID.
func (c *PumpsClient) exist(nodeID string) bool {
	c.RLock()
	_, ok := c.Pumps.Pumps[nodeID]
	c.RUnlock()
	return ok
}

// watchStatus watchs pump's status in etcd.
func (c *PumpsClient) watchStatus(revision int64) {
	rch := c.EtcdRegistry.WatchNode(c.ctx, c.nodePath, revision)

	for {
		select {
		case <-c.ctx.Done():
			log.Info("[pumps client] watch status finished")
			return
		case wresp := <-rch:
			if wresp.Err() != nil {
				// meet error, watch from the latest revision.
				// pump will update the key periodly, it's ok for we to lost some event here
				log.Warn("[pumps client] watch status meet error", zap.Error(wresp.Err()))
				rch = c.EtcdRegistry.WatchNode(c.ctx, c.nodePath, 0)
				continue
			}

			for _, ev := range wresp.Events {
				status := &node.Status{}
				err := json.Unmarshal(ev.Kv.Value, &status)
				if err != nil {
					log.Error("[pumps client] unmarshal pump status failed", zap.ByteString("value", ev.Kv.Value), zap.Error(err))
					continue
				}

				switch ev.Type {
				case mvccpb.PUT:
					if !c.exist(status.NodeID) {
						log.Info("[pumps client] find a new pump", zap.String("NodeID", status.NodeID))
						c.addPump(NewPumpStatus(status, c.Security), true)
						continue
					}

					pump, availableChanged, available := c.updatePump(status)
					if availableChanged {
						log.Info("[pumps client] pump's state is changed", zap.String("NodeID", pump.Status.NodeID), zap.String("state", status.State))
						c.setPumpAvailable(pump, available)
					}

				case mvccpb.DELETE:
					// now will not delete pump node in fact, just for compatibility.
					nodeID := node.AnalyzeNodeID(string(ev.Kv.Key))
					log.Info("[pumps client] remove pump", zap.String("NodeID", nodeID))
					c.removePump(nodeID)
				}
			}
		}
	}
}

// detect send detect binlog to pumps with online state in UnAvaliablePumps,
func (c *PumpsClient) detect() {
	checkTick := time.NewTicker(CheckInterval)
	defer checkTick.Stop()

	for {
		select {
		case <-c.ctx.Done():
			log.Info("[pumps client] heartbeat finished")
			return
		case <-checkTick.C:
			// send detect binlog to pump, if this pump can return response without error
			// means this pump is available.
			needCheckPumps := make([]*PumpStatus, 0, len(c.Pumps.UnAvaliablePumps))
			checkPassPumps := make([]*PumpStatus, 0, 1)

			// TODO: send a more normal request, currently pump will just return success for this empty payload
			// not write to Storage
			req := &pb.WriteBinlogReq{ClusterID: c.ClusterID, Payload: nil}
			c.RLock()
			for _, pump := range c.Pumps.UnAvaliablePumps {
				if pump.ShouldBeUsable() {
					needCheckPumps = append(needCheckPumps, pump)
				}
			}
			c.RUnlock()

			for _, pump := range needCheckPumps {
				_, err := pump.WriteBinlog(req, c.BinlogWriteTimeout)
				if err == nil {
					log.Info("[pumps client] write detect binlog to unavailable pump success", zap.String("NodeID", pump.NodeID))
					checkPassPumps = append(checkPassPumps, pump)
				} else {
					log.Warn("[pumps client] write detect binlog to pump failed", zap.String("NodeID", pump.NodeID), zap.Error(err))
				}
			}

			for _, pump := range checkPassPumps {
				c.setPumpAvailable(pump, true)
			}
		}
	}
}

// Close closes the PumpsClient.
func (c *PumpsClient) Close() {
	log.Info("[pumps client] is closing")
	c.cancel()
	c.wg.Wait()
	log.Info("[pumps client] is closed")
}

func isRetryableError(err error) bool {
	// ResourceExhausted is a error code in grpc.
	// ResourceExhausted indicates some resource has been exhausted, perhaps
	// a per-user quota, or perhaps the entire file system is out of space.
	// https://github.com/grpc/grpc-go/blob/9cc4fdbde2304827ffdbc7896f49db40c5536600/codes/codes.go#L76
	return !strings.Contains(err.Error(), "ResourceExhausted")
}

func isConnUnAvliable(err error) bool {
	// Unavailable indicates the service is currently unavailable.
	// This is a most likely a transient condition and may be corrected
	// by retrying with a backoff.
	// https://github.com/grpc/grpc-go/blob/76cc50721c5fde18bae10a36f4c202f5f2f95bb7/codes/codes.go#L139
	return status.Code(err) == codes.Unavailable
}

func copyPumps(pumps map[string]*PumpStatus) []*PumpStatus {
	ps := make([]*PumpStatus, 0, len(pumps))
	for _, pump := range pumps {
		ps = append(ps, pump)
	}

	return ps
}
