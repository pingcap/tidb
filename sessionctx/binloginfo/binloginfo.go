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

package binloginfo

import (
	"sync"
	"time"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tipb/go-binlog"
	log "github.com/sirupsen/logrus"
	goctx "golang.org/x/net/context"
	grpc "google.golang.org/grpc"
)

func init() {
	grpc.EnableTracing = false
}

// pumpClient is the gRPC client to write binlog, it is opened on server start and never close,
// shared by all sessions.
var pumpClient binlog.PumpClient
var pumpClientLock sync.RWMutex

// BinlogInfo contains binlog data and binlog client.
type BinlogInfo struct {
	Data   *binlog.Binlog
	Client binlog.PumpClient
}

// GetPumpClient gets the pump client instance.
func GetPumpClient() binlog.PumpClient {
	pumpClientLock.RLock()
	client := pumpClient
	pumpClientLock.RUnlock()
	return client
}

// SetPumpClient sets the pump client instance.
func SetPumpClient(client binlog.PumpClient) {
	pumpClientLock.Lock()
	pumpClient = client
	pumpClientLock.Unlock()
}

// GetPrewriteValue gets binlog prewrite value in the context.
func GetPrewriteValue(ctx context.Context, createIfNotExists bool) *binlog.PrewriteValue {
	vars := ctx.GetSessionVars()
	v, ok := vars.TxnCtx.Binlog.(*binlog.PrewriteValue)
	if !ok && createIfNotExists {
		schemaVer := ctx.GetSessionVars().TxnCtx.SchemaVersion
		v = &binlog.PrewriteValue{SchemaVersion: schemaVer}
		vars.TxnCtx.Binlog = v
	}
	return v
}

// WriteBinlog writes a binlog to Pump.
func (info *BinlogInfo) WriteBinlog(clusterID uint64) error {
	commitData, err := info.Data.Marshal()
	if err != nil {
		return errors.Trace(err)
	}
	req := &binlog.WriteBinlogReq{ClusterID: clusterID, Payload: commitData}

	// Retry many times because we may raise CRITICAL error here.
	for i := 0; i < 20; i++ {
		var resp *binlog.WriteBinlogResp
		resp, err = info.Client.WriteBinlog(goctx.Background(), req)
		if err == nil && resp.Errmsg != "" {
			err = errors.New(resp.Errmsg)
		}
		if err == nil {
			return nil
		}
		log.Errorf("write binlog error %v", err)
		time.Sleep(time.Second)
	}
	return terror.ErrCritical.GenByArgs(err)
}

// SetDDLBinlog sets DDL binlog in the kv.Transaction.
func SetDDLBinlog(client interface{}, txn kv.Transaction, jobID int64, ddlQuery string) {
	if client == nil {
		return
	}
	info := &BinlogInfo{
		Data: &binlog.Binlog{
			Tp:       binlog.BinlogType_Prewrite,
			DdlJobId: jobID,
			DdlQuery: []byte(ddlQuery),
		},
		Client: client.(binlog.PumpClient),
	}
	txn.SetOption(kv.BinlogInfo, info)
}
