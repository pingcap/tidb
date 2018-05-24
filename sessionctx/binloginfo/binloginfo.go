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
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/terror"
	binlog "github.com/pingcap/tipb/go-binlog"
	log "github.com/sirupsen/logrus"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

func init() {
	grpc.EnableTracing = false
}

var binlogWriteTimeout = 15 * time.Second

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

// SetGRPCTimeout sets grpc timeout for writing binlog.
func SetGRPCTimeout(timeout time.Duration) {
	if timeout < 300*time.Millisecond {
		log.Warnf("set binlog grpc timeout %s ignored, use default value %s", timeout, binlogWriteTimeout)
		return // Avoid invalid value
	}
	binlogWriteTimeout = timeout
}

// GetPrewriteValue gets binlog prewrite value in the context.
func GetPrewriteValue(ctx sessionctx.Context, createIfNotExists bool) *binlog.PrewriteValue {
	vars := ctx.GetSessionVars()
	v, ok := vars.TxnCtx.Binlog.(*binlog.PrewriteValue)
	if !ok && createIfNotExists {
		schemaVer := ctx.GetSessionVars().TxnCtx.SchemaVersion
		v = &binlog.PrewriteValue{SchemaVersion: schemaVer}
		vars.TxnCtx.Binlog = v
	}
	return v
}

var skipBinlog uint32
var ignoreError uint32

// DisableSkipBinlogFlag disable the skipBinlog flag.
func DisableSkipBinlogFlag() {
	atomic.StoreUint32(&skipBinlog, 0)
	log.Warn("[binloginfo] disable the skipBinlog flag")
}

// SetIgnoreError sets the ignoreError flag, this function called when TiDB start
// up and find config.Binlog.IgnoreError is true.
func SetIgnoreError(on bool) {
	if on {
		atomic.StoreUint32(&ignoreError, 1)
	} else {
		atomic.StoreUint32(&ignoreError, 0)
	}
}

// WriteBinlog writes a binlog to Pump.
func (info *BinlogInfo) WriteBinlog(clusterID uint64) error {
	skip := atomic.LoadUint32(&skipBinlog)
	if skip > 0 {
		metrics.CriticalErrorCounter.Add(1)
		return nil
	}

	commitData, err := info.Data.Marshal()
	if err != nil {
		return errors.Trace(err)
	}
	req := &binlog.WriteBinlogReq{ClusterID: clusterID, Payload: commitData}

	// Retry many times because we may raise CRITICAL error here.
	for i := 0; i < 20; i++ {
		var resp *binlog.WriteBinlogResp
		ctx, cancel := context.WithTimeout(context.Background(), binlogWriteTimeout)
		resp, err = info.Client.WriteBinlog(ctx, req)
		cancel()
		if err == nil && resp.Errmsg != "" {
			err = errors.New(resp.Errmsg)
		}
		if err == nil {
			return nil
		}
		if strings.Contains(err.Error(), "received message larger than max") {
			// This kind of error is not critical and not retryable, return directly.
			return errors.Errorf("binlog data is too large (%s)", err.Error())
		}
		log.Errorf("write binlog error %v", err)
		time.Sleep(time.Second)
	}

	if err != nil {
		if atomic.LoadUint32(&ignoreError) == 1 {
			log.Errorf("critical error, write binlog fail but error ignored: %s", errors.ErrorStack(err))
			metrics.CriticalErrorCounter.Add(1)
			// If error happens once, we'll stop writing binlog.
			atomic.CompareAndSwapUint32(&skipBinlog, skip, skip+1)
			return nil
		}
	}

	return terror.ErrCritical.GenByArgs(err)
}

// SetDDLBinlog sets DDL binlog in the kv.Transaction.
func SetDDLBinlog(client interface{}, txn kv.Transaction, jobID int64, ddlQuery string) {
	if client == nil {
		return
	}
	ddlQuery = addSpecialComment(ddlQuery)
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

const specialPrefix = `/*!90000 `

func addSpecialComment(ddlQuery string) string {
	if strings.Contains(ddlQuery, specialPrefix) {
		return ddlQuery
	}
	upperQuery := strings.ToUpper(ddlQuery)
	reg, err := regexp.Compile(`SHARD_ROW_ID_BITS\s*=\s*\d+`)
	terror.Log(err)
	loc := reg.FindStringIndex(upperQuery)
	if len(loc) < 2 {
		return ddlQuery
	}
	return ddlQuery[:loc[0]] + specialPrefix + ddlQuery[loc[0]:loc[1]] + ` */` + ddlQuery[loc[1]:]
}
