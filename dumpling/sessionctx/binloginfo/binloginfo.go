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
	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tipb/go-binlog"
	goctx "golang.org/x/net/context"
)

// PumpClient is the gRPC client to write binlog, it is opened on server start and never close,
// shared by all sessions.
var PumpClient binlog.PumpClient

// keyType is a dummy type to avoid naming collision in context.
type keyType int

// String defines a Stringer function for debugging and pretty printing.
func (k keyType) String() string {
	if k == schemaVersionKey {
		return "schema_version"
	}
	return "binlog"
}

const (
	schemaVersionKey keyType = 0
	binlogKey        keyType = 1
)

// SetSchemaVersion sets schema version to the context.
func SetSchemaVersion(ctx context.Context, version int64) {
	ctx.SetValue(schemaVersionKey, version)
}

// GetSchemaVersion gets schema version in the context.
func GetSchemaVersion(ctx context.Context) int64 {
	v, ok := ctx.Value(schemaVersionKey).(int64)
	if !ok {
		log.Error("get schema version failed")
	}
	return v
}

// GetPrewriteValue gets binlog prewrite value in the context.
func GetPrewriteValue(ctx context.Context, createIfNotExists bool) *binlog.PrewriteValue {
	v, ok := ctx.Value(binlogKey).(*binlog.PrewriteValue)
	if !ok && createIfNotExists {
		schemaVer := GetSchemaVersion(ctx)
		v = &binlog.PrewriteValue{SchemaVersion: schemaVer}
		ctx.SetValue(binlogKey, v)
	}
	return v
}

// WriteBinlog writes a binlog to Pump.
func WriteBinlog(bin *binlog.Binlog, clusterID uint64) error {
	commitData, _ := bin.Marshal()
	req := &binlog.WriteBinlogReq{ClusterID: clusterID, Payload: commitData}
	resp, err := PumpClient.WriteBinlog(goctx.Background(), req)
	if err == nil && resp.Errmsg != "" {
		err = errors.New(resp.Errmsg)
	}
	return errors.Trace(err)
}

// SetDDLBinlog sets DDL binlog in the kv.Transaction.
func SetDDLBinlog(txn kv.Transaction, jobID int64, ddlQuery string) {
	bin := &binlog.Binlog{
		Tp:       binlog.BinlogType_Prewrite,
		DdlJobId: jobID,
		DdlQuery: []byte(ddlQuery),
	}
	txn.SetOption(kv.BinlogData, bin)
}

// ClearBinlog clears binlog in the Context.
func ClearBinlog(ctx context.Context) {
	ctx.ClearValue(binlogKey)
}
