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
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tipb/go-binlog"
)

// PumpClient is the gRPC client to write binlog, it is opened on server start and never close,
// shared by all sessions.
var PumpClient binlog.PumpClient

// ClusterID is set by command line argument, if not set, use default value 1.
var ClusterID uint64 = 1

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

// SetSchemaVersion sets schema version to a context.
func SetSchemaVersion(ctx context.Context, version int64) {
	ctx.SetValue(schemaVersionKey, version)
}

// GetSchemaVersion gets schema version in a context.
func GetSchemaVersion(ctx context.Context) int64 {
	v, ok := ctx.Value(schemaVersionKey).(int64)
	if !ok {
		log.Error("get schema version failed")
	}
	return v
}

// GetPrewriteValue gets binlog prewrite value in a context.
func GetPrewriteValue(ctx context.Context, createIfNotExists bool) *binlog.PrewriteValue {
	v, ok := ctx.Value(binlogKey).(*binlog.PrewriteValue)
	if !ok && createIfNotExists {
		schemaVer := GetSchemaVersion(ctx)
		v = &binlog.PrewriteValue{SchemaVersion: schemaVer}
		ctx.SetValue(binlogKey, v)
	}
	return v
}

// ClearBinlog clears binlog in a context.
func ClearBinlog(ctx context.Context) {
	ctx.ClearValue(binlogKey)
}
