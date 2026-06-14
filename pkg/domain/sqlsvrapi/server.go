// Copyright 2025 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package sqlsvrapi

import (
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/owner"
	"github.com/pingcap/tidb/pkg/util"
)

// Runtime is the runtime view for accessing a storage through KV or session.
type Runtime interface {
	Store() kv.Storage
	SysSessionPool() util.DestroyableSessionPool
}

// KSRuntimeHandle is an acquired runtime handle for a target keyspace.
type KSRuntimeHandle interface {
	Runtime
	// Release releases the holding of the runtime handle. After calling Release,
	// the handle should not be used anymore.
	// the underlying runtime has different lifecycle, the handle is just a view
	// and does not manage the lifecycle of the runtime.
	Release()
}

// Server defines the interface for a SQL server.
// The SQL server manages nearly everything related to SQL execution.
type Server interface {
	// GetRuntime returns the runtime for current instance.
	GetRuntime() Runtime
	// AcquireKSRuntime acquires a runtime handle for the target keyspace.
	// The acquired handle should be released after use.
	// this is only used in next-gen to access keyspace other than current.
	AcquireKSRuntime(targetKS string, holderID string) (KSRuntimeHandle, error)
	GetDDLOwnerMgr() owner.Manager
}
