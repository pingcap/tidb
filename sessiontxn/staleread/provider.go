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

package staleread

import (
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessiontxn"
)

type staleReadTxnContextProvider struct {
	is               infoschema.InfoSchema
	ts               uint64
	readReplicaScope string
}

func (p *staleReadTxnContextProvider) GetTxnInfoSchema() infoschema.InfoSchema {
	return p.is
}

func (p *staleReadTxnContextProvider) GetReadReplicaScope() string {
	return p.readReplicaScope
}

func (p *staleReadTxnContextProvider) GetStmtReadTS() (uint64, error) {
	return p.ts, nil
}

func IsTxnStaleness(sctx sessionctx.Context) bool {
	provider := sessiontxn.GetTxnManager(sctx).GetContextProvider()
	if provider == nil {
		return false
	}

	_, ok := provider.(*staleReadTxnContextProvider)
	return ok
}
