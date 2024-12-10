// Copyright 2023 PingCAP, Inc.
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

package ddl

import (
	"github.com/pingcap/tidb/pkg/ddl/notifier"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/statistics/handle/util"
)

func (h *ddlHandlerImpl) onExchangeAPartition(t *notifier.SchemaChangeEvent) error {
	globalTableInfo, originalPartInfo,
		originalTableInfo := t.GetExchangePartitionInfo()
	// Note: Put all the operations in a transaction.
	return util.CallWithSCtx(h.statsHandler.SPool(), func(sctx sessionctx.Context) error {
		return updateGlobalTableStats4ExchangePartition(
			util.StatsCtx,
			sctx,
			globalTableInfo,
			originalPartInfo,
			originalTableInfo,
		)
	}, util.FlagWrapTxn)
}
