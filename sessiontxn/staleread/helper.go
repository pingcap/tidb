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
	"context"
	"time"

	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/kv"

	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/types"

	"github.com/pingcap/tidb/parser/ast"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/tikv/client-go/v2/oracle"
)

const maxAllowedStaleReadIntervalAfterNow = 100 * time.Millisecond

// ValidateStaleReadTS validates that readTS does not exceed the current time not strictly.
func ValidateStaleReadTS(ctx context.Context, sctx sessionctx.Context, readTS uint64) error {
	// readTS == 0 means no stale read
	if readTS == 0 {
		return nil
	}

	currentTS, err := sctx.GetStore().GetOracle().GetStaleTimestamp(ctx, oracle.GlobalTxnScope, 0)
	// If we fail to calculate currentTS from local time, fallback to get a timestamp from PD
	if err != nil {
		metrics.ValidateReadTSFromPDCount.Inc()
		currentVer, err := sctx.GetStore().CurrentVersion(oracle.GlobalTxnScope)
		if err != nil {
			return errors.Errorf("fail to validate read timestamp: %v", err)
		}
		currentTS = currentVer.Ver
	}
	if oracle.GetTimeFromTS(readTS).After(oracle.GetTimeFromTS(currentTS).Add(maxAllowedStaleReadIntervalAfterNow)) {
		return errors.Errorf("cannot set read timestamp to a future time")
	}
	return nil
}

func calculateAsOfTsExpr(sctx sessionctx.Context, asOfClause *ast.AsOfClause) (uint64, error) {
	tsVal, err := expression.EvalAstExpr(sctx, asOfClause.TsExpr)
	if err != nil {
		return 0, err
	}
	toTypeTimestamp := types.NewFieldType(mysql.TypeTimestamp)
	// We need at least the millionsecond here, so set fsp to 3.
	toTypeTimestamp.Decimal = 3
	tsTimestamp, err := tsVal.ConvertTo(sctx.GetSessionVars().StmtCtx, toTypeTimestamp)
	if err != nil {
		return 0, err
	}
	tsTime, err := tsTimestamp.GetMysqlTime().GoTime(sctx.GetSessionVars().Location())
	if err != nil {
		return 0, err
	}
	return oracle.GoTimeToTS(tsTime), nil
}

func calculateTsWithReadStaleness(sctx sessionctx.Context, readStaleness time.Duration) (uint64, error) {
	nowVal, err := expression.GetStmtTimestamp(sctx)
	if err != nil {
		return 0, err
	}
	tsVal := nowVal.Add(readStaleness)
	minTsVal := expression.GetMinSafeTime(sctx)
	return oracle.GoTimeToTS(expression.CalAppropriateTime(tsVal, nowVal, minTsVal)), nil
}

func parseAndValidateAsOf(sctx sessionctx.Context, asOf *ast.AsOfClause) (uint64, error) {
	if asOf == nil {
		return 0, nil
	}

	ts, err := calculateAsOfTsExpr(sctx, asOf)
	if err != nil {
		return 0, err
	}

	if err = ValidateStaleReadTS(context.TODO(), sctx, ts); err != nil {
		return 0, err
	}

	return ts, nil
}

func getTsEvaluatorFromReadStaleness(sctx sessionctx.Context) PreparedTSEvaluator {
	readStaleness := sctx.GetSessionVars().ReadStaleness
	if readStaleness == 0 {
		return nil
	}

	return func(sctx sessionctx.Context) (uint64, error) {
		return calculateTsWithReadStaleness(sctx, readStaleness)
	}
}

func getStaleReadReplicaScope(sctx sessionctx.Context) string {
	instanceScope := config.GetTxnScopeFromConfig()
	if sctx.GetSessionVars().GetReplicaRead().IsClosestRead() && instanceScope != kv.GlobalReplicaScope {
		return instanceScope
	}
	return kv.GlobalReplicaScope
}
