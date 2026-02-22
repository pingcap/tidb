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

package isolation

import (
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessiontxn"
	"github.com/pingcap/tidb/pkg/sessiontxn/internal"
	"github.com/pingcap/tidb/pkg/store/driver/txn"
	"github.com/pingcap/tidb/pkg/table/temptable"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/util/tableutil"
	"github.com/pingcap/tidb/pkg/util/tracing"
	tikvstore "github.com/tikv/client-go/v2/kv"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/txnkv/transaction"
)
// getSnapshotByTS get snapshot from store according to the snapshotTS and set the transaction related
// options before return
func (p *baseTxnContextProvider) getSnapshotByTS(snapshotTS uint64) (kv.Snapshot, error) {
	txn, err := p.sctx.Txn(false)
	if err != nil {
		return nil, err
	}

	txnCtx := p.sctx.GetSessionVars().TxnCtx

	var snapshot kv.Snapshot
	if txn.Valid() && txnCtx.StartTS == txnCtx.GetForUpdateTS() && txnCtx.StartTS == snapshotTS {
		snapshot = txn.GetSnapshot()
	} else {
		snapshot = internal.GetSnapshotWithTS(
			p.sctx,
			snapshotTS,
			temptable.SessionSnapshotInterceptor(p.sctx, p.infoSchema),
		)
	}
	snapshot.SetOption(kv.ReplicaRead, p.sctx.GetSessionVars().GetReplicaRead())

	return snapshot, nil
}

func (p *baseTxnContextProvider) SetOptionsOnTxnActive(txn kv.Transaction) {
	sessVars := p.sctx.GetSessionVars()

	readReplicaType := sessVars.GetReplicaRead()
	if readReplicaType.IsFollowerRead() {
		txn.SetOption(kv.ReplicaRead, readReplicaType)
	}

	if interceptor := temptable.SessionSnapshotInterceptor(
		p.sctx,
		p.infoSchema,
	); interceptor != nil {
		txn.SetOption(kv.SnapInterceptor, interceptor)
	}

	if sessVars.StmtCtx.WeakConsistency {
		txn.SetOption(kv.IsolationLevel, kv.RC)
	}

	internal.SetTxnAssertionLevel(txn, sessVars.AssertionLevel)

	if p.sctx.GetSessionVars().InRestrictedSQL {
		txn.SetOption(kv.RequestSourceInternal, true)
	}

	if txn.IsPipelined() {
		txn.SetOption(kv.RequestSourceType, "p-dml")
	} else if tp := p.sctx.GetSessionVars().RequestSourceType; tp != "" {
		txn.SetOption(kv.RequestSourceType, tp)
	}

	if sessVars.LoadBasedReplicaReadThreshold > 0 {
		txn.SetOption(kv.LoadBasedReplicaReadThreshold, sessVars.LoadBasedReplicaReadThreshold)
	}

	txn.SetOption(kv.CommitHook, func(info string, _ error) { sessVars.LastTxnInfo = info })
	txn.SetOption(kv.EnableAsyncCommit, sessVars.EnableAsyncCommit)
	txn.SetOption(kv.Enable1PC, sessVars.Enable1PC)
	if sessVars.DiskFullOpt != kvrpcpb.DiskFullOpt_NotAllowedOnFull {
		txn.SetDiskFullOpt(sessVars.DiskFullOpt)
	}
	txn.SetOption(kv.InfoSchema, sessVars.TxnCtx.InfoSchema)
	if sessVars.StmtCtx.KvExecCounter != nil {
		// Bind an interceptor for client-go to count the number of SQL executions of each TiKV.
		txn.SetOption(kv.RPCInterceptor, sessVars.StmtCtx.KvExecCounter.RPCInterceptor())
	}
	txn.SetOption(kv.ResourceGroupTagger, sessVars.StmtCtx.GetResourceGroupTagger())
	txn.SetOption(kv.ExplicitRequestSourceType, sessVars.ExplicitRequestSourceType)

	if p.causalConsistencyOnly || !sessVars.GuaranteeLinearizability {
		// priority of the sysvar is lower than `start transaction with causal consistency only`
		txn.SetOption(kv.GuaranteeLinearizability, false)
	} else {
		// We needn't ask the TiKV client to guarantee linearizability for auto-commit transactions
		// because the property is naturally holds:
		// We guarantee the commitTS of any transaction must not exceed the next timestamp from the TSO.
		// An auto-commit transaction fetches its startTS from the TSO so its commitTS > its startTS > the commitTS
		// of any previously committed transactions.
		// Additionally, it's required to guarantee linearizability for snapshot read-only transactions though
		// it does take effects on read-only transactions now.
		txn.SetOption(
			kv.GuaranteeLinearizability,
			!sessVars.IsAutocommit() ||
				sessVars.SnapshotTS > 0 ||
				p.enterNewTxnType == sessiontxn.EnterNewTxnDefault ||
				p.enterNewTxnType == sessiontxn.EnterNewTxnWithBeginStmt,
		)
	}

	txn.SetOption(kv.SessionID, p.sctx.GetSessionVars().ConnectionID)

	// backgroundGoroutineWaitGroup is pre-initialized before the closure to avoid accessing `p.sctx` in the closure,
	// which may cause unexpected race condition.
	backgroundGoroutineWaitGroup := p.sctx.GetCommitWaitGroup()
	lifecycleHooks := transaction.LifecycleHooks{
		Pre: func() {
			backgroundGoroutineWaitGroup.Add(1)
		},
		Post: func() {
			backgroundGoroutineWaitGroup.Done()
		},
	}
	txn.SetOption(kv.BackgroundGoroutineLifecycleHooks, lifecycleHooks)
}

func (p *baseTxnContextProvider) SetOptionsBeforeCommit(
	txn kv.Transaction, commitTSChecker func(uint64) bool,
) error {
	sessVars := p.sctx.GetSessionVars()
	// Pipelined dml txn already flushed mutations into stores, so we don't need to set options for them.
	// Instead, some invariants must be checked to avoid anomalies though are unreachable in designed usages.
	if p.txn.IsPipelined() {
		if p.txn.IsPipelined() && !sessVars.TxnCtx.EnableMDL {
			return errors.New("cannot commit pipelined transaction without Metadata Lock: MDL is OFF")
		}
		if len(sessVars.TxnCtx.TemporaryTables) > 0 {
			return errors.New("pipelined dml with temporary tables is not allowed")
		}
		if sessVars.CDCWriteSource != 0 {
			return errors.New("pipelined dml with CDC source is not allowed")
		}
		if commitTSChecker != nil {
			return errors.New("pipelined dml with commitTS checker is not allowed")
		}
		return nil
	}

	// set resource tagger again for internal tasks separated in different transactions
	txn.SetOption(kv.ResourceGroupTagger, sessVars.StmtCtx.GetResourceGroupTagger())

	// Get the related table or partition IDs.
	relatedPhysicalTables := sessVars.TxnCtx.TableDeltaMap
	// Get accessed temporary tables in the transaction.
	temporaryTables := sessVars.TxnCtx.TemporaryTables
	physicalTableIDs := make([]int64, 0, len(relatedPhysicalTables))
	for id := range relatedPhysicalTables {
		// Schema change on global temporary tables doesn't affect transactions.
		if _, ok := temporaryTables[id]; ok {
			continue
		}
		physicalTableIDs = append(physicalTableIDs, id)
	}
	needCheckSchemaByDelta := true
	// Set this option for 2 phase commit to validate schema lease.
	if sessVars.TxnCtx != nil {
		needCheckSchemaByDelta = !sessVars.TxnCtx.EnableMDL
	}

	// TODO: refactor SetOption usage to avoid race risk, should detect it in test.
	// The pipelined txn will may be flushed in background, not touch the options to avoid races.
	// to avoid session set overlap the txn set.
	txn.SetOption(
		kv.SchemaChecker,
		domain.NewSchemaChecker(
			p.sctx.GetSchemaValidator(),
			p.GetTxnInfoSchema().SchemaMetaVersion(),
			physicalTableIDs,
			needCheckSchemaByDelta,
		),
	)

	if sessVars.StmtCtx.KvExecCounter != nil {
		// Bind an interceptor for client-go to count the number of SQL executions of each TiKV.
		txn.SetOption(kv.RPCInterceptor, sessVars.StmtCtx.KvExecCounter.RPCInterceptor())
	}

	if tables := sessVars.TxnCtx.TemporaryTables; len(tables) > 0 {
		txn.SetOption(kv.KVFilter, temporaryTableKVFilter(tables))
	}

	var txnSource uint64
	if val := txn.GetOption(kv.TxnSource); val != nil {
		txnSource, _ = val.(uint64)
	}
	// If the transaction is started by CDC, we need to set the CDCWriteSource option.
	if sessVars.CDCWriteSource != 0 {
		err := kv.SetCDCWriteSource(&txnSource, sessVars.CDCWriteSource)
		if err != nil {
			return errors.Trace(err)
		}

		txn.SetOption(kv.TxnSource, txnSource)
	}

	if commitTSChecker != nil {
		txn.SetOption(kv.CommitTSUpperBoundCheck, commitTSChecker)
	}

	// Optimization:
	// If an auto-commit optimistic transaction can retry in pessimistic mode,
	// do not resolve locks when prewrite.
	// 1. safety: The locks can be resolved later when it retries in pessimistic mode.
	// 2. benefit: In high-contention scenarios, pessimistic transactions perform better.
	prewriteEncounterLockPolicy := transaction.TryResolvePolicy
	if sessVars.TxnCtx.CouldRetry &&
		sessVars.IsAutocommit() &&
		!sessVars.InTxn() &&
		!sessVars.TxnCtx.IsPessimistic {
		prewriteEncounterLockPolicy = transaction.NoResolvePolicy
	}
	txn.SetOption(kv.PrewriteEncounterLockPolicy, prewriteEncounterLockPolicy)

	return nil
}

// canReuseTxnWhenExplicitBegin returns whether we should reuse the txn when starting a transaction explicitly
func canReuseTxnWhenExplicitBegin(sctx sessionctx.Context) bool {
	sessVars := sctx.GetSessionVars()
	txnCtx := sessVars.TxnCtx
	// If BEGIN is the first statement in TxnCtx, we can reuse the existing transaction, without the
	// need to call NewTxn, which commits the existing transaction and begins a new one.
	// If the last un-committed/un-rollback transaction is a time-bounded read-only transaction, we should
	// always create a new transaction.
	// If the variable `tidb_snapshot` is set, we should always create a new transaction because the current txn may be
	// initialized with snapshot ts.
	return txnCtx.History == nil && !txnCtx.IsStaleness && sessVars.SnapshotTS == 0
}

// newOracleFuture creates new future according to the scope and the session context
func newOracleFuture(ctx context.Context, sctx sessionctx.Context, scope string) oracle.Future {
	r, ctx := tracing.StartRegionEx(ctx, "isolation.newOracleFuture")
	defer r.End()

	failpoint.Inject("requestTsoFromPD", func() {
		sessiontxn.TsoRequestCountInc(sctx)
	})

	oracleStore := sctx.GetStore().GetOracle()
	option := &oracle.Option{TxnScope: scope}

	if sctx.GetSessionVars().UseLowResolutionTSO() {
		return oracleStore.GetLowResolutionTimestampAsync(ctx, option)
	}
	return oracleStore.GetTimestampAsync(ctx, option)
}

// funcFuture implements oracle.Future
type funcFuture func() (uint64, error)

// Wait returns a ts got from the func
func (f funcFuture) Wait() (uint64, error) {
	return f()
}

// basePessimisticTxnContextProvider extends baseTxnContextProvider with some functionalities that are commonly used in
// pessimistic transactions.
type basePessimisticTxnContextProvider struct {
	baseTxnContextProvider
}

// OnPessimisticStmtStart is the hook that should be called when starts handling a pessimistic DML or
// a pessimistic select-for-update statements.
func (p *basePessimisticTxnContextProvider) OnPessimisticStmtStart(ctx context.Context) error {
	if err := p.baseTxnContextProvider.OnPessimisticStmtStart(ctx); err != nil {
		return err
	}
	if p.sctx.GetSessionVars().PessimisticTransactionFairLocking &&
		p.txn != nil &&
		p.sctx.GetSessionVars().ConnectionID != 0 &&
		!p.sctx.GetSessionVars().InRestrictedSQL {
		if err := p.txn.StartFairLocking(); err != nil {
			return err
		}
	}
	return nil
}

// OnPessimisticStmtEnd is the hook that should be called when finishes handling a pessimistic DML or
// select-for-update statement.
func (p *basePessimisticTxnContextProvider) OnPessimisticStmtEnd(ctx context.Context, isSuccessful bool) error {
	if err := p.baseTxnContextProvider.OnPessimisticStmtEnd(ctx, isSuccessful); err != nil {
		return err
	}
	if p.txn != nil && p.txn.IsInFairLockingMode() {
		if isSuccessful {
			if err := p.txn.DoneFairLocking(ctx); err != nil {
				return err
			}
		} else {
			if err := p.txn.CancelFairLocking(ctx); err != nil {
				return err
			}
		}
	}

	if isSuccessful {
		p.sctx.GetSessionVars().TxnCtx.FlushStmtPessimisticLockCache()
	} else {
		p.sctx.GetSessionVars().TxnCtx.CurrentStmtPessimisticLockCache = nil
	}
	return nil
}

func (p *basePessimisticTxnContextProvider) retryFairLockingIfNeeded(ctx context.Context) error {
	if p.txn != nil && p.txn.IsInFairLockingMode() {
		if err := p.txn.RetryFairLocking(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (p *basePessimisticTxnContextProvider) cancelFairLockingIfNeeded(ctx context.Context) error {
	if p.txn != nil && p.txn.IsInFairLockingMode() {
		if err := p.txn.CancelFairLocking(ctx); err != nil {
			return err
		}
	}
	return nil
}

type temporaryTableKVFilter map[int64]tableutil.TempTable

func (m temporaryTableKVFilter) IsUnnecessaryKeyValue(
	key, value []byte, flags tikvstore.KeyFlags,
) (bool, error) {
	tid := tablecodec.DecodeTableID(key)
	if _, ok := m[tid]; ok {
		return true, nil
	}

	// This is the default filter for all tables.
	defaultFilter := txn.TiDBKVFilter{}
	return defaultFilter.IsUnnecessaryKeyValue(key, value, flags)
}
