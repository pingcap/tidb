// Copyright 2026 PingCAP, Inc.

package expression

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/expression/sessionexpr"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/types"
	driver "github.com/pingcap/tidb/pkg/types/parser_driver"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

func TestStoredFuncSigCloneDoesNotShareCallStmt(t *testing.T) {
	origEval4StoredFunc := Eval4StoredFunc
	origGetCallStmt4StoredFuncExpr := GetCallStmt4StoredFuncExpr
	t.Cleanup(func() {
		Eval4StoredFunc = origEval4StoredFunc
		GetCallStmt4StoredFuncExpr = origGetCallStmt4StoredFuncExpr
	})

	origEnableProcedure := variable.TiDBEnableProcedureValue.Load()
	variable.TiDBEnableProcedureValue.Store(true)
	t.Cleanup(func() {
		variable.TiDBEnableProcedureValue.Store(origEnableProcedure)
	})

	GetCallStmt4StoredFuncExpr = func(_ context.Context, _ sessionctx.Context, _ *ast.CallStmt) (any, error) {
		return struct{}{}, nil
	}

	var enterCount int32
	unblock := make(chan struct{})
	Eval4StoredFunc = func(_ sessionctx.Context, _ any, node *ast.CallStmt) (*types.Datum, error) {
		switch atomic.AddInt32(&enterCount, 1) {
		case 1:
			select {
			case <-unblock:
			case <-time.After(5 * time.Second):
				return nil, context.DeadlineExceeded
			}
		case 2:
			close(unblock)
		}

		d := node.Procedure.Args[0].(*driver.ValueExpr).Datum
		return &d, nil
	}

	sctx := mock.NewContext()
	buildCtx := sessionexpr.NewExprContext(sctx)
	retType := types.NewFieldType(mysql.TypeLonglong)
	fc := &StoredFuncClass{Name: [2]string{"test", "f"}, RetType: retType}

	sig, err := fc.getFunction(buildCtx, []Expression{&Column{RetType: retType, Index: 0}})
	require.NoError(t, err)
	clonedSig := sig.Clone()
	clonedSig.(*storedFuncSig).sctx = mock.NewContext()

	evalCtx := buildCtx.GetEvalCtx()
	row1 := chunk.MutRowFromDatums([]types.Datum{types.NewIntDatum(1)}).ToRow()
	row2 := chunk.MutRowFromDatums([]types.Datum{types.NewIntDatum(2)}).ToRow()

	var wg sync.WaitGroup
	wg.Add(2)

	var d1, d2 types.Datum
	var err1, err2 error
	go func() {
		defer wg.Done()
		d1, err1 = evalBuiltinFunc(sig, evalCtx, row1)
	}()
	go func() {
		defer wg.Done()
		d2, err2 = evalBuiltinFunc(clonedSig, evalCtx, row2)
	}()
	wg.Wait()

	require.NoError(t, err1)
	require.NoError(t, err2)
	require.Equal(t, int64(1), d1.GetInt64())
	require.Equal(t, int64(2), d2.GetInt64())
}

func TestStoredFuncSigConcurrentClonesDoNotShareProcedureRuntime(t *testing.T) {
	origEval4StoredFunc := Eval4StoredFunc
	origGetCallStmt4StoredFuncExpr := GetCallStmt4StoredFuncExpr
	t.Cleanup(func() {
		Eval4StoredFunc = origEval4StoredFunc
		GetCallStmt4StoredFuncExpr = origGetCallStmt4StoredFuncExpr
	})

	origEnableProcedure := variable.TiDBEnableProcedureValue.Load()
	variable.TiDBEnableProcedureValue.Store(true)
	t.Cleanup(func() {
		variable.TiDBEnableProcedureValue.Store(origEnableProcedure)
	})

	GetCallStmt4StoredFuncExpr = func(_ context.Context, _ sessionctx.Context, _ *ast.CallStmt) (any, error) {
		return struct{}{}, nil
	}

	var active int32
	var maxActive int32
	firstReady := make(chan struct{})
	var firstReadyOnce sync.Once
	retType := types.NewFieldType(mysql.TypeLonglong)
	Eval4StoredFunc = func(sctx sessionctx.Context, _ any, node *ast.CallStmt) (*types.Datum, error) {
		arg := node.Procedure.Args[0].(*driver.ValueExpr).Datum.GetInt64()
		vars := sctx.GetSessionVars()
		vars.SetInCallProcedure()
		defer vars.OutCallProcedure(false)

		procCtx := variable.NewProcedureContext(variable.BLOCKLABEL)
		procVar := variable.NewProcedureVars("v", retType)
		value := types.NewIntDatum(arg)
		procVar.SetVar(value)
		procCtx.Vars = append(procCtx.Vars, procVar)
		if err := vars.SetProcedureContext(procCtx); err != nil {
			return nil, err
		}

		activeNow := atomic.AddInt32(&active, 1)
		for {
			prev := atomic.LoadInt32(&maxActive)
			if activeNow <= prev || atomic.CompareAndSwapInt32(&maxActive, prev, activeNow) {
				break
			}
		}
		if activeNow == 1 {
			firstReadyOnce.Do(func() {
				close(firstReady)
			})
		}
		defer atomic.AddInt32(&active, -1)

		time.Sleep(50 * time.Millisecond)

		_, ret, notFound := vars.GetProcedureVariable("v")
		require.False(t, notFound)
		return &ret, nil
	}

	sctx := mock.NewContext()
	buildCtx := sessionexpr.NewExprContext(sctx)
	fc := &StoredFuncClass{Name: [2]string{"test", "f"}, RetType: retType}

	sig, err := fc.getFunction(buildCtx, []Expression{&Column{RetType: retType, Index: 0}})
	require.NoError(t, err)
	clonedSig := sig.Clone()

	evalCtx := buildCtx.GetEvalCtx()
	row1 := chunk.MutRowFromDatums([]types.Datum{types.NewIntDatum(1)}).ToRow()
	row2 := chunk.MutRowFromDatums([]types.Datum{types.NewIntDatum(2)}).ToRow()

	var wg sync.WaitGroup
	wg.Add(2)

	var d1, d2 types.Datum
	var err1, err2 error
	go func() {
		defer wg.Done()
		d1, err1 = evalBuiltinFunc(sig, evalCtx, row1)
	}()
	<-firstReady
	go func() {
		defer wg.Done()
		d2, err2 = evalBuiltinFunc(clonedSig, evalCtx, row2)
	}()
	wg.Wait()

	require.NoError(t, err1)
	require.NoError(t, err2)
	require.Equal(t, int64(1), d1.GetInt64())
	require.Equal(t, int64(2), d2.GetInt64())
	require.Equal(t, int32(1), atomic.LoadInt32(&maxActive))
}

func TestSchemaQualifiedStoredFunctionResolvesFirst(t *testing.T) {
	origGetCallStmt4StoredFuncExpr := GetCallStmt4StoredFuncExpr
	t.Cleanup(func() {
		GetCallStmt4StoredFuncExpr = origGetCallStmt4StoredFuncExpr
	})

	origEnableProcedure := variable.TiDBEnableProcedureValue.Load()
	variable.TiDBEnableProcedureValue.Store(true)
	t.Cleanup(func() {
		variable.TiDBEnableProcedureValue.Store(origEnableProcedure)
	})

	GetCallStmt4StoredFuncExpr = func(_ context.Context, _ sessionctx.Context, _ *ast.CallStmt) (any, error) {
		return struct{}{}, nil
	}

	sctx := mock.NewContext()
	buildCtx := sessionexpr.NewExprContext(sctx)

	sc := sctx.GetSessionVars().StmtCtx
	sc.UserFuncCtx.Lock()
	sc.UserFuncCtx.StoredFuncName = map[[2]string]*types.FieldType{
		{"test", "abs"}: types.NewFieldType(mysql.TypeLonglong),
	}
	sc.UserFuncCtx.Unlock()

	expr, err := newFunctionImpl(buildCtx, 0, "test", "abs", types.NewFieldType(mysql.TypeUnspecified), nil)
	require.NoError(t, err)

	sf, ok := expr.(*ScalarFunction)
	require.True(t, ok)
	_, ok = sf.Function.(*storedFuncSig)
	require.True(t, ok)
	require.Equal(t, mysql.TypeLonglong, sf.GetType(buildCtx.GetEvalCtx()).GetType())
}

func TestSchemaQualifiedBuiltinFallsBackWhenStoredFunctionMissing(t *testing.T) {
	sctx := mock.NewContext()
	buildCtx := sessionexpr.NewExprContext(sctx)

	sc := sctx.GetSessionVars().StmtCtx
	sc.UserFuncCtx.Lock()
	sc.UserFuncCtx.StoredFuncName = map[[2]string]*types.FieldType{}
	sc.UserFuncCtx.Unlock()

	arg := &Constant{Value: types.NewIntDatum(1), RetType: types.NewFieldType(mysql.TypeLonglong)}
	expr, err := newFunctionImpl(buildCtx, 0, "test", ast.Upper, types.NewFieldType(mysql.TypeUnspecified), nil, arg)
	require.Nil(t, expr)
	require.ErrorIs(t, err, exeerrors.ErrSpDoesNotExist)
	require.ErrorContains(t, err, "FUNCTION test.upper does not exist")
}
