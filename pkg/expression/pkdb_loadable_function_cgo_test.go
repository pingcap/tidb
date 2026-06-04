//go:build cgo
// +build cgo

// Copyright 2026 PingCAP, Inc.

package expression

import (
	"sync/atomic"
	"testing"
	"unsafe"

	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestLoadableFunctionSoHandleRefCount(t *testing.T) {
	ctx := createContext(t)

	soName := "test_loadable_function_ref_count.so"
	fakeHandle := unsafe.Pointer(uintptr(1))

	udfSoHandleCache.mu.Lock()
	_, existed := udfSoHandleCache.handles[soName]
	inserted := false
	if !existed {
		udfSoHandleCache.handles[soName] = &soHandleRef{handle: fakeHandle, refs: 1}
		inserted = true
	}
	udfSoHandleCache.mu.Unlock()
	require.False(t, existed)

	origDlclose := udfDlclose
	var dlcloseCalls int64
	udfDlclose = func(handle unsafe.Pointer) {
		atomic.AddInt64(&dlcloseCalls, 1)
	}
	defer func() {
		udfDlclose = origDlclose
		if inserted {
			udfSoHandleCache.mu.Lock()
			delete(udfSoHandleCache.handles, soName)
			udfSoHandleCache.mu.Unlock()
		}
	}()

	def := &LoadableFunctionDef{
		name:     "test_udf",
		evalTp:   types.ETInt,
		soName:   soName,
		soHandle: fakeHandle,
	}
	sig := &loadableFuncSig{
		baseBuiltinFunc: baseBuiltinFunc{
			tp: types.NewFieldType(mysql.TypeLonglong),
		},
		def: def,
	}
	require.NoError(t, sig.initRuntime(ctx))
	defer sig.cleanupRuntime()

	def.Drop()
	require.Equal(t, int64(0), atomic.LoadInt64(&dlcloseCalls))
	udfSoHandleCache.mu.Lock()
	entry, ok := udfSoHandleCache.handles[soName]
	udfSoHandleCache.mu.Unlock()
	require.True(t, ok)
	require.Equal(t, 1, entry.refs)

	sig.cleanupRuntime()
	require.Equal(t, int64(1), atomic.LoadInt64(&dlcloseCalls))
	udfSoHandleCache.mu.Lock()
	_, ok = udfSoHandleCache.handles[soName]
	udfSoHandleCache.mu.Unlock()
	require.False(t, ok)
}
