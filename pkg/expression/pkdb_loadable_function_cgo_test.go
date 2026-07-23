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
	fakeHandle := unsafe.Pointer(new(int))
	initialEntry := &soHandleRef{handle: fakeHandle, refs: 1}

	udfSoHandleCache.mu.Lock()
	_, existed := udfSoHandleCache.handles[soName]
	inserted := false
	if !existed {
		udfSoHandleCache.handles[soName] = &soHandleBucket{
			entries:  []*soHandleRef{initialEntry},
			reusable: initialEntry,
		}
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
	udfSoHandleCache.mu.Lock()
	bucket := udfSoHandleCache.handles[soName]
	udfSoHandleCache.mu.Unlock()
	require.NotNil(t, bucket)
	require.Same(t, initialEntry, bucket.reusable)
	require.Equal(t, 2, bucket.reusable.refs)

	def.Drop()
	require.Equal(t, int64(0), atomic.LoadInt64(&dlcloseCalls))
	udfSoHandleCache.mu.Lock()
	bucket = udfSoHandleCache.handles[soName]
	udfSoHandleCache.mu.Unlock()
	require.NotNil(t, bucket)
	var (
		entry *soHandleRef
		ok    bool
	)
	for _, e := range bucket.entries {
		if e.handle == fakeHandle {
			entry = e
			ok = true
			break
		}
	}
	require.True(t, ok)
	require.Equal(t, 1, entry.refs)

	sig.cleanupRuntime()
	require.Equal(t, int64(1), atomic.LoadInt64(&dlcloseCalls))
	udfSoHandleCache.mu.Lock()
	_, ok = udfSoHandleCache.handles[soName]
	udfSoHandleCache.mu.Unlock()
	require.False(t, ok)
}

func TestLoadableFunctionInvalidateSoHandleCache(t *testing.T) {
	soName := "test_loadable_function_reload.so"
	handleV1 := unsafe.Pointer(new(int))
	handleV2 := unsafe.Pointer(new(int))

	origLoadSoHandle := udfLoadSoHandle
	origLoadSymbols := udfLoadSymbols
	origDlclose := udfDlclose
	udfSoHandleCache.mu.Lock()
	origHandles := udfSoHandleCache.handles
	udfSoHandleCache.handles = make(map[string]*soHandleBucket)
	udfSoHandleCache.mu.Unlock()
	t.Cleanup(func() {
		udfLoadSoHandle = origLoadSoHandle
		udfLoadSymbols = origLoadSymbols
		udfDlclose = origDlclose
		udfSoHandleCache.mu.Lock()
		udfSoHandleCache.handles = origHandles
		udfSoHandleCache.mu.Unlock()
	})

	loadCount := 0
	udfLoadSoHandle = func(gotSoName string) (unsafe.Pointer, error) {
		require.Equal(t, soName, gotSoName)
		loadCount++
		if loadCount == 1 {
			return handleV1, nil
		}
		return handleV2, nil
	}
	udfLoadSymbols = func(handle unsafe.Pointer, funcName string) (
		fn unsafe.Pointer,
		fnInit unsafe.Pointer,
		fnDeinit unsafe.Pointer,
		err error,
	) {
		switch handle {
		case handleV1:
			if funcName == "udf_a" {
				return handle, nil, nil, nil
			}
		case handleV2:
			if funcName == "udf_a" || funcName == "udf_b" {
				return handle, nil, nil, nil
			}
		}
		return nil, nil, nil, errCantFindDlEntry.FastGenByArgs(funcName)
	}

	closedHandles := make([]unsafe.Pointer, 0, 2)
	udfDlclose = func(handle unsafe.Pointer) {
		closedHandles = append(closedHandles, handle)
	}

	defA, err := LoadUDF(soName, "udf_a", types.ETInt)
	require.NoError(t, err)
	require.Equal(t, handleV1, defA.soHandle)
	require.Equal(t, 1, loadCount)

	_, err = LoadUDF(soName, "udf_b", types.ETInt)
	require.ErrorContains(t, err, "udf_b")
	require.Equal(t, 1, loadCount)

	InvalidateSoHandleCache(soName)
	udfSoHandleCache.mu.Lock()
	bucket := udfSoHandleCache.handles[soName]
	udfSoHandleCache.mu.Unlock()
	require.NotNil(t, bucket)
	require.Nil(t, bucket.reusable)

	defB, err := LoadUDF(soName, "udf_b", types.ETInt)
	require.NoError(t, err)
	require.Equal(t, handleV2, defB.soHandle)
	require.Equal(t, 2, loadCount)

	udfSoHandleCache.mu.Lock()
	bucket = udfSoHandleCache.handles[soName]
	var entries []*soHandleRef
	var reusable *soHandleRef
	if bucket != nil {
		entries = append([]*soHandleRef(nil), bucket.entries...)
		reusable = bucket.reusable
	}
	udfSoHandleCache.mu.Unlock()
	require.NotNil(t, bucket)
	require.Len(t, entries, 2)
	require.Equal(t, handleV1, entries[0].handle)
	require.Equal(t, handleV2, entries[1].handle)
	require.NotNil(t, reusable)
	require.Equal(t, handleV2, reusable.handle)

	entryByHandle := make(map[unsafe.Pointer]*soHandleRef, len(entries))
	for _, entry := range entries {
		entryByHandle[entry.handle] = entry
	}
	require.Equal(t, 1, entryByHandle[handleV1].refs)
	require.Equal(t, 1, entryByHandle[handleV2].refs)

	reusedHandle, err := acquireSoHandle(soName)
	require.NoError(t, err)
	require.Equal(t, handleV2, reusedHandle)
	require.Equal(t, 2, loadCount)
	releaseSoHandle(soName, reusedHandle)

	defB.Drop()
	require.Equal(t, []unsafe.Pointer{handleV2}, closedHandles)

	defA.Drop()
	require.Equal(t, []unsafe.Pointer{handleV2, handleV1}, closedHandles)

	udfSoHandleCache.mu.Lock()
	_, ok := udfSoHandleCache.handles[soName]
	udfSoHandleCache.mu.Unlock()
	require.False(t, ok)
}
