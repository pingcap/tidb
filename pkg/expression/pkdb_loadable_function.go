//go:build cgo
// +build cgo

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

package expression

/*
#cgo CFLAGS: -I${SRCDIR}/loadable_function/include/mysql/
#cgo LDFLAGS: -ldl
#include <dlfcn.h>
#include <stdlib.h>
#include <string.h>
#include "udf_registration_types.h"

bool call_udf_func_init(void *handle, UDF_INIT *initid, UDF_ARGS *args, char *message) {
  return ((udf_init)handle)(initid, args, message);
}

void call_udf_func_deinit(void *handle, UDF_INIT *initid) {
  ((udf_deinit)handle)(initid);
}

long long call_udf_func_longlong(void *handle, UDF_INIT *initid, UDF_ARGS *args, unsigned char *is_null, unsigned char *error) {
  return ((udf_longlong)handle)(initid, args, is_null, error);
}

double call_udf_func_double(void *handle, UDF_INIT *initid, UDF_ARGS *args, unsigned char *is_null, unsigned char *error) {
  return ((udf_double)handle)(initid, args, is_null, error);
}

char *call_udf_func_string(void *handle, UDF_INIT *initid, UDF_ARGS *args, char *result, unsigned long *length, unsigned char *is_null, unsigned char *error) {
  return ((udf_string)handle)(initid, args, result, length, is_null, error);
}

// ---- Test-only mock UDF symbols ----
//
// These symbols are linked into the tidb test binary and are used by
// MockLoadUDFForTest to cover the SQL flow without compiling a real shared
// object (.so). They are intentionally simple and deterministic.
static int mock_udf_ll_init_calls = 0;
static int mock_udf_ll_deinit_calls = 0;
static int mock_udf_ll_calls = 0;

bool mock_udf_ll_init(UDF_INIT *initid, UDF_ARGS *args, char *message) {
  __sync_add_and_fetch(&mock_udf_ll_init_calls, 1);
  (void)initid;
  (void)args;
  if (message != NULL) {
    message[0] = 0;
  }
  return false;
}

void mock_udf_ll_deinit(UDF_INIT *initid) {
  __sync_add_and_fetch(&mock_udf_ll_deinit_calls, 1);
  (void)initid;
}

long long mock_udf_ll(UDF_INIT *initid, UDF_ARGS *args, unsigned char *is_null, unsigned char *error) {
  __sync_add_and_fetch(&mock_udf_ll_calls, 1);
  (void)initid;
  if (is_null != NULL) {
    *is_null = 0;
  }
  if (error != NULL) {
    *error = 0;
  }
  if (args == NULL || args->arg_count == 0 || args->args == NULL || args->args[0] == NULL) {
    if (is_null != NULL) {
      *is_null = 1;
    }
    return 0;
  }
  // For INT_RESULT args, TiDB passes a pointer to long long.
  return *((long long*)args->args[0]) + 1;
}

bool mock_udf_init_fail(UDF_INIT *initid, UDF_ARGS *args, char *message) {
  (void)initid;
  (void)args;
  if (message != NULL) {
    const char *msg = "mock init failure";
    strncpy(message, msg, 255);
    message[255] = 0;
  }
  return true;
}

void* mock_udf_ll_ptr() { return (void*)mock_udf_ll; }
void* mock_udf_ll_init_ptr() { return (void*)mock_udf_ll_init; }
void* mock_udf_ll_deinit_ptr() { return (void*)mock_udf_ll_deinit; }
void* mock_udf_init_fail_ptr() { return (void*)mock_udf_init_fail; }

void mock_udf_reset_counts() {
  mock_udf_ll_init_calls = 0;
  mock_udf_ll_deinit_calls = 0;
  mock_udf_ll_calls = 0;
}

int mock_udf_get_ll_init_calls() { return mock_udf_ll_init_calls; }
int mock_udf_get_ll_deinit_calls() { return mock_udf_ll_deinit_calls; }
int mock_udf_get_ll_calls() { return mock_udf_ll_calls; }
*/
import "C"

import (
	"math"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/expression/expropt"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

var (
	errCantOpenLibrary   = dbterror.ClassExpression.NewStd(errno.ErrCantOpenLibrary)
	errCantFindDlEntry   = dbterror.ClassExpression.NewStd(errno.ErrCantFindDlEntry)
	errCantInitializeUdf = dbterror.ClassExpression.NewStd(errno.ErrCantInitializeUdf)
	errUdfNoPaths        = dbterror.ClassExpression.NewStd(errno.ErrUdfNoPaths)
	// ErrNativeFctNameCollision indicates a native function name collision error.
	ErrNativeFctNameCollision = dbterror.ClassExpression.NewStd(mysql.ErrNativeFctNameCollision)
)

const udfInitMessageSize = 256

// udfDlclose is a wrapper around C.dlclose to allow mocking in tests.
var udfDlclose = func(handle unsafe.Pointer) {
	C.dlclose(handle)
}

type loadUDFHookForTestFn func(soName, funcName string, funcRetType types.EvalType) (*LoadableFunctionDef, error)

var loadUDFHookForTest atomic.Pointer[loadUDFHookForTestFn]

// SetLoadUDFHookForTest overrides LoadUDF behavior in unit tests.
// It returns a restore function that should be deferred by the caller.
//
// NOTE: This is a process-global hook. Tests that rely on it must not run in parallel.
func SetLoadUDFHookForTest(hook loadUDFHookForTestFn) (restore func()) {
	old := loadUDFHookForTest.Load()
	loadUDFHookForTest.Store(&hook)
	return func() { loadUDFHookForTest.Store(old) }
}

type soHandleRef struct {
	handle unsafe.Pointer
	refs   int
}

type soHandleCache struct {
	mu      sync.Mutex
	handles map[string]*soHandleRef
}

// loadableFuncs stores the loadable functions created by CREATE FUNCTION ... SONAME ...
// The key is the lowercase function name, and the value is *loadableFuncClass.
var (
	loadableFuncs    sync.Map
	udfSoHandleCache = soHandleCache{handles: make(map[string]*soHandleRef)}
)

// LoadableFunctionDef is the definition for a loadable function.
type LoadableFunctionDef struct {
	// name is the function's name.
	name string
	// evalTp is the type of the return value.
	evalTp types.EvalType
	// soName is the name of the shared object file.
	soName string
	// soHandle is the handle of the shared object file.
	soHandle unsafe.Pointer
	// fn is the loadable function pointer to the symbol in the shared object file.
	fn unsafe.Pointer
	// fnInit is the initialization function pointer to the symbol.
	fnInit unsafe.Pointer
	// fnDeinit is the deinitialization function pointer to the symbol.
	fnDeinit unsafe.Pointer
}

// LoadUDF loads a loadable function from the shared object file.
func LoadUDF(soName, funcName string, funcRetType types.EvalType) (def *LoadableFunctionDef, errRet error) {
	if intest.InTest {
		if hook := loadUDFHookForTest.Load(); hook != nil {
			logutil.BgLogger().Info("lance test hook not nil")
			return (*hook)(soName, funcName, funcRetType)
		}
		logutil.BgLogger().Info("lance test hook nil")
	}

	handle, err := acquireSoHandle(soName)
	if err != nil {
		return nil, err
	}
	defer func() {
		if errRet != nil {
			releaseSoHandle(soName, handle)
		}
	}()

	fn, fnInit, fnDeinit, err := loadSymbols(handle, funcName)
	if err != nil {
		return nil, err
	}

	funcDef := &LoadableFunctionDef{
		name:     funcName,
		evalTp:   funcRetType,
		soName:   soName,
		soHandle: handle,
		fn:       fn,
		fnInit:   fnInit,
		fnDeinit: fnDeinit,
	}

	return funcDef, nil
}

// MockLoadUDFForTest is a deterministic mock implementation of LoadUDF for unit
// tests, which add 1 to input number. It bypasses dlopen/dlsym and returns
// in-process mock symbols.
//
// The mock supports basic error injection by function name / soName:
// - soName contains path separators -> ErrUdfNoPaths (same as production validateSoName)
// - soName == "missing.so" -> ErrCantOpenLibrary
// - funcName contains "missing_symbol" -> ErrCantFindDlEntry
// - funcName contains "init_fail" -> init returns error (ErrCantInitializeUdf) when the function is evaluated
func MockLoadUDFForTest(soName, funcName string, funcRetType types.EvalType) (*LoadableFunctionDef, error) {
	if err := validateSoName(soName); err != nil {
		return nil, err
	}
	if soName == "missing.so" {
		return nil, errCantOpenLibrary.FastGenByArgs(soName, 2, "mock missing shared library")
	}
	if strings.Contains(funcName, "missing_symbol") {
		return nil, errCantFindDlEntry.FastGenByArgs(funcName)
	}

	def := &LoadableFunctionDef{
		name:   funcName,
		evalTp: funcRetType,
		soName: soName,
		// Keep soHandle nil so Drop() becomes a no-op in unit tests.
		// Real dlopen/dlclose coverage is intentionally left to later PRs.
		soHandle: nil,
	}
	switch funcRetType {
	case types.ETInt:
		def.fn = C.mock_udf_ll_ptr()
	default:
		return nil, errors.Errorf("unsupported mock UDF return type: %v", funcRetType)
	}
	if strings.Contains(funcName, "init_fail") {
		def.fnInit = C.mock_udf_init_fail_ptr()
	} else {
		def.fnInit = C.mock_udf_ll_init_ptr()
	}
	def.fnDeinit = C.mock_udf_ll_deinit_ptr()
	return def, nil
}

// ResetMockUDFCountersForTest resets counters used by MockLoadUDFForTest.
func ResetMockUDFCountersForTest() {
	C.mock_udf_reset_counts()
}

// MockUDFCountersForTest returns counters (initCalls, deinitCalls, callCalls) used by MockLoadUDFForTest.
func MockUDFCountersForTest() (int, int, int) {
	return int(C.mock_udf_get_ll_init_calls()), int(C.mock_udf_get_ll_deinit_calls()), int(C.mock_udf_get_ll_calls())
}

func validateSoName(soName string) error {
	if strings.ContainsAny(soName, `/\`) {
		return errUdfNoPaths.FastGenByArgs()
	}
	return nil
}

func acquireSoHandle(soName string) (handle unsafe.Pointer, errRet error) {
	if err := validateSoName(soName); err != nil {
		return nil, err
	}
	udfSoHandleCache.mu.Lock()
	if entry, ok := udfSoHandleCache.handles[soName]; ok {
		entry.refs++
		handle = entry.handle
		udfSoHandleCache.mu.Unlock()
		return handle, nil
	}
	handle, errRet = loadSoHandle(soName)
	if errRet != nil {
		udfSoHandleCache.mu.Unlock()
		return nil, errRet
	}
	udfSoHandleCache.handles[soName] = &soHandleRef{handle: handle, refs: 1}
	udfSoHandleCache.mu.Unlock()
	return handle, nil
}

func releaseSoHandle(soName string, handle unsafe.Pointer) {
	if handle == nil {
		return
	}
	var toClose unsafe.Pointer
	udfSoHandleCache.mu.Lock()
	entry, ok := udfSoHandleCache.handles[soName]
	if !ok {
		udfSoHandleCache.mu.Unlock()
		logutil.BgLogger().Warn("UDF handle cache miss on release", zap.String("soName", soName))
		return
	}
	entry.refs--
	if entry.refs <= 0 {
		delete(udfSoHandleCache.handles, soName)
		toClose = entry.handle
	}
	udfSoHandleCache.mu.Unlock()
	if toClose != nil {
		udfDlclose(toClose)
	}
}

func loadSoHandle(soName string) (handle unsafe.Pointer, errRet error) {
	pluginDir := config.GetGlobalConfig().Instance.PluginDir
	if pluginDir == "" {
		return nil, errCantOpenLibrary.FastGenByArgs(soName, 2, "plugin-dir is not configured")
	}
	if !filepath.IsAbs(pluginDir) {
		return nil, errCantOpenLibrary.FastGenByArgs(soName, 2, "plugin-dir must be an absolute path")
	}
	path := filepath.Join(pluginDir, soName)

	cPath := C.CString(path)
	defer C.free(unsafe.Pointer(cPath))
	handle = C.dlopen(cPath, C.RTLD_NOW)
	// always call dlerror() to clear the error
	// https://man7.org/linux/man-pages/man3/dlsym.3.html#DESCRIPTION
	errMsg := C.GoString(C.dlerror())
	if handle == nil {
		// In Linux, dlerror() is thread-safe, so we don't need to lock.
		// https://man7.org/linux/man-pages/man3/dlerror.3.html#ATTRIBUTES
		logutil.BgLogger().Info("lance test 1", zap.Stack("stack"))
		return nil, errCantOpenLibrary.FastGenByArgs(soName, 2, errMsg)
	}
	return handle, nil
}

func loadSymbols(handle unsafe.Pointer, funcName string) (
	fn unsafe.Pointer,
	fnInit unsafe.Pointer,
	fnDeinit unsafe.Pointer,
	err error,
) {
	fn, errMsg := loadSymbol(handle, funcName)
	if errMsg != "" {
		logutil.BgLogger().Error("load symbol (dlsym) failed",
			zap.String("funcName", funcName),
			zap.String("errMsg", errMsg))
		return nil, nil, nil, errCantFindDlEntry.FastGenByArgs(funcName)
	}

	fnInit, _ = loadSymbol(handle, funcName+"_init")
	fnDeinit, _ = loadSymbol(handle, funcName+"_deinit")
	return fn, fnInit, fnDeinit, nil
}

func loadSymbol(handle unsafe.Pointer, funcName string) (fn unsafe.Pointer, errMsg string) {
	cFuncName := C.CString(funcName)
	defer C.free(unsafe.Pointer(cFuncName))
	fn = C.dlsym(handle, cFuncName)
	errMsg = C.GoString(C.dlerror())
	return fn, errMsg
}

// Drop releases the shared object handle.
func (d *LoadableFunctionDef) Drop() {
	if d.soHandle != nil {
		releaseSoHandle(d.soName, d.soHandle)
		d.soHandle = nil
	}
}

// ValidateLoadableFunctionDef validates whether the loadable function definition
// can be registered as a UDF (for example, native name collision / return type).
//
// It does not mutate the global loadable function registry.
func ValidateLoadableFunctionDef(def *LoadableFunctionDef) error {
	_, err := createFuncClassFromDef(def)
	return err
}

func createFuncClassFromDef(def *LoadableFunctionDef) (*loadableFuncClass, error) {
	lowerName := strings.ToLower(def.name)
	if nameConflictsWithNativeOrExtensionFunc(lowerName) {
		return nil, ErrNativeFctNameCollision.FastGenByArgs(def.name)
	}

	var flen int
	switch def.evalTp {
	case types.ETString:
		flen = mysql.MaxFieldVarCharLength
	case types.ETInt:
		flen = mysql.MaxIntWidth
	case types.ETReal:
		flen = mysql.MaxRealWidth
	case types.ETDecimal:
		flen = mysql.MaxDecimalWidth
	default:
		return nil, errors.Errorf("unsupported extension function ret type: '%v'", def.evalTp)
	}

	return &loadableFuncClass{
		baseFunctionClass: baseFunctionClass{def.name, math.MaxInt, -1},
		flen:              flen,
		funcDef:           def,
	}, nil
}

func (c *loadableFuncClass) getFunction(
	ctx BuildContext,
	args []Expression,
) (f builtinFunc, errRet error) {
	if _, err := c.GetPrivilegeChecker(ctx.GetEvalCtx()); err != nil {
		return nil, err
	}
	// MySQL does not gate loadable UDF invocation with routine-level/global EXECUTE.

	// TODO: mimic newBaseBuiltinFuncWithTp when types needs to be coerced.
	var ft byte
	switch c.funcDef.evalTp {
	case types.ETString:
		ft = mysql.TypeString
	case types.ETInt:
		ft = mysql.TypeLonglong
	case types.ETReal:
		ft = mysql.TypeDouble
	case types.ETDecimal:
		ft = mysql.TypeNewDecimal
	}
	tp := types.NewFieldType(ft)
	tp.SetFlen(c.flen)
	if c.funcDef.evalTp == types.ETDecimal {
		tp.SetDecimal(types.UnspecifiedLength)
	}
	bf, err := newBaseBuiltinFunc(ctx, c.funcName, args, tp)
	if err != nil {
		return nil, err
	}
	// Skip plan cache for loadable functions because there is no strong requirement
	// to do it, and skipping it makes the behavior simple.
	ctx.SetSkipPlanCache("loadable function should not be cached")
	sig := &loadableFuncSig{
		baseBuiltinFunc: bf,
		def:             c.funcDef,
	}

	if err := sig.initRuntime(ctx.GetEvalCtx()); err != nil {
		return nil, err
	}
	if err := sig.registerCleanup(ctx.GetEvalCtx()); err != nil {
		sig.cleanupRuntime()
		return nil, err
	}
	return sig, nil
}

type loadableFuncSig struct {
	baseBuiltinFunc
	expropt.StmtCleanupPropReader

	def     *LoadableFunctionDef
	runtime udfRuntime
}

type udfConstArg struct {
	isConst bool
	datum   types.Datum
	argType types.EvalType
	ptr     unsafe.Pointer
	length  C.ulong
}

type udfRuntime struct {
	ptrUDFInit        *C.UDF_INIT
	ptrUDFArgs        *C.UDF_ARGS
	soHandle          unsafe.Pointer
	argTypes          []types.EvalType
	constArgs         []udfConstArg
	resultBuf         unsafe.Pointer
	resultBufLen      C.ulong
	freeUDFInit       func()
	freeUDFArgs       func()
	initialized       bool
	initCalled        bool
	cleanupRegistered bool
	cleaned           bool
}

func (b *loadableFuncSig) Clone() builtinFunc {
	newSig := &loadableFuncSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	newSig.def = b.def
	// Runtime contains C pointers and must never be shared across clones.
	newSig.runtime = udfRuntime{}
	return newSig
}

// RequiredOptionalEvalProps implements the RequireOptionalEvalProps interface.
func (b *loadableFuncSig) RequiredOptionalEvalProps() OptionalEvalPropKeySet {
	return b.StmtCleanupPropReader.RequiredOptionalEvalProps()
}

func (b *loadableFuncSig) ensureInitialized(ctx EvalContext) error {
	if !b.runtime.initialized {
		if err := b.initRuntime(ctx); err != nil {
			return err
		}
	}
	if !b.runtime.cleanupRegistered {
		if err := b.registerCleanup(ctx); err != nil {
			b.cleanupRuntime()
			return err
		}
	}
	return nil
}

func (b *loadableFuncSig) initRuntime(ctx EvalContext) (errRet error) {
	if b.runtime.initialized {
		return nil
	}
	b.runtime.cleaned = false

	if intest.InTest {
		if hook := loadUDFHookForTest.Load(); hook == nil {
			soHandle, err := acquireSoHandle(b.def.soName)
			if err != nil {
				return err
			}
			b.runtime.soHandle = soHandle
		}
	}

	defer func() {
		if errRet != nil && b.runtime.soHandle != nil {
			releaseSoHandle(b.def.soName, b.runtime.soHandle)
			b.runtime.soHandle = nil
		}
	}()

	ptrUDFInit, freeUDFInit, err := newUDFInit()
	if err != nil {
		return err
	}
	defer func() {
		if errRet != nil {
			freeUDFInit()
		}
	}()

	ptrUDFArgs, freeUDFArgs, err := newUDFArgs(getArgFieldTypes(ctx, b.args))
	if err != nil {
		return err
	}
	defer func() {
		if errRet != nil {
			freeUDFArgs()
		}
	}()

	b.runtime.ptrUDFInit = ptrUDFInit
	b.runtime.ptrUDFArgs = ptrUDFArgs
	b.runtime.freeUDFInit = freeUDFInit
	b.runtime.freeUDFArgs = freeUDFArgs

	argCount := len(b.args)
	initialArgTypes := readUDFArgTypes(ptrUDFArgs, argCount)
	if err := b.setConstArgs(ctx, initialArgTypes); err != nil {
		return err
	}

	if b.def.fnInit != nil {
		message := (*C.char)(C.calloc(udfInitMessageSize, 1))
		if message == nil {
			return errors.New("failed to allocate message buffer for UDF init")
		}
		defer C.free(unsafe.Pointer(message))
		if C.call_udf_func_init(b.def.fnInit, ptrUDFInit, ptrUDFArgs, message) {
			errMsg := C.GoString(message)
			b.freeConstArgs()
			return errCantInitializeUdf.FastGenByArgs(b.def.name, errMsg)
		}
		b.runtime.initCalled = true
	}

	b.runtime.argTypes = readUDFArgTypes(ptrUDFArgs, argCount)
	if err := b.refreshConstArgs(ctx, b.runtime.argTypes); err != nil {
		b.freeConstArgs()
		return err
	}

	b.applyInitToReturnType()
	b.runtime.initialized = true
	return nil
}

func (b *loadableFuncSig) registerCleanup(ctx EvalContext) error {
	if err := b.StmtCleanupPropReader.RegisterCleanup(ctx, b.cleanupRuntime); err != nil {
		return err
	}
	b.runtime.cleanupRegistered = true
	return nil
}

func (b *loadableFuncSig) cleanupRuntime() {
	if b.runtime.cleaned {
		return
	}
	b.runtime.cleaned = true
	if b.runtime.ptrUDFInit != nil && b.def.fnDeinit != nil && (b.runtime.initCalled || b.def.fnInit == nil) {
		C.call_udf_func_deinit(b.def.fnDeinit, b.runtime.ptrUDFInit)
	}
	for i := range b.runtime.constArgs {
		if b.runtime.constArgs[i].ptr != nil {
			C.free(b.runtime.constArgs[i].ptr)
			b.runtime.constArgs[i].ptr = nil
		}
	}
	if b.runtime.resultBuf != nil {
		C.free(b.runtime.resultBuf)
		b.runtime.resultBuf = nil
	}
	if b.runtime.freeUDFArgs != nil {
		b.runtime.freeUDFArgs()
		b.runtime.freeUDFArgs = nil
	}
	if b.runtime.freeUDFInit != nil {
		b.runtime.freeUDFInit()
		b.runtime.freeUDFInit = nil
	}
	b.runtime.ptrUDFArgs = nil
	b.runtime.ptrUDFInit = nil
	b.runtime.argTypes = nil
	b.runtime.constArgs = nil
	b.runtime.resultBufLen = 0
	b.runtime.initialized = false
	b.runtime.initCalled = false
	b.runtime.cleanupRegistered = false

	if b.runtime.soHandle != nil {
		releaseSoHandle(b.def.soName, b.runtime.soHandle)
		b.runtime.soHandle = nil
	}
}

func (b *loadableFuncSig) freeConstArgs() {
	for i := range b.runtime.constArgs {
		if b.runtime.constArgs[i].ptr != nil {
			C.free(b.runtime.constArgs[i].ptr)
			b.runtime.constArgs[i].ptr = nil
		}
	}
	b.runtime.constArgs = nil
}

func (b *loadableFuncSig) setConstArgs(ctx EvalContext, argTypes []types.EvalType) error {
	argCount := len(b.args)
	if argCount == 0 {
		b.runtime.constArgs = nil
		return nil
	}
	constArgs := make([]udfConstArg, argCount)
	argsBuf := (*[1 << 30]*C.char)(unsafe.Pointer((*b.runtime.ptrUDFArgs).args))[:argCount:argCount]
	lengthsBuf := (*[1 << 30]C.ulong)(unsafe.Pointer((*b.runtime.ptrUDFArgs).lengths))[:argCount:argCount]
	for i, arg := range b.args {
		datum, ok := constArgDatum(arg)
		if !ok {
			continue
		}
		ptr, length, err := datumToUDFArg(ctx, datum, argTypes[i])
		if err != nil {
			for j := range constArgs {
				if constArgs[j].ptr != nil {
					C.free(constArgs[j].ptr)
				}
			}
			return err
		}
		constArgs[i] = udfConstArg{
			isConst: true,
			datum:   datum,
			argType: argTypes[i],
			ptr:     ptr,
			length:  length,
		}
		argsBuf[i] = (*C.char)(ptr)
		lengthsBuf[i] = length
	}
	b.runtime.constArgs = constArgs
	return nil
}

func (b *loadableFuncSig) refreshConstArgs(ctx EvalContext, argTypes []types.EvalType) error {
	if len(b.runtime.constArgs) == 0 {
		return nil
	}
	argCount := len(b.runtime.constArgs)
	argsBuf := (*[1 << 30]*C.char)(unsafe.Pointer((*b.runtime.ptrUDFArgs).args))[:argCount:argCount]
	lengthsBuf := (*[1 << 30]C.ulong)(unsafe.Pointer((*b.runtime.ptrUDFArgs).lengths))[:argCount:argCount]
	for i := range b.runtime.constArgs {
		if !b.runtime.constArgs[i].isConst {
			continue
		}
		if b.runtime.constArgs[i].argType != argTypes[i] {
			if b.runtime.constArgs[i].ptr != nil {
				C.free(b.runtime.constArgs[i].ptr)
			}
			ptr, length, err := datumToUDFArg(ctx, b.runtime.constArgs[i].datum, argTypes[i])
			if err != nil {
				return err
			}
			b.runtime.constArgs[i].ptr = ptr
			b.runtime.constArgs[i].length = length
			b.runtime.constArgs[i].argType = argTypes[i]
		}
		argsBuf[i] = (*C.char)(b.runtime.constArgs[i].ptr)
		lengthsBuf[i] = b.runtime.constArgs[i].length
	}
	return nil
}

func (b *loadableFuncSig) applyInitToReturnType() {
	if b.runtime.ptrUDFInit == nil {
		return
	}
	init := b.runtime.ptrUDFInit
	if init.maybe_null {
		b.tp.DelFlag(mysql.NotNullFlag)
	} else {
		b.tp.AddFlag(mysql.NotNullFlag)
	}
	if init.max_length != 0 && (b.tp.GetFlen() == types.UnspecifiedLength || int(init.max_length) < b.tp.GetFlen()) {
		b.tp.SetFlen(int(init.max_length))
	}
	if (b.def.evalTp == types.ETReal || b.def.evalTp == types.ETDecimal) && (init.decimals != 0 || b.tp.GetDecimal() == types.UnspecifiedLength) {
		b.tp.SetDecimal(int(init.decimals))
	}
}

func (b *loadableFuncSig) setUDFArgs(ctx EvalContext, row chunk.Row) (cleanup func(), err error) {
	argCount := len(b.args)
	if argCount == 0 {
		return func() {}, nil
	}
	argsBuf := (*[1 << 30]*C.char)(unsafe.Pointer((*b.runtime.ptrUDFArgs).args))[:argCount:argCount]
	lengthsBuf := (*[1 << 30]C.ulong)(unsafe.Pointer((*b.runtime.ptrUDFArgs).lengths))[:argCount:argCount]
	toFree := make([]unsafe.Pointer, 0, argCount)
	for i, arg := range b.args {
		if i < len(b.runtime.constArgs) && b.runtime.constArgs[i].isConst {
			argsBuf[i] = (*C.char)(b.runtime.constArgs[i].ptr)
			lengthsBuf[i] = b.runtime.constArgs[i].length
			continue
		}
		val, evalErr := arg.Eval(ctx, row)
		if evalErr != nil {
			for _, ptr := range toFree {
				C.free(ptr)
			}
			return nil, evalErr
		}
		ptr, length, convErr := datumToUDFArg(ctx, val, b.runtime.argTypes[i])
		if convErr != nil {
			for _, ptr := range toFree {
				C.free(ptr)
			}
			return nil, convErr
		}
		argsBuf[i] = (*C.char)(ptr)
		lengthsBuf[i] = length
		if ptr != nil {
			toFree = append(toFree, ptr)
		}
	}
	return func() {
		for _, ptr := range toFree {
			C.free(ptr)
		}
	}, nil
}

func (b *loadableFuncSig) ensureResultBuffer() error {
	if b.runtime.resultBuf != nil {
		return nil
	}
	maxLen := int64(b.tp.GetFlen())
	if b.runtime.ptrUDFInit != nil && b.runtime.ptrUDFInit.max_length != 0 {
		maxLen = int64(b.runtime.ptrUDFInit.max_length)
	}
	if maxLen <= 0 {
		maxLen = int64(mysql.MaxFieldVarCharLength)
	}
	if maxLen > int64(^uint(0)>>1) {
		return errors.New("result buffer size is too large for UDF")
	}
	buf := C.malloc(C.size_t(maxLen + 1))
	if buf == nil {
		return errors.New("failed to allocate UDF result buffer")
	}
	b.runtime.resultBuf = buf
	b.runtime.resultBufLen = C.ulong(maxLen)
	return nil
}

func constArgDatum(arg Expression) (types.Datum, bool) {
	con, ok := arg.(*Constant)
	if !ok {
		return types.Datum{}, false
	}
	if con.ParamMarker != nil || con.DeferredExpr != nil {
		return types.Datum{}, false
	}
	return con.Value, true
}

func readUDFArgTypes(ptrUDFArgs *C.UDF_ARGS, argCount int) []types.EvalType {
	if argCount == 0 || ptrUDFArgs == nil {
		return nil
	}
	goArgsTypeBuf := (*[1 << 30]C.enum_Item_result)(unsafe.Pointer((*ptrUDFArgs).arg_type))[:argCount:argCount]
	result := make([]types.EvalType, argCount)
	for i, argType := range goArgsTypeBuf {
		result[i] = CastUDFArgTypeIntToEvalType(int(argType))
	}
	return result
}

func datumToUDFArg(ctx EvalContext, datum types.Datum, evalTp types.EvalType) (unsafe.Pointer, C.ulong, error) {
	if datum.IsNull() {
		return nil, 0, nil
	}
	typeCtx := types.DefaultStmtNoWarningContext
	if ctx != nil {
		typeCtx = ctx.TypeCtx()
	}
	switch evalTp {
	case types.ETInt:
		val, err := datum.ToInt64(typeCtx)
		if err != nil {
			return nil, 0, err
		}
		ptr := C.malloc(C.sizeof_longlong)
		if ptr == nil {
			return nil, 0, errors.New("failed to allocate UDF int argument")
		}
		*(*C.longlong)(ptr) = C.longlong(val)
		return ptr, 0, nil
	case types.ETReal:
		val, err := datum.ToFloat64(typeCtx)
		if err != nil {
			return nil, 0, err
		}
		ptr := C.malloc(C.sizeof_double)
		if ptr == nil {
			return nil, 0, errors.New("failed to allocate UDF real argument")
		}
		*(*C.double)(ptr) = C.double(val)
		return ptr, 0, nil
	case types.ETDecimal:
		dec, err := datum.ToDecimal(typeCtx)
		if err != nil {
			return nil, 0, err
		}
		return allocUDFBytes([]byte(dec.String()))
	case types.ETString:
		if datum.Kind() == types.KindString || datum.Kind() == types.KindBytes {
			return allocUDFBytes(datum.GetBytes())
		}
		str, err := datum.ToString()
		if err != nil {
			return nil, 0, err
		}
		return allocUDFBytes([]byte(str))
	default:
		return nil, 0, errors.Errorf("unsupported UDF argument type: %v", evalTp)
	}
}

func allocUDFBytes(b []byte) (unsafe.Pointer, C.ulong, error) {
	size := len(b) + 1
	ptr := C.malloc(C.size_t(size))
	if ptr == nil {
		return nil, 0, errors.New("failed to allocate UDF string argument")
	}
	buf := (*[1 << 30]byte)(ptr)[:size:size]
	if len(b) > 0 {
		copy(buf[:len(b):len(b)], b)
	}
	buf[len(b)] = 0
	return ptr, C.ulong(len(b)), nil
}

func (b *loadableFuncSig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	if b.def.evalTp == types.ETString || b.def.evalTp == types.ETDecimal {
		return b.evalStringUDF(ctx, row)
	}
	return b.baseBuiltinFunc.evalString(ctx, row)
}

func (b *loadableFuncSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	if b.def.evalTp == types.ETInt {
		return b.evalIntUDF(ctx, row)
	}
	return b.baseBuiltinFunc.evalInt(ctx, row)
}

func (b *loadableFuncSig) evalIntUDF(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	if err := b.ensureInitialized(ctx); err != nil {
		return 0, false, err
	}
	cleanup, err := b.setUDFArgs(ctx, row)
	if err != nil {
		return 0, false, err
	}
	defer cleanup()

	var isNull C.uchar
	var errFlag C.uchar
	ret := C.call_udf_func_longlong(b.def.fn, b.runtime.ptrUDFInit, b.runtime.ptrUDFArgs, &isNull, &errFlag)
	if errFlag != 0 {
		return 0, false, errors.Errorf("UDF %s execution error", b.def.name)
	}
	if isNull != 0 {
		return 0, true, nil
	}
	return int64(ret), false, nil
}

func (b *loadableFuncSig) evalReal(ctx EvalContext, row chunk.Row) (val float64, isNull bool, err error) {
	if b.def.evalTp == types.ETReal {
		return b.evalRealUDF(ctx, row)
	}
	return b.baseBuiltinFunc.evalReal(ctx, row)
}

func (b *loadableFuncSig) evalRealUDF(ctx EvalContext, row chunk.Row) (float64, bool, error) {
	if err := b.ensureInitialized(ctx); err != nil {
		return 0, false, err
	}
	cleanup, err := b.setUDFArgs(ctx, row)
	if err != nil {
		return 0, false, err
	}
	defer cleanup()

	var isNull C.uchar
	var errFlag C.uchar
	ret := C.call_udf_func_double(b.def.fn, b.runtime.ptrUDFInit, b.runtime.ptrUDFArgs, &isNull, &errFlag)
	if errFlag != 0 {
		return 0, false, errors.Errorf("UDF %s execution error", b.def.name)
	}
	if isNull != 0 {
		return 0, true, nil
	}
	return float64(ret), false, nil
}

func (b *loadableFuncSig) evalStringUDF(ctx EvalContext, row chunk.Row) (string, bool, error) {
	if err := b.ensureInitialized(ctx); err != nil {
		return "", false, err
	}
	if err := b.ensureResultBuffer(); err != nil {
		return "", false, err
	}
	cleanup, err := b.setUDFArgs(ctx, row)
	if err != nil {
		return "", false, err
	}
	defer cleanup()

	var isNull C.uchar
	var errFlag C.uchar
	resLen := b.runtime.resultBufLen
	retPtr := C.call_udf_func_string(
		b.def.fn,
		b.runtime.ptrUDFInit,
		b.runtime.ptrUDFArgs,
		(*C.char)(b.runtime.resultBuf),
		&resLen,
		&isNull,
		&errFlag,
	)
	if errFlag != 0 {
		return "", false, errors.Errorf("UDF %s execution error", b.def.name)
	}
	if isNull != 0 {
		return "", true, nil
	}
	if retPtr == nil || resLen == 0 {
		return "", false, nil
	}
	if resLen > C.ulong(^uint(0)>>1) {
		return "", false, errors.New("UDF result is too large")
	}
	data := C.GoBytes(unsafe.Pointer(retPtr), C.int(resLen))
	return string(data), false, nil
}

func (b *loadableFuncSig) evalDecimal(ctx EvalContext, row chunk.Row) (*types.MyDecimal, bool, error) {
	if b.def.evalTp == types.ETDecimal {
		str, isNull, err := b.evalStringUDF(ctx, row)
		if err != nil || isNull {
			return nil, isNull, err
		}
		datum := types.NewStringDatum(str)
		dec, err := datum.ToDecimal(ctx.TypeCtx())
		if err != nil {
			return nil, false, err
		}
		return dec, false, nil
	}
	return b.baseBuiltinFunc.evalDecimal(ctx, row)
}

func newUDFInit() (ptrUDFInit *C.UDF_INIT, cleanup func(), err error) {
	ptr := C.calloc(1, C.sizeof_UDF_INIT)
	if ptr == nil {
		return nil, nil, errors.New("failed to allocate memory in newUDFInit")
	}
	return (*C.UDF_INIT)(ptr), func() {
		C.free(ptr)
	}, nil
}

// Ref https://dev.mysql.com/doc/extending-mysql/8.4/en/adding-loadable-function.html#loadable-function-arguments
func newUDFArgs(argTypes []*types.FieldType) (ptrUDFArgs *C.UDF_ARGS, cleanup func(), errRet error) {
	ptr := C.calloc(1, C.sizeof_UDF_ARGS)
	if ptr == nil {
		return nil, nil, errors.New("failed to allocate memory in newUDFArgs")
	}
	defer func() {
		if errRet != nil {
			C.free(ptr)
		}
	}()

	ptrUDFArgs = (*C.UDF_ARGS)(ptr)
	(*ptrUDFArgs).arg_count = C.uint(len(argTypes))

	argsTypeBuf := C.calloc(C.size_t(len(argTypes)), C.sizeof_enum_Item_result)
	goArgsTypeBuf := (*[1 << 30]C.enum_Item_result)(argsTypeBuf)[:len(argTypes):len(argTypes)]
	(*ptrUDFArgs).arg_type = (*C.enum_Item_result)(argsTypeBuf)
	defer func() {
		if errRet != nil {
			C.free(argsTypeBuf)
		}
	}()
	for i, argType := range argTypes {
		t, err := castFieldTypeToUDFArgType(argType)
		if err != nil {
			logutil.BgLogger().Error("failed to cast FieldType to UDF arg type",
				zap.Int("argIndex", i),
				zap.Error(err))
			return nil, nil, err
		}
		goArgsTypeBuf[i] = t
	}

	argsBuf := C.calloc(C.size_t(len(argTypes)), C.size_t(unsafe.Sizeof((*C.char)(nil))))
	(*ptrUDFArgs).args = (**C.char)(argsBuf)

	lengthBuf := C.calloc(C.size_t(len(argTypes)), C.sizeof_ulong)
	(*ptrUDFArgs).lengths = (*C.ulong)(lengthBuf)

	maybeNullBuf := C.calloc(C.size_t(len(argTypes)), C.size_t(1))
	(*ptrUDFArgs).maybe_null = (*C.char)(maybeNullBuf)

	goMaybeNullBuf := (*[1 << 30]C.char)(maybeNullBuf)[:len(argTypes):len(argTypes)]
	for i, argType := range argTypes {
		if mysql.HasNotNullFlag(argType.GetFlag()) {
			goMaybeNullBuf[i] = 0
		} else {
			goMaybeNullBuf[i] = 1
		}
	}

	return ptrUDFArgs, func() {
		C.free(maybeNullBuf)
		C.free(lengthBuf)
		C.free(argsBuf)
		C.free(argsTypeBuf)
		C.free(ptr)
	}, nil
}

func castFieldTypeToUDFArgType(argType *types.FieldType) (C.enum_Item_result, error) {
	return castEvalTypeToUDFArgType(argType.EvalType())
}

func castEvalTypeToUDFArgType(evalTp types.EvalType) (C.enum_Item_result, error) {
	switch evalTp {
	case types.ETInt:
		return C.INT_RESULT, nil
	case types.ETReal:
		return C.REAL_RESULT, nil
	case types.ETDecimal:
		return C.DECIMAL_RESULT, nil
	case types.ETString:
		return C.STRING_RESULT, nil
	default:
		return C.INVALID_RESULT, errors.Errorf("unsupported type for UDF: %d", evalTp)
	}
}

// CastEvalTypeToUDFArgTypeInt casts the EvalType to an int of MySQL's Item_result enum.
func CastEvalTypeToUDFArgTypeInt(evalTp types.EvalType) int {
	t, _ := castEvalTypeToUDFArgType(evalTp)
	return int(t)
}

// CastUDFArgTypeIntToEvalType casts an int of MySQL's Item_result enum to EvalType.
func CastUDFArgTypeIntToEvalType(i int) types.EvalType {
	switch i {
	case 0:
		return types.ETString
	case 1:
		return types.ETReal
	case 2:
		return types.ETInt
	case 4:
		return types.ETDecimal
	default:
		logutil.BgLogger().Error("unsupported type for UDF, fallback to ETString",
			zap.Int("Item_result", i))
		return types.ETString
	}
}

func getArgFieldTypes(ctx EvalContext, args []Expression) []*types.FieldType {
	result := make([]*types.FieldType, 0, len(args))
	for _, arg := range args {
		result = append(result, arg.GetType(ctx))
	}
	return result
}

type loadableFuncClass struct {
	baseFunctionClass
	expropt.PrivilegeCheckerPropReader
	funcDef *LoadableFunctionDef
	flen    int
}

// CreateLoadableFunction registers a loadable function to the system. The input
// definition will not be dropped in this function, caller should take care of it.
func CreateLoadableFunction(def *LoadableFunctionDef) (exist bool, err error) {
	class, err := createFuncClassFromDef(def)
	if err != nil {
		return false, err
	}
	_, exist = loadableFuncs.LoadOrStore(strings.ToLower(def.name), class)
	return exist, err
}

// DropLoadableFunction removes a loadable function. It's only the reverse
// function of CreateLoadableFunction. Note that the so library resource is
// acquired in LoadUDF and should call LoadableFunctionDef.Drop to release.
func DropLoadableFunction(name string) {
	loadableFuncs.Delete(strings.ToLower(name))
}

// RemoveLoadableFunction removes a loadable function and returns its definition if it exists.
func RemoveLoadableFunction(name string) (*LoadableFunctionDef, bool) {
	value, ok := loadableFuncs.LoadAndDelete(strings.ToLower(name))
	if !ok {
		return nil, false
	}
	class, ok := value.(*loadableFuncClass)
	if !ok {
		return nil, true
	}
	return class.funcDef, true
}

// LoadableFunctionNames returns the names of all registered loadable functions.
func LoadableFunctionNames() []string {
	names := make([]string, 0)
	loadableFuncs.Range(func(key, _ any) bool {
		if name, ok := key.(string); ok {
			names = append(names, name)
		}
		return true
	})
	return names
}

// HasLoadableFunction checks if a loadable function with the given name exists.
func HasLoadableFunction(name string) bool {
	_, ok := loadableFuncs.Load(strings.ToLower(name))
	return ok
}

func nameIsSpecialNativeFunc(lowerName string) bool {
	// Keep in sync with special cases handled in newFunctionImpl.
	switch lowerName {
	case ast.Cast, ast.GetVar, ast.GetProcedureVar, InternalFuncFromBinary, InternalFuncToBinary:
		return true
	default:
		return false
	}
}

func nameConflictsWithNativeOrExtensionFunc(lowerName string) bool {
	if _, ok := funcs[lowerName]; ok {
		return true
	}
	if nameIsSpecialNativeFunc(lowerName) {
		return true
	}
	_, ok := extensionFuncs.Load(lowerName)
	return ok
}

// NameIsBuiltinFunc returns true when the lowercase name is a builtin function name.
func NameIsBuiltinFunc(lowerName string) bool {
	_, exist := funcs[lowerName]
	return exist
}
