// Copyright 2025 PingCAP, Inc.
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

package expression

import (
	"strings"
	"sync"

	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

var (
	// Keep the same MySQL error code used by other unsupported features.
	errLoadableFunctionNotSupported = dbterror.ClassExpression.NewStd(mysql.ErrNotSupportedYet)

	// ErrNativeFctNameCollision indicates a native function name collision error.
	//
	// Keep it defined even when UDF is disabled so other packages can depend on it
	// without needing to care about build tags.
	ErrNativeFctNameCollision = dbterror.ClassExpression.NewStd(mysql.ErrNativeFctNameCollision)
)

// loadableFuncs stores the loadable functions created by CREATE FUNCTION ... SONAME ...
// The key is the lowercase function name, and the value is *loadableFuncClass.
//
// Loadable functions (UDF) are intentionally not implemented in this build.
// Keep the map to make other expression code compile (e.g. constant folding and
// function resolution paths).
var loadableFuncs sync.Map

var logLoadableFunctionUnsupportedOnce sync.Once

func logLoadableFunctionUnsupported() {
	logLoadableFunctionUnsupportedOnce.Do(func() {
		logutil.BgLogger().Warn("loadable functions (UDF) are not implemented")
	})
}

func loadableFunctionUnsupportedErr() error {
	return errLoadableFunctionNotSupported.FastGenByArgs("loadable functions (UDF)")
}

// LoadableFunctionDef is the definition for a loadable function.
//
// It's kept as an opaque placeholder so dependent code (e.g. DDL worker /
// infoschema reload) can compile, but it cannot be used to execute a UDF.
type LoadableFunctionDef struct{}

// Drop releases the shared object handle. It's a no-op when UDF is disabled.
func (d *LoadableFunctionDef) Drop() {}

// LoadUDF loads a loadable function from a shared object file.
// This build does not support this feature.
func LoadUDF(soName, funcName string, funcRetType types.EvalType) (*LoadableFunctionDef, error) {
	logLoadableFunctionUnsupported()
	_ = soName
	_ = funcName
	_ = funcRetType
	return nil, loadableFunctionUnsupportedErr()
}

// ValidateLoadableFunctionDef validates the loadable function definition.
// This build does not support this feature.
func ValidateLoadableFunctionDef(def *LoadableFunctionDef) error {
	logLoadableFunctionUnsupported()
	_ = def
	return loadableFunctionUnsupportedErr()
}

// CreateLoadableFunction registers a loadable function to the system.
// This build does not support this feature.
func CreateLoadableFunction(def *LoadableFunctionDef) (exist bool, err error) {
	logLoadableFunctionUnsupported()
	_ = def
	return false, loadableFunctionUnsupportedErr()
}

// DropLoadableFunction removes a loadable function.
func DropLoadableFunction(name string) {
	loadableFuncs.Delete(strings.ToLower(name))
}

// RemoveLoadableFunction removes a loadable function and returns its definition if it exists.
func RemoveLoadableFunction(name string) (*LoadableFunctionDef, bool) {
	v, ok := loadableFuncs.LoadAndDelete(strings.ToLower(name))
	if !ok {
		return nil, false
	}
	def, _ := v.(*LoadableFunctionDef)
	return def, true
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

// loadableFuncClass is a stub implementation to keep compilation working when
// UDF is disabled. It should never be used to execute a UDF.
type loadableFuncClass struct {
	baseFunctionClass
}

func (c *loadableFuncClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	_ = ctx
	_ = args
	logLoadableFunctionUnsupported()
	return nil, loadableFunctionUnsupportedErr()
}

// loadableFuncSig is a stub type that only exists so other expression code can
// compile (e.g. constant folding checks).
//
// It embeds builtinFunc purely to satisfy the interface at compile time.
type loadableFuncSig struct {
	builtinFunc
}

// CastEvalTypeToUDFArgTypeInt casts the EvalType to an int of MySQL's Item_result enum.
func CastEvalTypeToUDFArgTypeInt(evalTp types.EvalType) int {
	switch evalTp {
	case types.ETString:
		return 0 // STRING_RESULT
	case types.ETReal:
		return 1 // REAL_RESULT
	case types.ETInt:
		return 2 // INT_RESULT
	case types.ETDecimal:
		return 4 // DECIMAL_RESULT
	default:
		return -1 // INVALID_RESULT
	}
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
