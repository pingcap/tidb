// Copyright 2024 PingCAP, Inc.
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

package main

import (
	"bytes"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"log"
	"os"
	"path"
	"sort"
	"strings"
)

func collectThreadSafeBuiltinFuncs(file string) (safeFuncNames, unsafeFuncNames []string) {
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, file, nil, 0)
	if err != nil {
		panic(err)
	}

	allFuncNames := make([]string, 0, 32)
	ast.Inspect(f, func(n ast.Node) bool {
		if n != nil {
			switch x := n.(type) {
			case *ast.TypeSpec: // get all type definitions
				typeName := x.Name.Name
				if strings.HasPrefix(typeName, "builtin") && // type name is "builtin*Sig"
					strings.HasSuffix(typeName, "Sig") {
					if x.Type == nil {
						return true
					}
					if structType, ok := x.Type.(*ast.StructType); ok { // this type is a structure
						allFuncNames = append(allFuncNames, typeName)
						if len(structType.Fields.List) != 1 { // this structure only has 1 field
							return true
						}
						// this builtinXSig has only 1 field and this field is
						// `baseBuiltinFunc` or `baseBuiltinCastFunc`.
						if ident, ok := structType.Fields.List[0].Type.(*ast.Ident); ok &&
							(ident.Name == "baseBuiltinFunc" || ident.Name == "baseBuiltinCastFunc") {
							safeFuncNames = append(safeFuncNames, typeName)
						}
					}
				}
			}
		}
		return true
	})

	safeFuncMap := make(map[string]struct{}, len(safeFuncNames))
	for _, name := range safeFuncNames {
		safeFuncMap[name] = struct{}{}
	}
	for _, fName := range allFuncNames {
		if _, ok := safeFuncMap[fName]; !ok {
			unsafeFuncNames = append(unsafeFuncNames, fName)
		}
	}

	return safeFuncNames, unsafeFuncNames
}

func genBuiltinThreadSafeCode(dir string) (safe, unsafe []byte) {
	files := []string{
		"builtin.go",
		"builtin_arithmetic.go",
		"builtin_cast.go",
		"builtin_compare.go",
		"builtin_control.go",
		"builtin_convert_charset.go",
		"builtin_encryption.go",
		"builtin_func_param.go",
		"builtin_grouping.go",
		"builtin_ilike.go",
		"builtin_info.go",
		"builtin_json.go",
		"builtin_like.go",
		"builtin_math.go",
		"builtin_miscellaneous.go",
		"builtin_op.go",
		"builtin_other.go",
		"builtin_regexp.go",
		"builtin_regexp_util.go",
		"builtin_string.go",
		"builtin_time.go",
	}

	safeFuncs := make([]string, 0, 32)
	unsafeFuncs := make([]string, 0, 32)
	for _, file := range files {
		safeNames, unsafeNames := collectThreadSafeBuiltinFuncs(path.Join(dir, file))
		safeFuncs = append(safeFuncs, safeNames...)
		unsafeFuncs = append(unsafeFuncs, unsafeNames...)
	}
	sort.Strings(safeFuncs)

	var buffer bytes.Buffer
	buffer.WriteString(safeHeader)
	for _, funcName := range safeFuncs {
		buffer.WriteString(fmt.Sprintf(safeFuncTemp, funcName))
	}

	formattedSafe, err := format.Source(buffer.Bytes())
	if err != nil {
		panic(err)
	}

	buffer.Reset()
	buffer.WriteString(unsafeHeader)
	for _, funcName := range unsafeFuncs {
		buffer.WriteString(fmt.Sprintf(unsafeFuncTemp, funcName))
	}
	formattedUnsafe, err := format.Source(buffer.Bytes())
	if err != nil {
		panic(err)
	}

	return formattedSafe, formattedUnsafe
}

func main() {
	safeCode, unsafeCode := genBuiltinThreadSafeCode(".")
	if err := os.WriteFile("./builtin_threadsafe_generated.go", safeCode, 0644); err != nil {
		log.Fatalln("failed to write plan_clone_generated.go", err)
	}
	if err := os.WriteFile("./builtin_threadunsafe_generated.go", unsafeCode, 0644); err != nil {
		log.Fatalln("failed to write plan_clone_generated.go", err)
	}
}

const (
	safeFuncTemp = `// SafeToShareAcrossSession implements BuiltinFunc.SafeToShareAcrossSession.
func (s *%s) SafeToShareAcrossSession() bool {
	return safeToShareAcrossSession(&s.safeToShareAcrossSessionFlag, s.args)
}
`
	unsafeFuncTemp = `// SafeToShareAcrossSession implements BuiltinFunc.SafeToShareAcrossSession.
func (s *%s) SafeToShareAcrossSession() bool {
	return false
}
`
	safeHeader = `// Copyright 2024 PingCAP, Inc.
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

// Code generated by go generate in expression/generator; DO NOT EDIT.

package expression

import "sync/atomic"

func safeToShareAcrossSession(flag *uint32, args []Expression) bool {
	flagV := atomic.LoadUint32(flag)
	if flagV != 0 {
		return flagV == 1
	}

	allArgsSafe := true
	for _, arg := range args {
		if !arg.SafeToShareAcrossSession() {
			allArgsSafe = false
			break
		}
	}
	if allArgsSafe {
		atomic.StoreUint32(flag, 1)
	} else {
		atomic.StoreUint32(flag, 2)
	}
	return allArgsSafe
}

`

	unsafeHeader = `// Copyright 2024 PingCAP, Inc.
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

// Code generated by go generate in expression/generator; DO NOT EDIT.

package expression

`
)