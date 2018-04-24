// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package expression

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"plugin"
	"strconv"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tipb/go-tipb"
)

func CompileAndLoad(goCode string) (*plugin.Plugin, error) {
	goFile := filepath.Join(os.TempDir(), "codegen.go")
	err := ioutil.WriteFile(goFile, []byte(goCode), 0644)
	if err != nil {
		return nil, errors.Trace(err)
	}

	pluginFile := filepath.Join(os.TempDir(), "codegen.so")
	cmd := exec.Command("go", "build", "-buildmode", "plugin", "-o", pluginFile, goFile)
	err = cmd.Run()
	if err != nil {
		return nil, errors.Trace(err)
	}

	return plugin.Open(pluginFile)
}

func PBToGoExpr(expr *tipb.Expr) (string, error) {
	switch expr.Tp {
	case tipb.ExprType_Int64:
		_, i, err := codec.DecodeInt(expr.Val)
		if err != nil {
			return "", errors.Trace(err)
		}
		return strconv.FormatInt(i, 10), nil
	case tipb.ExprType_ColumnRef:
		_, colIdx, err := codec.DecodeInt(expr.Val)
		if err != nil {
			return "", errors.Trace(err)
		}
		return fmt.Sprintf("row.GetInt64(%d)", colIdx), nil
	case tipb.ExprType_ScalarFunc:
		args := make([]string, 0, len(expr.Children))
		for _, child := range expr.Children {
			expr, err := PBToGoExpr(child)
			if err != nil {
				return "", errors.Trace(err)
			}
			args = append(args, expr)
		}
		var buf bytes.Buffer
		buf.WriteString(expr.Sig.String())
		buf.WriteString("(")
		for i := 0; i < len(args); i++ {
			if i != 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(args[i])
		}
		buf.WriteString(")")
		return buf.String(), nil
	}
	panic("never here")
}
