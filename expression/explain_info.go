// Copyright 2017 PingCAP, Inc.
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
	"fmt"
	"bytes"
)

func (expr *ScalarFunction) ExplainInfo() string {
	buffer := bytes.NewBufferString(fmt.Sprintf("%s(", expr.FuncName.L))
	for i, arg := range expr.GetArgs() {
		buffer.WriteString(fmt.Sprintf("%s", arg.ExplainInfo()))
		if i<len(expr.GetArgs()) {
			buffer.WriteString(", ")
		}
	}
	buffer.WriteString(")")
	return buffer.String()
}

func (expr *Column) ExplainInfo() string {
	return fmt.Sprintf("col(%s)", expr.String())
}

func (expr *Constant) ExplainInfo() string {
	valStr, err := expr.Value.ToString()
	if err != nil {
		valStr = "not recognize const value"
	}
	return fmt.Sprintf("const(%s)", valStr)
}
