// Copyright 2023 PingCAP, Inc.
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

package context

// WarnHandler provides a function to add a warning.
// Using interface rather than a simple function/closure can avoid memory allocation in some cases.
// See https://github.com/pingcap/tidb/issues/49277
type WarnHandler interface {
	// AppendWarning appends a warning
	AppendWarning(err error)
}

type ignoreWarn struct{}

func (*ignoreWarn) AppendWarning(_ error) {}

// IgnoreWarn is WarnHandler which does nothing
var IgnoreWarn WarnHandler = &ignoreWarn{}

type funcWarnHandler struct {
	fn func(err error)
}

func (r *funcWarnHandler) AppendWarning(err error) {
	r.fn(err)
}

// NewFuncWarnHandlerForTest creates a `WarnHandler` which will use the function to handle warn
// To have a better performance, it's not suggested to use this function in production.
func NewFuncWarnHandlerForTest(fn func(err error)) WarnHandler {
	return &funcWarnHandler{fn}
}
