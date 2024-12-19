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

//go:build go1.22

package fastrand

import (
	_ "unsafe" // required by go:linkname
)

<<<<<<< HEAD:pkg/util/fastrand/runtime_1.22.go
// Uint32 returns a lock free uint32 value.
//
//go:linkname Uint32 runtime.cheaprand
func Uint32() uint32
=======
// NewContext creates a new mocked sessionctx.Context.
// This function should only be used for testing.
// Avoid using this when you are in a context with a `kv.Storage` instance, especially when you are going to access
// the data in it. Consider using testkit.NewSession(t, store) instead when possible.
func NewContext() *Context {
	return newContext()
}
>>>>>>> 0bf3e019002 (*: Update client-go and verify all read ts (#58054)):pkg/util/mock/fortest.go
