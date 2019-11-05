// Copyright 2019 PingCAP, Inc.
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

package failpoint

import "context"

// Inject marks a fail point routine, which will be rewrite to a `if` statement
// and be triggered by fail point name specified `fpname`
// Note: The fail point closure  parameter type can only be `failpoint.Value`
// e.g:
// failpoint.Inject("fail-point-name", func() (...){}
// failpoint.Inject("fail-point-name", func(val failpoint.Value) (...){}
// failpoint.Inject("fail-point-name", func(_ failpoint.Value) (...){}
func Inject(fpname string, fpbody interface{}) {}

// InjectContext marks a fail point routine, which will be rewrite to a `if` statement
// and be triggered by fail point name specified `fpname`
// Note: The fail point closure  parameter type can only be `failpoint.Value`
// e.g:
// failpoint.InjectContext(ctx, "fail-point-name", func() (...){}
// failpoint.InjectContext(ctx, "fail-point-name", func(val failpoint.Value) (...){}
// failpoint.InjectContext(ctx, "fail-point-name", func(_ failpoint.Value) (...){}
func InjectContext(ctx context.Context, fpname string, fpbody interface{}) {}

// Break will generate a break statement in a loop, e.g:
// case1:
//   for i := 0; i < max; i++ {
//       failpoint.Inject("break-if-index-equal-2", func() {
//           if i == 2 {
//               failpoint.Break()
//           }
//       }
//   }
// failpoint.Break() => break
//
// case2:
//   outer:
//   for i := 0; i < max; i++ {
//       for j := 0; j < max / 2; j++ {
//           failpoint.Inject("break-if-index-i-equal-j", func() {
//               if i == j {
//                   failpoint.Break("outer")
//               }
//           }
//       }
//   }
// failpoint.Break("outer") => break outer
func Break(label ...string) {}

// Goto will generate a goto statement the same as `failpoint.Break()`
func Goto(label string) {}

// Continue will generate a continue statement the same as `failpoint.Break()`
func Continue(label ...string) {}

// Fallthrough will translate to a `fallthrough` statement
func Fallthrough() {}

// Return will translate to a `return` statement
func Return(result ...interface{}) {}

// Label will generate a label statement, e.g.
// case1:
//   failpoint.Label("outer")
//   for i := 0; i < max; i++ {
//       for j := 0; j < max / 2; j++ {
//           failpoint.Inject("break-if-index-i-equal-j", func() {
//               if i == j {
//                   failpoint.Break("outer")
//               }
//           }
//       }
//   }
// failpoint.Label("outer") => outer:
// failpoint.Break("outer") => break outer
func Label(label string) {}
