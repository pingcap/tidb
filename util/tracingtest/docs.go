// Copyright 2021 PingCAP, Inc.
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

// Package tracingtest provide a simple tracing-based test toolkit.
// with it, white-box-testcase writers can:
//
//  - add/continue breakpoints for routines to simulate different execution orders
//  - ensure routines wait on special breakpoint or ensure it has finished without unstable `time.Sleep(n)`
//  - enable/disable failpoint in routine level easily
//  - collect and assert execution path pattern by tracing-log to ensure routine cover desired code-path.
//
// it's standing on the shoulders of giants, it combines failpoint, opentracing, panicparse...
// it's not a substitute for failpoint, it bases on failpoint and wants to simulate execution flow easier
// and support assert execution path just like assert explain result
//
// Terminology
//
//  - Sandbox: every test using tracingtest need create a Sandbox to manage and monitor test Routines.
//  - Routine: one test can have one or many routines(i.e. different txn), it derived from Sandbox and be managed and monitored by Sandbox, routine's logic need use routine's `context.Context`
//  - Breakpoint: one routine can set several breakpoints, and each breakpoint can be hit or continued individually and also support wait breakpoint be hit
//
// How to Use
//
// 1. create an `Sandbox` at first in test
// 2. divide testcase to several routine and `StartRoutine` for each of them(e.g. one routine for one txn)
// 3. add `CheckBreakpoint` to place that want to wait on
// 4. `EnableBreakpoint` for special routine's `context.Context`
// 5. do testcase and using right routine's `context.Context`
// 6. check whether breakpoint has hit via `EnsureWaitOnBreakpoint`
// 7. continue do testcase or `EnableBreakpoint` for another breakpoint if needed
// 8. `ContinueBreakpoint` to resume waited routine
// 9. use `EnsureGoroutineFinished` to ensure some goroutine has finished
// 10. use `CollectLogs` to collect execution path info and do assert
// 11. build TiDB-server with `make failpoint-enable` then run testcase
//
// see example testcase in: 2pc_test.go => TestCommitSecondarySlow
package tracingtest
