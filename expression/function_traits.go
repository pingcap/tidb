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
	"github.com/pingcap/parser/ast"
)

// UnCacheableFunctions stores functions which can not be cached to plan cache.
var UnCacheableFunctions = map[string]struct{}{
	ast.Database:     {},
	ast.CurrentUser:  {},
	ast.CurrentRole:  {},
	ast.User:         {},
	ast.ConnectionID: {},
	ast.LastInsertId: {},
	ast.RowCount:     {},
	ast.Version:      {},
	ast.Like:         {},
}

// unFoldableFunctions stores functions which can not be folded duration constant folding stage.
var unFoldableFunctions = map[string]struct{}{
	ast.Sysdate:   {},
	ast.FoundRows: {},
	ast.Rand:      {},
	ast.UUID:      {},
	ast.Sleep:     {},
	ast.RowFunc:   {},
	ast.Values:    {},
	ast.SetVar:    {},
	ast.GetVar:    {},
	ast.GetParam:  {},
	ast.Benchmark: {},
	ast.DayName:   {},
	ast.NextVal:   {},
	ast.LastVal:   {},
	ast.SetVal:    {},
}

// DisableFoldFunctions stores functions which prevent child scope functions from being constant folded.
// Typically, these functions shall also exist in unFoldableFunctions, to stop from being folded when they themselves
// are in child scope of an outer function, and the outer function is recursively folding its children.
var DisableFoldFunctions = map[string]struct{}{
	ast.Benchmark: {},
}

// IllegalFunctions4GeneratedColumns stores functions that is illegal for generated columns.
// See https://github.com/mysql/mysql-server/blob/5.7/mysql-test/suite/gcol/inc/gcol_blocked_sql_funcs_main.inc for details
var IllegalFunctions4GeneratedColumns = map[string]struct{}{
	ast.ConnectionID:     {},
	ast.LoadFile:         {},
	ast.LastInsertId:     {},
	ast.Rand:             {},
	ast.UUID:             {},
	ast.UUIDShort:        {},
	ast.Curdate:          {},
	ast.CurrentDate:      {},
	ast.Curtime:          {},
	ast.CurrentTime:      {},
	ast.CurrentTimestamp: {},
	ast.LocalTime:        {},
	ast.LocalTimestamp:   {},
	ast.Now:              {},
	ast.UnixTimestamp:    {},
	ast.UTCDate:          {},
	ast.UTCTime:          {},
	ast.UTCTimestamp:     {},
	ast.Benchmark:        {},
	ast.CurrentUser:      {},
	ast.Database:         {},
	ast.FoundRows:        {},
	ast.GetLock:          {},
	ast.IsFreeLock:       {},
	ast.IsUsedLock:       {},
	ast.MasterPosWait:    {},
	ast.NameConst:        {},
	ast.ReleaseLock:      {},
	ast.RowCount:         {},
	ast.Schema:           {},
	ast.SessionUser:      {},
	ast.Sleep:            {},
	ast.Sysdate:          {},
	ast.SystemUser:       {},
	ast.User:             {},
	ast.Values:           {},
	ast.Encrypt:          {},
	ast.Version:          {},
	ast.JSONMerge:        {},
	ast.SetVar:           {},
	ast.GetVar:           {},
	ast.ReleaseAllLocks:  {},
}

// DeferredFunctions stores non-deterministic functions, which can be deferred only when the plan cache is enabled.
var DeferredFunctions = map[string]struct{}{
	ast.Now:              {},
	ast.RandomBytes:      {},
	ast.CurrentTimestamp: {},
	ast.UTCTime:          {},
	ast.Curtime:          {},
	ast.CurrentTime:      {},
	ast.UTCTimestamp:     {},
	ast.UnixTimestamp:    {},
	ast.Sysdate:          {},
	ast.Curdate:          {},
	ast.CurrentDate:      {},
	ast.UTCDate:          {},
	ast.Rand:             {},
	ast.UUID:             {},
}

// inequalFunctions stores functions which cannot be propagated from column equal condition.
var inequalFunctions = map[string]struct{}{
	ast.IsNull: {},
}

// mutableEffectsFunctions stores functions which are mutable or have side effects, specifically,
// we cannot remove them from filter even if they have duplicates.
var mutableEffectsFunctions = map[string]struct{}{
	// Time related functions in MySQL have various behaviors when executed multiple times in a single SQL,
	// for example:
	// mysql> select current_timestamp(), sleep(5), current_timestamp();
	// +---------------------+----------+---------------------+
	// | current_timestamp() | sleep(5) | current_timestamp() |
	// +---------------------+----------+---------------------+
	// | 2018-12-18 17:55:39 |        0 | 2018-12-18 17:55:39 |
	// +---------------------+----------+---------------------+
	// while:
	// mysql> select sysdate(), sleep(5), sysdate();
	// +---------------------+----------+---------------------+
	// | sysdate()           | sleep(5) | sysdate()           |
	// +---------------------+----------+---------------------+
	// | 2018-12-18 17:57:38 |        0 | 2018-12-18 17:57:43 |
	// +---------------------+----------+---------------------+
	// for safety consideration, treat them all as mutable.
	ast.Now:              {},
	ast.CurrentTimestamp: {},
	ast.UTCTime:          {},
	ast.Curtime:          {},
	ast.CurrentTime:      {},
	ast.UTCTimestamp:     {},
	ast.UnixTimestamp:    {},
	ast.Sysdate:          {},
	ast.Curdate:          {},
	ast.CurrentDate:      {},
	ast.UTCDate:          {},

	ast.Rand:        {},
	ast.RandomBytes: {},
	ast.UUID:        {},
	ast.UUIDShort:   {},
	ast.Sleep:       {},
	ast.SetVar:      {},
	ast.GetVar:      {},
	ast.AnyValue:    {},
}

// some functions like "get_lock" and "release_lock" currently do NOT have
// right implementations, but may have noop ones(like with any inputs, always return 1)
// if apps really need these "funcs" to run, we offer sys var(tidb_enable_noop_functions) to enable noop usage
var noopFuncs = map[string]struct{}{
	ast.GetLock:     {},
	ast.ReleaseLock: {},
}
