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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package stmtstats

// StatementObserver is an abstract interface as a callback to the corresponding
// position of TiDB's SQL statement execution process. StatementStats implements
// StatementObserver and performs counting such as SQLExecCount/SQLDuration internally.
// The caller only needs to be responsible for calling different methods at the
// corresponding locations, without paying attention to implementation details.
type StatementObserver interface {
	// The following events should be called during reading from the MySQL protocol.

	OnCmdDispatchBegin()
	OnCmdDispatchFinish()
	OnCmdQueryBegin()
	OnCmdQueryFinish()
	OnCmdQueryProcessStmtBegin(stmtIdx int)
	OnCmdQueryProcessStmtFinish(stmtIdx int)
	OnCmdStmtExecuteBegin()
	OnCmdStmtExecuteFinish()
	OnCmdStmtFetchBegin()
	OnCmdStmtFetchFinish(sqlDigest []byte, planDigest []byte)

	// OnDigestKnown is called when a digest is known.
	// It may be called multiple times when a statement is retried.
	// It may be called within a MySQL protocol when this session is created from a connection.
	// It may be called without a MySQL protocol when this session is created in background.
	OnDigestKnown(sqlDigest []byte, planDigest []byte)

	// The following events should be called during reading from wire protocol.
	OnExecuteBegin()
	OnExecuteFinish()
}
