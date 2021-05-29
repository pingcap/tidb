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

package tikv

import (
	"context"
)

// BinlogExecutor defines the logic to replicate binlogs during transaction commit.
type BinlogExecutor interface {
	Prewrite(ctx context.Context, primary []byte) <-chan BinlogWriteResult
	Commit(ctx context.Context, commitTS int64)
	Skip()
}

// BinlogWriteResult defines the result of prewrite binlog.
type BinlogWriteResult interface {
	Skipped() bool
	GetError() error
}
