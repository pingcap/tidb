// Copyright 2026 PingCAP, Inc.
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

package importer

import (
	"context"
	"sync/atomic"
)

type sourceRecordCounterKey struct{}

// WithSourceRecordCounter attaches a subtask-attempt counter to the real read
// path. Sampling and estimated chunk row IDs never contribute to this counter.
func WithSourceRecordCounter(ctx context.Context, counter *atomic.Uint64) context.Context {
	return context.WithValue(ctx, sourceRecordCounterKey{}, counter)
}

func recordSourceRead(ctx context.Context) {
	if counter, ok := ctx.Value(sourceRecordCounterKey{}).(*atomic.Uint64); ok {
		counter.Add(1)
	}
}
