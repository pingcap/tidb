// Copyright 2024 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ttlworker

import "context"

// WorkerTestExt is the extension interface for worker in test.
type WorkerTestExt interface {
	SetCtx(f func(ctx context.Context) context.Context)
}

var _ WorkerTestExt = &baseWorker{}

// SetCtx modifies the context of the worker.
func (w *baseWorker) SetCtx(f func(ctx context.Context) context.Context) {
	w.ctx = f(w.ctx)
}
