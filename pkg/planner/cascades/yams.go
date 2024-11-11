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

package cascades

// Yams is a basic cascades search framework portal, drove by yamsContext.
type Yams struct {
	yCtx *YamsContext
}

// Execute run the yams search flow inside, returns error if it happened.
func (y *Yams) Execute() error {
	return y.yCtx.scheduler.ExecuteTasks()
}

// NewYams return a new yams struct as the memo search portal structure.
func NewYams(ctx *YamsContext) *Yams {
	return &Yams{
		yCtx: ctx,
	}
}
