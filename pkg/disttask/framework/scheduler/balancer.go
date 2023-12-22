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

package scheduler

import (
	"context"
	"time"
)

var (
	// balanceCheckInterval is the interval to check whether we need to balance the subtasks.
	balanceCheckInterval = 3 * checkTaskFinishedInterval
)

// balancer is used to balance the subtasks of tasks, it handles 2 cases:
//   - managed node scale in/out.
//   - nodes might run subtasks in different speed, cause the subtasks are not balanced.
type balancer struct {
}

func (b *balancer) balanceLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(balanceCheckInterval):
		}
		b.balance(ctx)
	}
}

func (b *balancer) balance(ctx context.Context) {

}
