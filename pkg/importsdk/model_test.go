// Copyright 2025 PingCAP, Inc.
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

package importsdk

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestJobStatus(t *testing.T) {
	tests := []struct {
		status      string
		isFinished  bool
		isFailed    bool
		isCancelled bool
		isCompleted bool
	}{
		{
			status:      "finished",
			isFinished:  true,
			isFailed:    false,
			isCancelled: false,
			isCompleted: true,
		},
		{
			status:      "failed",
			isFinished:  false,
			isFailed:    true,
			isCancelled: false,
			isCompleted: true,
		},
		{
			status:      "cancelled",
			isFinished:  false,
			isFailed:    false,
			isCancelled: true,
			isCompleted: true,
		},
		{
			status:      "running",
			isFinished:  false,
			isFailed:    false,
			isCancelled: false,
			isCompleted: false,
		},
		{
			status:      "pending",
			isFinished:  false,
			isFailed:    false,
			isCancelled: false,
			isCompleted: false,
		},
		{
			status:      "unknown",
			isFinished:  false,
			isFailed:    false,
			isCancelled: false,
			isCompleted: false,
		},
	}

	for _, tt := range tests {
		job := &JobStatus{Status: tt.status}
		require.Equal(t, tt.isFinished, job.IsFinished(), "status: %s", tt.status)
		require.Equal(t, tt.isFailed, job.IsFailed(), "status: %s", tt.status)
		require.Equal(t, tt.isCancelled, job.IsCancelled(), "status: %s", tt.status)
		require.Equal(t, tt.isCompleted, job.IsCompleted(), "status: %s", tt.status)
	}
}
