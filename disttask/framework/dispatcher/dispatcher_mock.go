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

package dispatcher

import (
	"context"

	"github.com/stretchr/testify/mock"
)

// MockHandle is used to mock the Handle.
type MockHandle struct {
	mock.Mock
}

// GetAllSchedulerIDs implements the Handle.GetAllSchedulerIDs interface.
func (m *MockHandle) GetAllSchedulerIDs(ctx context.Context, gTaskID int64) ([]string, error) {
	args := m.Called(ctx, gTaskID)
	if args.Error(1) != nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]string), nil
}
