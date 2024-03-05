// Copyright 2022 PingCAP, Inc.
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

package infosync

import (
	"context"
	"sync"

	pd "github.com/tikv/pd/client/http"
)

// ScheduleManager manages schedule configs
type ScheduleManager interface {
	GetScheduleConfig(ctx context.Context) (map[string]any, error)
	SetScheduleConfig(ctx context.Context, config map[string]any) error
}

// PDScheduleManager manages schedule with pd
type PDScheduleManager struct {
	pd.Client
}

type mockScheduleManager struct {
	sync.RWMutex
	schedules map[string]any
}

// GetScheduleConfig get schedule config from schedules map
func (mm *mockScheduleManager) GetScheduleConfig(context.Context) (map[string]any, error) {
	mm.Lock()

	schedules := make(map[string]any)
	for key, values := range mm.schedules {
		schedules[key] = values
	}

	mm.Unlock()
	return schedules, nil
}

// SetScheduleConfig set schedule config to schedules map
func (mm *mockScheduleManager) SetScheduleConfig(_ context.Context, config map[string]any) error {
	mm.Lock()

	if mm.schedules == nil {
		mm.schedules = make(map[string]any)
	}
	for key, value := range config {
		mm.schedules[key] = value
	}

	mm.Unlock()
	return nil
}
