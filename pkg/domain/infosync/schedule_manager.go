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
	GetPDScheduleConfig(ctx context.Context) (map[string]interface{}, error)
	SetPDScheduleConfig(ctx context.Context, config map[string]interface{}) error
}

// PDScheduleManager manages schedule with pd
type PDScheduleManager struct {
	pdHTTPCli pd.Client
}

// GetPDScheduleConfig get schedule config from pd
func (sm *PDScheduleManager) GetPDScheduleConfig(ctx context.Context) (map[string]interface{}, error) {
	return sm.pdHTTPCli.GetScheduleConfig(ctx)
}

// SetPDScheduleConfig set schedule config to pd
func (sm *PDScheduleManager) SetPDScheduleConfig(ctx context.Context, config map[string]interface{}) error {
	return sm.pdHTTPCli.SetScheduleConfig(ctx, config)
}

type mockScheduleManager struct {
	sync.RWMutex
	schedules map[string]interface{}
}

// GetPDScheduleConfig get schedule config from schedules map
func (mm *mockScheduleManager) GetPDScheduleConfig(ctx context.Context) (map[string]interface{}, error) {
	mm.Lock()

	schedules := make(map[string]interface{})
	for key, values := range mm.schedules {
		schedules[key] = values
	}

	mm.Unlock()
	return schedules, nil
}

// SetPDScheduleConfig set schedule config to schedules map
func (mm *mockScheduleManager) SetPDScheduleConfig(ctx context.Context, config map[string]interface{}) error {
	mm.Lock()

	if mm.schedules == nil {
		mm.schedules = make(map[string]interface{})
	}
	for key, value := range config {
		mm.schedules[key] = value
	}

	mm.Unlock()
	return nil
}
