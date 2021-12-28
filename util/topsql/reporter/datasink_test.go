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

package reporter

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestDefaultDataSinkRegisterer(t *testing.T) {
	var err error
	r := NewDefaultDataSinkRegisterer(context.Background())
	m1 := newMockDataSink2()
	m2 := newMockDataSink2()
	err = r.Register(m1)
	assert.NoError(t, err)
	err = r.Register(m2)
	assert.NoError(t, err)
	assert.Len(t, r.dataSinks, 2)
	r.Deregister(m1)
	r.Deregister(m2)
	assert.Empty(t, r.dataSinks)
}

type mockDataSink2 struct {
	data   []*ReportData
	closed bool
}

func newMockDataSink2() *mockDataSink2 {
	return &mockDataSink2{
		data: []*ReportData{},
	}
}

func (m *mockDataSink2) TrySend(data *ReportData, deadline time.Time) error {
	m.data = append(m.data, data)
	return nil
}

func (m *mockDataSink2) OnReporterClosing() {
	m.closed = true
}
