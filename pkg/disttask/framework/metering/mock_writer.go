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

package metering

import (
	"context"
	"sync"
)

// MockWriter is a mock implementation of Writer for testing purposes.
type MockWriter struct {
	mu          sync.Mutex
	writeData   []interface{}
	writeErrors []error
	writeCount  int
	closeError  error
	closed      bool
}

// NewMockWriter creates a new MockWriter instance.
func NewMockWriter() *MockWriter {
	return &MockWriter{
		writeData: make([]interface{}, 0),
	}
}

// Write records the data passed to it and returns the configured error.
func (m *MockWriter) Write(_ context.Context, data interface{}) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	m.writeData = append(m.writeData, data)
	var err error
	if m.writeCount < len(m.writeErrors) {
		err = m.writeErrors[m.writeCount]
	}
	m.writeCount++
	return err
}

// Close marks the writer as closed and returns the configured error.
func (m *MockWriter) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	m.closed = true
	return m.closeError
}

// GetWriteData returns all data that was written to the mock.
func (m *MockWriter) GetWriteData() []interface{} {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	result := make([]interface{}, len(m.writeData))
	copy(result, m.writeData)
	return result
}

// GetWriteCount returns the number of times Write was called.
func (m *MockWriter) GetWriteCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	return m.writeCount
}

// IsClosed returns whether Close has been called.
func (m *MockWriter) IsClosed() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	return m.closed
}

// SetWriteErrors configures the errors to return from Write calls.
func (m *MockWriter) SetWriteErrors(errors []error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	m.writeErrors = errors
}

// SetCloseError configures the error to return from Close.
func (m *MockWriter) SetCloseError(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	m.closeError = err
}
