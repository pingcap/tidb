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
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMockWriter(t *testing.T) {
	mock := NewMockWriter()
	require.NotNil(t, mock)
	require.Equal(t, 0, mock.GetWriteCount())
	require.False(t, mock.IsClosed())
}

func TestMockWriterWrite(t *testing.T) {
	mock := NewMockWriter()
	
	// Test successful write
	testData := "test data"
	err := mock.Write(context.Background(), testData)
	require.NoError(t, err)
	require.Equal(t, 1, mock.GetWriteCount())
	
	data := mock.GetWriteData()
	require.Len(t, data, 1)
	require.Equal(t, testData, data[0])
}

func TestMockWriterWriteMultiple(t *testing.T) {
	mock := NewMockWriter()
	
	// Write multiple times
	for i := 0; i < 5; i++ {
		err := mock.Write(context.Background(), i)
		require.NoError(t, err)
	}
	
	require.Equal(t, 5, mock.GetWriteCount())
	data := mock.GetWriteData()
	require.Len(t, data, 5)
	for i := 0; i < 5; i++ {
		require.Equal(t, i, data[i])
	}
}

func TestMockWriterWriteErrors(t *testing.T) {
	mock := NewMockWriter()
	
	// Configure errors
	expectedErrors := []error{
		errors.New("error 1"),
		nil,
		errors.New("error 3"),
	}
	mock.SetWriteErrors(expectedErrors)
	
	// Write and check errors
	for i := 0; i < 3; i++ {
		err := mock.Write(context.Background(), i)
		if expectedErrors[i] != nil {
			require.Error(t, err)
			require.Equal(t, expectedErrors[i], err)
		} else {
			require.NoError(t, err)
		}
	}
	
	// After configured errors are exhausted, should return nil
	err := mock.Write(context.Background(), "extra")
	require.NoError(t, err)
}

func TestMockWriterClose(t *testing.T) {
	mock := NewMockWriter()
	
	require.False(t, mock.IsClosed())
	err := mock.Close()
	require.NoError(t, err)
	require.True(t, mock.IsClosed())
}

func TestMockWriterCloseError(t *testing.T) {
	mock := NewMockWriter()
	
	expectedErr := errors.New("close error")
	mock.SetCloseError(expectedErr)
	
	err := mock.Close()
	require.Error(t, err)
	require.Equal(t, expectedErr, err)
	require.True(t, mock.IsClosed())
}

func TestMockWriterConcurrent(t *testing.T) {
	mock := NewMockWriter()
	
	// Test concurrent writes
	done := make(chan bool)
	for i := 0; i < 10; i++ {
		go func(val int) {
			mock.Write(context.Background(), val)
			done <- true
		}(i)
	}
	
	for i := 0; i < 10; i++ {
		<-done
	}
	
	require.Equal(t, 10, mock.GetWriteCount())
	require.Len(t, mock.GetWriteData(), 10)
}
