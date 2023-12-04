// Copyright 2015 PingCAP, Inc.
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

package taskexecutor

import (
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
)

// Callback is used for task executor.
type Callback interface {
	OnInitBefore(task *proto.Task) error
	OnSubtaskRunBefore(subtask *proto.Subtask) bool
	OnSubtaskRunAfter(subtask *proto.Subtask) error
	OnSubtaskFinishedBefore(subtask *proto.Subtask) error
	OnSubtaskFinishedAfter(subtask *proto.Subtask)
}

var _ Callback = (*BaseCallback)(nil)

// BaseCallback implements Callback interface.
type BaseCallback struct {
}

// OnInitBefore implements Callback interface.
func (*BaseCallback) OnInitBefore(_ *proto.Task) error {
	return nil
}

// OnSubtaskRunBefore implements Callback interface.
func (*BaseCallback) OnSubtaskRunBefore(_ *proto.Subtask) bool {
	return false
}

// OnSubtaskRunAfter implements Callback interface.
func (*BaseCallback) OnSubtaskRunAfter(_ *proto.Subtask) error {
	return nil
}

// OnSubtaskFinishedBefore implements Callback interface.
func (*BaseCallback) OnSubtaskFinishedBefore(_ *proto.Subtask) error {
	return nil
}

// OnSubtaskFinishedAfter implements Callback interface.
func (*BaseCallback) OnSubtaskFinishedAfter(_ *proto.Subtask) {

}

// TestCallBack is used to customize callback for testing.
type TestCallBack struct {
	*BaseCallback
	OnInitBeforeExported            func(task *proto.Task) error
	OnSubtaskRunBeforeExported      func(subtask *proto.Subtask) bool
	OnSubtaskRunAfterExported       func(subtask *proto.Subtask) error
	OnSubtaskFinishedBeforeExported func(subtask *proto.Subtask) error
	OnSubtaskFinishedAfterExported  func(subtask *proto.Subtask)
}

// OnInitBefore overrides Callback interface.
func (tc *TestCallBack) OnInitBefore(task *proto.Task) error {
	if tc.OnInitBeforeExported != nil {
		return tc.OnInitBeforeExported(task)
	}
	return nil
}

// OnSubtaskRunBefore overrides Callback interface.
func (tc *TestCallBack) OnSubtaskRunBefore(subtask *proto.Subtask) bool {
	if tc.OnSubtaskRunBeforeExported != nil {
		return tc.OnSubtaskRunBeforeExported(subtask)
	}
	return false
}

// OnSubtaskRunAfter overrides Callback interface.
func (tc *TestCallBack) OnSubtaskRunAfter(subtask *proto.Subtask) error {
	if tc.OnSubtaskRunAfterExported != nil {
		return tc.OnSubtaskRunAfterExported(subtask)
	}
	return nil
}

// OnSubtaskFinishedBefore overrides Callback interface.
func (tc *TestCallBack) OnSubtaskFinishedBefore(subtask *proto.Subtask) error {
	if tc.OnSubtaskFinishedBeforeExported != nil {
		return tc.OnSubtaskFinishedBeforeExported(subtask)
	}
	return nil
}

// OnSubtaskFinishedAfter overrides Callback interface.
func (tc *TestCallBack) OnSubtaskFinishedAfter(subtask *proto.Subtask) {
	if tc.OnSubtaskFinishedAfterExported != nil {
		tc.OnSubtaskFinishedAfterExported(subtask)
	}
}
