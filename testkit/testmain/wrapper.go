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

//go:build !codes

package testmain

import "go.uber.org/goleak"

type testingM struct {
	goleak.TestingM
	callback func(int) int
}

func (m *testingM) Run() int {
	return m.callback(m.TestingM.Run())
}

// WrapTestingM returns a TestingM wrapped with callback on m.Run returning
func WrapTestingM(m goleak.TestingM, callback func(int) int) *testingM {
	if callback == nil {
		callback = func(i int) int {
			return i
		}
	}

	return &testingM{
		TestingM: m,
		callback: callback,
	}
}
