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

package proto

type Pool interface {
	Run(func()) error
	RunWithConcurrency(func(), uint64) error
	ReleaseAndWait()
}

type DefaultPool struct{}

func (*DefaultPool) Run(f func()) error {
	go f()
	return nil
}

func (*DefaultPool) RunWithConcurrency(f func(), n uint64) error {
	for i := uint64(0); i < n; i++ {
		go f()
	}
	return nil
}

func (*DefaultPool) ReleaseAndWait() {}

func NewPool(concurrency int) Pool {
	return &DefaultPool{}
}
