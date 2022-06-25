// Copyright 2019 PingCAP, Inc.
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

package common

import (
	"sync"
)

// OnceError is an error value which will can be assigned once.
//
// The zero value is ready for use.
type OnceError struct {
	lock sync.Mutex
	err  error
}

// Set assigns an error to this instance, if `e != nil`.
//
// If this method is called multiple times, only the first call is effective.
func (oe *OnceError) Set(e error) {
	if e != nil {
		oe.lock.Lock()
		if oe.err == nil {
			oe.err = e
		}
		oe.lock.Unlock()
	}
}

// Get returns the first error value stored in this instance.
func (oe *OnceError) Get() error {
	oe.lock.Lock()
	defer oe.lock.Unlock()
	return oe.err
}
