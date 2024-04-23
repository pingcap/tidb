// Copyright 2024 PingCAP, Inc.
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

package context

import (
	"fmt"
	"sync/atomic"
)

// ValueStoreContext is a context that can store values.
type ValueStoreContext interface {
	// SetValue saves a value associated with this context for key.
	SetValue(key fmt.Stringer, value any)

	// Value returns the value associated with this context for key.
	Value(key fmt.Stringer) any

	// ClearValue clears the value associated with this context for key.
	ClearValue(key fmt.Stringer)
}

var contextIDGenerator atomic.Uint64

// GenContextID generates a unique context ID.
func GenContextID() uint64 {
	return contextIDGenerator.Add(1)
}
