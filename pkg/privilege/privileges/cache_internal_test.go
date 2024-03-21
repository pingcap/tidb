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

package privileges

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// Test with an empty cache
func BenchmarkMatchIdentityEmpty(b *testing.B) {
	var p MySQLPrivilege

	require.Len(b, p.User, 0)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p.matchIdentity("zzz", "127.0.0.1", false)
	}
}

// Test with a full cache of 100k entries. No match, so have to test all entries.
func BenchmarkMatchIdentityFull(b *testing.B) {
	var p MySQLPrivilege

	for i := 0; i < 100000; i++ {
		var user UserRecord
		user.User = fmt.Sprintf("testuser_%08d", i)
		user.Host = "127.0.0.1"
		p.User = append(p.User, user)
	}

	require.Len(b, p.User, 100000)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p.matchIdentity("zzz", "127.0.0.1", false)
	}
}

// Test with a full cache of 100k entries. Early match, so able to do an early exit of the loop.
func BenchmarkMatchIdentityFullFirst(b *testing.B) {
	var p MySQLPrivilege

	for i := 0; i < 100000; i++ {
		var user UserRecord
		user.User = fmt.Sprintf("testuser_%08d", i)
		user.Host = "127.0.0.1"
		p.User = append(p.User, user)
	}

	require.Len(b, p.User, 100000)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p.matchIdentity(
			fmt.Sprintf("testuser_%08d", 1),
			"127.0.0.1",
			false,
		)
	}
}

// Test with skip name resolving to avoid DNS lookups etc.
func BenchmarkMatchIdentityFullSkipLookup(b *testing.B) {
	var p MySQLPrivilege

	for i := 0; i < 100000; i++ {
		var user UserRecord
		user.User = fmt.Sprintf("testuser_%08d", i)
		user.Host = "127.0.0.1"
		p.User = append(p.User, user)
	}

	require.Len(b, p.User, 100000)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p.matchIdentity("zzz", "127.0.0.1", true)
	}
}
