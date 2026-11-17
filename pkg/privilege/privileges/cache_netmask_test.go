// Copyright 2026 PingCAP, Inc.
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
	"testing"

	"github.com/pingcap/tidb/pkg/util/stringutil"
	"github.com/stretchr/testify/require"
)

func TestMatchIdentityPrefersSpecificNetmask(t *testing.T) {
	const user = "netmask_order_repro"

	makeRecord := func(host string) UserRecord {
		return UserRecord{baseRecord: baseRecord{
			Host:      host,
			User:      user,
			patChars:  nil,
			patTypes:  nil,
			hostIPNet: parseHostIPNet(host),
		}}
	}

	makePrivilege := func(records ...UserRecord) *MySQLPrivilege {
		p := newMySQLPrivilege()
		p.user.ReplaceOrInsert(itemUser{username: user, data: records})
		p.SortUserTable()
		return p
	}

	broad := makeRecord("127.0.0.0/255.0.0.0")
	specific := makeRecord("127.0.0.0/255.255.0.0")
	for _, records := range [][]UserRecord{{broad, specific}, {specific, broad}} {
		p := makePrivilege(records...)
		matched := p.matchIdentity(user, "127.0.0.1", true)
		require.NotNil(t, matched)
		require.Equal(t, specific.Host, matched.Host)
	}

	// Equal masks remain ordered by the existing host comparison, and both
	// records continue to participate in matching.
	equalMask := makePrivilege(
		makeRecord("127.0.0.0/255.255.0.0"),
		makeRecord("127.1.0.0/255.255.0.0"),
	)
	matched := equalMask.matchIdentity(user, "127.1.0.1", true)
	require.NotNil(t, matched)
	require.Equal(t, "127.1.0.0/255.255.0.0", matched.Host)

	// Non-netmask hosts retain the existing host ordering and matching behavior.
	exact := UserRecord{baseRecord: baseRecord{
		Host:     "127.0.0.1",
		User:     user,
		patChars: nil,
		patTypes: nil,
	}}
	exact.patChars, exact.patTypes = stringutil.CompilePatternBinary(exact.Host, '\\')
	netmask := makeRecord("127.0.0.0/255.255.0.0")
	nonNetmask := makePrivilege(netmask, exact)
	matched = nonNetmask.matchIdentity(user, "127.0.0.1", true)
	require.NotNil(t, matched)
	require.Equal(t, exact.Host, matched.Host)
}
