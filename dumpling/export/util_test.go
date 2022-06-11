// Copyright 2021 PingCAP, Inc. Licensed under Apache-2.0.

package export

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/tidb/br/pkg/version"
)

func TestRepeatableRead(t *testing.T) {
	data := [][]interface{}{
		{version.ServerTypeUnknown, ConsistencyTypeNone, true},
		{version.ServerTypeMySQL, ConsistencyTypeFlush, true},
		{version.ServerTypeMariaDB, ConsistencyTypeLock, true},
		{version.ServerTypeTiDB, ConsistencyTypeNone, true},
		{version.ServerTypeTiDB, ConsistencyTypeSnapshot, false},
		{version.ServerTypeTiDB, ConsistencyTypeLock, true},
	}
	dec := func(d []interface{}) (version.ServerType, string, bool) {
		return version.ServerType(d[0].(int)), d[1].(string), d[2].(bool)
	}
	for tag, datum := range data {
		serverTp, consistency, expectRepeatableRead := dec(datum)
		comment := fmt.Sprintf("test case number: %d", tag)
		rr := needRepeatableRead(serverTp, consistency)
		require.True(t, rr == expectRepeatableRead, comment)
	}
}
