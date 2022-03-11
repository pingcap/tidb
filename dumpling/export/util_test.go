// Copyright 2021 PingCAP, Inc. Licensed under Apache-2.0.

package export

import (
	"fmt"
	"testing"

	"github.com/pingcap/tidb/br/pkg/version"
	"github.com/stretchr/testify/require"
)

func TestRepeatableRead(t *testing.T) {
	data := [][]interface{}{
		{version.ServerTypeUnknown, consistencyTypeNone, true},
		{version.ServerTypeMySQL, consistencyTypeFlush, true},
		{version.ServerTypeMariaDB, consistencyTypeLock, true},
		{version.ServerTypeTiDB, consistencyTypeNone, true},
		{version.ServerTypeTiDB, consistencyTypeSnapshot, false},
		{version.ServerTypeTiDB, consistencyTypeLock, true},
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
