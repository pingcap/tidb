// Copyright 2021 PingCAP, Inc. Licensed under Apache-2.0.

package export

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRepeatableRead(t *testing.T) {
	t.Parallel()

	data := [][]interface{}{
		{ServerTypeUnknown, consistencyTypeNone, true},
		{ServerTypeMySQL, consistencyTypeFlush, true},
		{ServerTypeMariaDB, consistencyTypeLock, true},
		{ServerTypeTiDB, consistencyTypeNone, true},
		{ServerTypeTiDB, consistencyTypeSnapshot, false},
		{ServerTypeTiDB, consistencyTypeLock, true},
	}
	dec := func(d []interface{}) (ServerType, string, bool) {
		return ServerType(d[0].(int)), d[1].(string), d[2].(bool)
	}
	for tag, datum := range data {
		serverTp, consistency, expectRepeatableRead := dec(datum)
		comment := fmt.Sprintf("test case number: %d", tag)
		rr := needRepeatableRead(serverTp, consistency)
		require.True(t, rr == expectRepeatableRead, comment)
	}
}
