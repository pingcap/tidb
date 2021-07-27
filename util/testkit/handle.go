package testkit

import (
	"testing"

	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/codec"
	"github.com/stretchr/testify/require"
)

func MustNewCommonHandle(t *testing.T, values ...interface{}) kv.Handle {
	encoded, err := codec.EncodeKey(new(stmtctx.StatementContext), nil, types.MakeDatums(values...)...)
	require.Nil(t, err)
	ch, err := kv.NewCommonHandle(encoded)
	require.Nil(t, err)
	return ch
}
