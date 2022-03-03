package core

import (
	"github.com/pingcap/tipb/go-tipb"
	"github.com/stretchr/testify/require"

	"testing"
)

func TestFragmentInitSingleton(t *testing.T) {
	r1, r2 := &PhysicalExchangeReceiver{}, &PhysicalExchangeReceiver{}
	r1.SetChildren(&PhysicalExchangeSender{ExchangeType: tipb.ExchangeType_PassThrough})
	r2.SetChildren(&PhysicalExchangeSender{ExchangeType: tipb.ExchangeType_Broadcast})
	p := &PhysicalHashJoin{}

	f := &Fragment{}
	p.SetChildren(r1, r1)
	err := f.init(p)
	require.NoError(t, err)
	require.Equal(t, f.singleton, true)

	f = &Fragment{}
	p.SetChildren(r1, r2)
	err = f.init(p)
	require.NoError(t, err)
	require.Equal(t, f.singleton, true)

	f = &Fragment{}
	p.SetChildren(r2, r1)
	err = f.init(p)
	require.NoError(t, err)
	require.Equal(t, f.singleton, true)

	f = &Fragment{}
	p.SetChildren(r2, r2)
	err = f.init(p)
	require.NoError(t, err)
	require.Equal(t, f.singleton, false)
}
