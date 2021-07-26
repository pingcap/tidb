// Copyright 2015 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package codec

import (
	"testing"

	"github.com/pingcap/tidb/types"
	"github.com/stretchr/testify/require"
)

func TestDecimalCodec(t *testing.T) {
	t.Parallel()

	inputs := []struct {
		Input float64
	}{
		{float64(123400)},
		{float64(1234)},
		{float64(12.34)},
		{float64(0.1234)},
		{float64(0.01234)},
		{float64(-0.1234)},
		{float64(-0.01234)},
		{float64(12.3400)},
		{float64(-12.34)},
		{float64(0.00000)},
		{float64(0)},
		{float64(-0.0)},
		{float64(-0.000)},
	}

	for _, input := range inputs {
		v := types.NewDecFromFloatForTest(input.Input)
		datum := types.NewDatum(v)

		b, err := EncodeDecimal([]byte{}, datum.GetMysqlDecimal(), datum.Length(), datum.Frac())
		require.NoError(t, err)
		_, d, prec, frac, err := DecodeDecimal(b)
		if datum.Length() != 0 {
			require.Equal(t, datum.Length(), prec)
			require.Equal(t, datum.Frac(), frac)
		} else {
			prec1, frac1 := datum.GetMysqlDecimal().PrecisionAndFrac()
			require.Equal(t, prec1, prec)
			require.Equal(t, frac1, frac)
		}
		require.NoError(t, err)
		require.Equal(t, 0, v.Compare(d))
	}
}

func TestFrac(t *testing.T) {
	t.Parallel()

	inputs := []struct {
		Input *types.MyDecimal
	}{
		{types.NewDecFromInt(3)},
		{types.NewDecFromFloatForTest(0.03)},
	}
	for _, v := range inputs {
		var datum types.Datum
		datum.SetMysqlDecimal(v.Input)

		b, err := EncodeDecimal([]byte{}, datum.GetMysqlDecimal(), datum.Length(), datum.Frac())
		require.NoError(t, err)

		_, dec, _, _, err := DecodeDecimal(b)
		require.NoError(t, err)
		require.Equal(t, v.Input.String(), dec.String())
	}
}
