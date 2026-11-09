package types

import (
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/parser/mysql"
	parsertypes "github.com/pingcap/tidb/pkg/parser/types"
	contextutil "github.com/pingcap/tidb/pkg/util/context"
	"github.com/stretchr/testify/require"
)

func TestFormatIntervalYearToMonth(t *testing.T) {
	require.Equal(t, "2-11", string(FormatIntervalYearToMonth(35)))
	require.Equal(t, "-1-1", string(FormatIntervalYearToMonth(-13)))
	require.Equal(t, "0-0", string(FormatIntervalYearToMonth(0)))
	require.Equal(t, "-0-1", string(FormatIntervalYearToMonth(-1)))
}

func TestFormatIntervalDayToSecond(t *testing.T) {
	require.Equal(t, "2 03:04:05", string(FormatIntervalDayToSecond(183845)))
	require.Equal(t, "-0 00:00:01", string(FormatIntervalDayToSecond(-1)))
	require.Equal(t, "0 00:00:00", string(FormatIntervalDayToSecond(0)))
}

func TestConvertToIntervalYearToMonthInt(t *testing.T) {
	parsertypes.EnableExtraDataType.Store(true)
	t.Cleanup(func() {
		parsertypes.EnableExtraDataType.Store(false)
	})

	ctx := NewContext(StrictFlags, time.UTC, contextutil.NewStaticWarnHandler(0))
	ft := NewFieldType(mysql.TypeLong)
	ft.SetFlen(7)
	ft.SetSubType(mysql.SubTypeIntervalYearToMonth)

	d := NewStringDatum("2-11")
	casted, err := d.ConvertTo(ctx, ft)
	require.NoError(t, err)
	require.Equal(t, int64(35), casted.GetInt64())

	d = NewStringDatum("-1-1")
	casted, err = d.ConvertTo(ctx, ft)
	require.NoError(t, err)
	require.Equal(t, int64(-13), casted.GetInt64())

	d = NewStringDatum("invalid")
	_, err = d.ConvertTo(ctx, ft)
	require.Error(t, err)
}

func TestConvertToIntervalDayToSecondInt(t *testing.T) {
	parsertypes.EnableExtraDataType.Store(true)
	t.Cleanup(func() {
		parsertypes.EnableExtraDataType.Store(false)
	})

	ctx := NewContext(StrictFlags, time.UTC, contextutil.NewStaticWarnHandler(0))
	ft := NewFieldType(mysql.TypeLong)
	ft.SetFlen(9)
	ft.SetSubType(mysql.SubTypeIntervalDayToSecond)

	d := NewStringDatum("2 03:04:05")
	casted, err := d.ConvertTo(ctx, ft)
	require.NoError(t, err)
	require.Equal(t, int64(183845), casted.GetInt64())

	d = NewStringDatum("-1 01:01:01")
	casted, err = d.ConvertTo(ctx, ft)
	require.NoError(t, err)
	require.Equal(t, int64(-90061), casted.GetInt64())

	d = NewStringDatum("1 24:00:00")
	_, err = d.ConvertTo(ctx, ft)
	require.Error(t, err)
}

func TestConvertIntervalStringToInt(t *testing.T) {
	t.Run("YearToMonth", func(t *testing.T) {
		cases := []struct {
			in   string
			p    int
			want int64
		}{
			{in: "2-11", p: 3, want: 35},
			{in: "-1-1", p: 3, want: -13},
			{in: "-0-0", p: 3, want: 0},
			{in: " 2 - 11 ", p: 3, want: 35},
			{in: "+2-11", p: 3, want: 35},
		}
		for _, ca := range cases {
			got, err := ConvertIntervalStringToInt(ca.in, mysql.SubTypeIntervalYearToMonth, ca.p)
			require.NoError(t, err, "input: %q", ca.in)
			require.Equal(t, ca.want, got, "input: %q", ca.in)
		}

		invalidCases := []string{
			"invalid",
			"88-18",
			"1000-1",
			"1-12",
			"1-123",
			"",
			"- 1-1",
			" ",
		}
		for _, in := range invalidCases {
			_, err := ConvertIntervalStringToInt(in, mysql.SubTypeIntervalYearToMonth, 3)
			require.EqualError(t, err, "[types:8801]Invalid Interval value", "input: %q", in)
		}
	})

	t.Run("DayToSecond", func(t *testing.T) {
		cases := []struct {
			in   string
			p    int
			want int64
		}{
			{in: "2 03:04:05", p: 3, want: 183845},
			{in: "-1 01:01:01", p: 3, want: -90061},
			{in: "-0 00:00:00", p: 3, want: 0},
			{in: " 02  3:4:5 ", p: 3, want: 183845},
			{in: "-1 1:1:1", p: 3, want: -90061},
			{in: "+2 03:04:05", p: 3, want: 183845},
		}
		for _, ca := range cases {
			got, err := ConvertIntervalStringToInt(ca.in, mysql.SubTypeIntervalDayToSecond, ca.p)
			require.NoError(t, err, "input: %q", ca.in)
			require.Equal(t, ca.want, got, "input: %q", ca.in)
		}

		invalidCases := []string{
			"invalid",
			"",
			"- 1 01:01:01",
			"1 24:00:00",
			"1 00:60:00",
			"1 00:00:60",
			"1000 00:00:00",
			"1 00:00",
			"1 00:00:00.1",
			" ",
		}
		for _, in := range invalidCases {
			_, err := ConvertIntervalStringToInt(in, mysql.SubTypeIntervalDayToSecond, 3)
			require.EqualError(t, err, "[types:8801]Invalid Interval value", "input: %q", in)
		}
	})
}
