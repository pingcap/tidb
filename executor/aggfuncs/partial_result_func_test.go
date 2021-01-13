package aggfuncs

import (
	"reflect"
	"testing"

	"github.com/pingcap/tidb/types"
)

func TestPRCoder(t *testing.T) {
	sf := new(sum4Float64)
	sfp := &partialResult4SumFloat64{
		val:             123.456,
		notNullRowCount: 2345678,
	}
	var buf []byte
	var err error
	buf, err = sf.DumpTo(PartialResult(sfp), buf)
	if err != nil {
		panic(err)
	}
	pr2, _, err := sf.LoadFrom(buf)
	if err != nil {
		panic(err)
	}
	sfp2 := (*partialResult4SumFloat64)(pr2)
	if !reflect.DeepEqual(sfp2, sfp) {
		panic(nil)
	}

	sd := new(sum4Decimal)
	sdp := &partialResult4SumDecimal{
		val:             *types.NewDecFromFloatForTest(123.456),
		notNullRowCount: 213123}
	buf = buf[:0]
	buf, err = sd.DumpTo(PartialResult(sdp), buf)
	if err != nil {
		panic(err)
	}
	pr2, _, err = sd.LoadFrom(buf)
	if err != nil {
		panic(err)
	}
	sdp2 := (*partialResult4SumDecimal)(pr2)
	if !reflect.DeepEqual(sdp2, sdp) {
		panic(nil)
	}
}
