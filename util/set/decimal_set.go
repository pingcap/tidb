package set

import (
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/hack"
)

type DecimalSet map[string]*types.MyDecimal

func NewDecimalSet(ds ...*types.MyDecimal) DecimalSet {
	set := make(DecimalSet)
	for _, d := range ds {
		hash, err := d.ToHashKey()
		if err != nil {
			return nil
		}
		decStr := string(hack.String(hash))
		set.Insert(decStr, d)
	}
	return set
}

func (s DecimalSet) Exist(valStr string) bool {
	_, ok := s[valStr]
	return ok
}

func (s DecimalSet) Insert(valStr string, val *types.MyDecimal) {
	s[valStr] = val
}

func (s DecimalSet) Count() int {
	return len(s)
}
