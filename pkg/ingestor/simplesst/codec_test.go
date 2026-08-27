// Copyright 2023 PingCAP, Inc.
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

package simplesst

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRangePropertyCodec(t *testing.T) {
	prop := &RangeProperty{
		FirstKey: []byte("key"),
		LastKey:  []byte("key2"),
		Offset:   1,
		Size:     2,
		Keys:     3,
	}
	buf := encodeProp(nil, prop)
	prop2 := decodeProp(buf)
	require.EqualValues(t, prop, prop2)

	p1, p2, p3 := &RangeProperty{}, &RangeProperty{}, &RangeProperty{}
	for i, p := range []*RangeProperty{p1, p2, p3} {
		p.FirstKey = fmt.Appendf(nil, "key%d", i)
		p.LastKey = fmt.Appendf(nil, "key%d9", i)
		p.Offset = uint64(10 * i)
		p.Size = uint64(20 * i)
		p.Keys = uint64(30 * i)
	}
	buf = encodeMultiProps(nil, []*RangeProperty{p1, p2, p3})
	props := decodeMultiProps(buf)
	require.EqualValues(t, []*RangeProperty{p1, p2, p3}, props)
}

func TestPropertyLengthExceptKeys(t *testing.T) {
	zero := &RangeProperty{}
	bs := encodeProp(nil, zero)
	require.EqualValues(t, propertyLengthExceptKeys, len(bs))
}
