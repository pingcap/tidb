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

package external

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRangePropertyCodec(t *testing.T) {
	prop := &rangeProperty{
		firstKey: []byte("key"),
		lastKey:  []byte("key2"),
		offset:   1,
		size:     2,
		keys:     3,
	}
	buf := encodeProp(nil, prop)
	prop2 := decodeProp(buf)
	require.EqualValues(t, prop, prop2)

	p1, p2, p3 := &rangeProperty{}, &rangeProperty{}, &rangeProperty{}
	for i, p := range []*rangeProperty{p1, p2, p3} {
		p.firstKey = []byte(fmt.Sprintf("key%d", i))
		p.lastKey = []byte(fmt.Sprintf("key%d9", i))
		p.offset = uint64(10 * i)
		p.size = uint64(20 * i)
		p.keys = uint64(30 * i)
	}
	buf = encodeMultiProps(nil, []*rangeProperty{p1, p2, p3})
	props := decodeMultiProps(buf)
	require.EqualValues(t, []*rangeProperty{p1, p2, p3}, props)
}

func TestPropertyLengthExceptKeys(t *testing.T) {
	zero := &rangeProperty{}
	bs := encodeProp(nil, zero)
	require.EqualValues(t, propertyLengthExceptKeys, len(bs))
}
