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

package usage

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestInsertAndDelete(t *testing.T) {
	sl := NewSessionStatsList()
	var items []*SessionStatsItem
	for i := 0; i < 5; i++ {
		items = append(items, sl.NewSessionStatsItem())
	}
	items[0].Delete() // delete tail
	items[2].Delete() // delete middle
	items[4].Delete() // delete head
	sl.SweepSessionStatsList()

	require.Equal(t, items[3], sl.listHead.next)
	require.Equal(t, items[1], items[3].next)
	require.Nil(t, items[1].next)

	// delete rest
	items[1].Delete()
	items[3].Delete()
	sl.SweepSessionStatsList()
	require.Nil(t, sl.listHead.next)
}
