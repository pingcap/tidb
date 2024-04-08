package infoschema

import (
	"testing"

	"github.com/pingcap/tidb/pkg/infoschema/internal"
	"github.com/stretchr/testify/require"
)

func TestGetAndSet(t *testing.T) {
	items := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	cache := newSieve[int, int](10 * mb)

	for _, v := range items {
		cache.Set(v, v*10)
	}

	for _, v := range items {
		val, ok := cache.Get(v)
		require.True(t, ok)
		require.Equal(t, v*10, val)
	}

	cache.Close()
}

func TestRemove(t *testing.T) {
	cache := newSieve[int, int](10 * mb)
	cache.Set(1, 10)

	val, ok := cache.Get(1)
	require.True(t, ok)
	require.Equal(t, 10, val)

	// After removing the key, it should not be found
	removed := cache.Remove(1)
	require.True(t, removed)

	_, ok = cache.Get(1)
	require.False(t, ok)

	// This should not panic
	removed = cache.Remove(-1)
	require.False(t, removed)

	cache.Close()
}

func TestSievePolicy(t *testing.T) {
	var e entry[int, int]
	cache := newSieve[int, int](10 * e.Size())
	oneHitWonders := []int{1, 2, 3, 4, 5}
	popularObjects := []int{6, 7, 8, 9, 10}

	// add objects to the cache
	for _, v := range oneHitWonders {
		cache.Set(v, v)
	}
	for _, v := range popularObjects {
		cache.Set(v, v)
	}

	// hit popular objects
	for _, v := range popularObjects {
		_, ok := cache.Get(v)
		require.True(t, ok)
	}

	// add another objects to the cache
	for _, v := range oneHitWonders {
		cache.Set(v*10, v*10)
	}

	// check popular objects are not evicted
	for _, v := range popularObjects {
		_, ok := cache.Get(v)
		require.True(t, ok)
	}

	cache.Close()
}

func TestContains(t *testing.T) {
	cache := newSieve[string, string](10 * mb)
	require.False(t, cache.Contains("hello"))

	cache.Set("hello", "world")
	require.True(t, cache.Contains("hello"))

	cache.Close()
}

func TestCacheSize(t *testing.T) {
	var e entry[int, int]
	sz := e.Size()

	cache := newSieve[int, int](10 * mb)
	require.Equal(t, 0, cache.Size())

	cache.Set(1, 1)
	require.Equal(t, 1*sz, cache.Size())

	// duplicated keys only update the recent-ness of the key and value
	cache.Set(1, 1)
	require.Equal(t, 1*sz, cache.Size())

	cache.Set(2, 2)
	require.Equal(t, 2*sz, cache.Size())

	cache.Close()
}

func TestPurge(t *testing.T) {
	cache := newSieve[int, int](10 * mb)
	cache.Set(1, 1)
	cache.Set(2, 2)
	require.Equal(t, 2, cache.Len())

	cache.Purge()
	require.Equal(t, 0, cache.Len())

	cache.Close()
}

func TestSize(t *testing.T) {
	tests := []struct {
		name string
		v    any
		want int
	}{
		{
			name: "Array",
			v:    [3]int32{1, 2, 3}, // 3 * 4  = 12
			want: 12,
		},
		{
			name: "Slice",
			v:    make([]int64, 2, 5), // 5 * 8 + 24 = 64
			want: 64,
		},
		{
			name: "String",
			v:    "ABCdef", // 6 + 16 = 22
			want: 22,
		},
		{
			name: "Map",
			// (8 + 3 + 16) + (8 + 4 + 16) = 55
			// 55 + 8 + 10.79 * 2 = 84
			v:    map[int64]string{0: "ABC", 1: "DEFG"},
			want: 84,
		},
		{
			name: "Struct",
			v: struct {
				slice     []int64
				array     [2]bool
				structure struct {
					i int8
					s string
				}
			}{
				slice: []int64{12345, 67890}, // 2 * 8 + 24 = 40
				array: [2]bool{true, false},  // 2 * 1 = 2
				structure: struct {
					i int8
					s string
				}{
					i: 5,     // 1
					s: "abc", // 3 * 1 + 16 = 19
				}, // 20 + 7 (padding) = 27
			}, // 40 + 2 + 27 = 69 + 6 (padding) = 75
			want: 75,
		},
		{
			name: "Struct With Func",
			v:    entry[int, int]{},
			want: 40,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := internal.Sizeof(tt.v); got != tt.want {
				t.Errorf("Of() = %v, want %v", got, tt.want)
			}
		})
	}
}
