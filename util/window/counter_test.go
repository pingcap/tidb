// Copyright 2022 PingCAP, Inc.
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

package window

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRollingCounterAdd(t *testing.T) {
	size := 3
	bucketDuration := time.Second
	opts := RollingCounterOpts{
		Size:           size,
		BucketDuration: bucketDuration,
	}
	r := NewRollingCounter[float64](opts)
	listBuckets := func() [][]float64 {
		buckets := make([][]float64, 0)
		r.Reduce(func(i BucketIterator[float64]) float64 {
			for i.HasNext() {
				bucket := i.Bucket()
				buckets = append(buckets, bucket.Points)
			}
			return 0.0
		})
		return buckets
	}
	require.Equal(t, [][]float64{{}, {}, {}}, listBuckets())
	r.Add(1)
	require.Equal(t, [][]float64{{}, {}, {1}}, listBuckets())
	time.Sleep(time.Second)
	r.Add(2)
	r.Add(3)
	require.Equal(t, [][]float64{{}, {1}, {5}}, listBuckets())
	time.Sleep(time.Second)
	r.Add(4)
	r.Add(5)
	r.Add(6)
	require.Equal(t, [][]float64{{1}, {5}, {15}}, listBuckets())
	time.Sleep(time.Second)
	r.Add(7)
	require.Equal(t, [][]float64{{5}, {15}, {7}}, listBuckets())
}

func TestRollingCounterReduce(t *testing.T) {
	size := 3
	bucketDuration := time.Second
	opts := RollingCounterOpts{
		Size:           size,
		BucketDuration: bucketDuration,
	}
	r := NewRollingCounter[float64](opts)
	for x := 0; x < size; x = x + 1 {
		for i := 0; i <= x; i++ {
			r.Add(1)
		}
		if x < size-1 {
			time.Sleep(bucketDuration)
		}
	}
	var result = r.Reduce(func(iterator BucketIterator[float64]) float64 {
		var result float64
		for iterator.HasNext() {
			bucket := iterator.Bucket()
			result += bucket.Points[0]
		}
		return result
	})
	if result != 6.0 {
		t.Fatalf("Validate sum of points. result: %f", result)
	}
}

func TestRollingCounterDataRace(t *testing.T) {
	size := 3
	bucketDuration := time.Millisecond * 10
	opts := RollingCounterOpts{
		Size:           size,
		BucketDuration: bucketDuration,
	}
	r := NewRollingCounter[float64](opts)
	var stop = make(chan bool)
	go func() {
		for {
			select {
			case <-stop:
				return
			default:
				r.Add(1)
				time.Sleep(time.Millisecond * 5)
			}
		}
	}()
	go func() {
		for {
			select {
			case <-stop:
				return
			default:
				_ = r.Reduce(func(i BucketIterator[float64]) float64 {
					for i.HasNext() {
						bucket := i.Bucket()
						for range bucket.Points {
							continue
						}
					}
					return 0
				})
			}
		}
	}()
	time.Sleep(time.Second * 3)
	close(stop)
}

func BenchmarkRollingCounterIncr(b *testing.B) {
	size := 3
	bucketDuration := time.Millisecond * 100
	opts := RollingCounterOpts{
		Size:           size,
		BucketDuration: bucketDuration,
	}
	r := NewRollingCounter[float64](opts)
	b.ResetTimer()
	for i := 0; i <= b.N; i++ {
		r.Add(1)
	}
}

func BenchmarkRollingCounterReduce(b *testing.B) {
	size := 3
	bucketDuration := time.Second
	opts := RollingCounterOpts{
		Size:           size,
		BucketDuration: bucketDuration,
	}
	r := NewRollingCounter[float64](opts)
	for i := 0; i <= 10; i++ {
		r.Add(1)
		time.Sleep(time.Millisecond * 500)
	}
	b.ResetTimer()
	for i := 0; i <= b.N; i++ {
		var _ = r.Reduce(func(i BucketIterator[float64]) float64 {
			var result float64
			for i.HasNext() {
				bucket := i.Bucket()
				if len(bucket.Points) != 0 {
					result += bucket.Points[0]
				}
			}
			return result
		})
	}
}
