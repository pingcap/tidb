// Copyright 2014 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package prometheus

import (
	"fmt"
	"testing"

	dto "github.com/prometheus/client_model/go"
)

func TestDelete(t *testing.T) {
	vec := NewUntypedVec(
		UntypedOpts{
			Name: "test",
			Help: "helpless",
		},
		[]string{"l1", "l2"},
	)
	testDelete(t, vec)
}

func TestDeleteWithCollisions(t *testing.T) {
	vec := NewUntypedVec(
		UntypedOpts{
			Name: "test",
			Help: "helpless",
		},
		[]string{"l1", "l2"},
	)
	vec.hashAdd = func(h uint64, s string) uint64 { return 1 }
	vec.hashAddByte = func(h uint64, b byte) uint64 { return 1 }
	testDelete(t, vec)
}

func testDelete(t *testing.T, vec *UntypedVec) {
	if got, want := vec.Delete(Labels{"l1": "v1", "l2": "v2"}), false; got != want {
		t.Errorf("got %v, want %v", got, want)
	}

	vec.With(Labels{"l1": "v1", "l2": "v2"}).(Untyped).Set(42)
	if got, want := vec.Delete(Labels{"l1": "v1", "l2": "v2"}), true; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := vec.Delete(Labels{"l1": "v1", "l2": "v2"}), false; got != want {
		t.Errorf("got %v, want %v", got, want)
	}

	vec.With(Labels{"l1": "v1", "l2": "v2"}).(Untyped).Set(42)
	if got, want := vec.Delete(Labels{"l2": "v2", "l1": "v1"}), true; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := vec.Delete(Labels{"l2": "v2", "l1": "v1"}), false; got != want {
		t.Errorf("got %v, want %v", got, want)
	}

	vec.With(Labels{"l1": "v1", "l2": "v2"}).(Untyped).Set(42)
	if got, want := vec.Delete(Labels{"l2": "v1", "l1": "v2"}), false; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := vec.Delete(Labels{"l1": "v1"}), false; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestDeleteLabelValues(t *testing.T) {
	vec := NewUntypedVec(
		UntypedOpts{
			Name: "test",
			Help: "helpless",
		},
		[]string{"l1", "l2"},
	)
	testDeleteLabelValues(t, vec)
}

func TestDeleteLabelValuesWithCollisions(t *testing.T) {
	vec := NewUntypedVec(
		UntypedOpts{
			Name: "test",
			Help: "helpless",
		},
		[]string{"l1", "l2"},
	)
	vec.hashAdd = func(h uint64, s string) uint64 { return 1 }
	vec.hashAddByte = func(h uint64, b byte) uint64 { return 1 }
	testDeleteLabelValues(t, vec)
}

func testDeleteLabelValues(t *testing.T, vec *UntypedVec) {
	if got, want := vec.DeleteLabelValues("v1", "v2"), false; got != want {
		t.Errorf("got %v, want %v", got, want)
	}

	vec.With(Labels{"l1": "v1", "l2": "v2"}).(Untyped).Set(42)
	vec.With(Labels{"l1": "v1", "l2": "v3"}).(Untyped).Set(42) // Add junk data for collision.
	if got, want := vec.DeleteLabelValues("v1", "v2"), true; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := vec.DeleteLabelValues("v1", "v2"), false; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := vec.DeleteLabelValues("v1", "v3"), true; got != want {
		t.Errorf("got %v, want %v", got, want)
	}

	vec.With(Labels{"l1": "v1", "l2": "v2"}).(Untyped).Set(42)
	// Delete out of order.
	if got, want := vec.DeleteLabelValues("v2", "v1"), false; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := vec.DeleteLabelValues("v1"), false; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestMetricVec(t *testing.T) {
	vec := NewUntypedVec(
		UntypedOpts{
			Name: "test",
			Help: "helpless",
		},
		[]string{"l1", "l2"},
	)
	testMetricVec(t, vec)
}

func TestMetricVecWithCollisions(t *testing.T) {
	vec := NewUntypedVec(
		UntypedOpts{
			Name: "test",
			Help: "helpless",
		},
		[]string{"l1", "l2"},
	)
	vec.hashAdd = func(h uint64, s string) uint64 { return 1 }
	vec.hashAddByte = func(h uint64, b byte) uint64 { return 1 }
	testMetricVec(t, vec)
}

func testMetricVec(t *testing.T, vec *UntypedVec) {
	vec.Reset() // Actually test Reset now!

	var pair [2]string
	// Keep track of metrics.
	expected := map[[2]string]int{}

	for i := 0; i < 1000; i++ {
		pair[0], pair[1] = fmt.Sprint(i%4), fmt.Sprint(i%5) // Varying combinations multiples.
		expected[pair]++
		vec.WithLabelValues(pair[0], pair[1]).Inc()

		expected[[2]string{"v1", "v2"}]++
		vec.WithLabelValues("v1", "v2").(Untyped).Inc()
	}

	var total int
	for _, metrics := range vec.children {
		for _, metric := range metrics {
			total++
			copy(pair[:], metric.values)

			var metricOut dto.Metric
			if err := metric.metric.Write(&metricOut); err != nil {
				t.Fatal(err)
			}
			actual := *metricOut.Untyped.Value

			var actualPair [2]string
			for i, label := range metricOut.Label {
				actualPair[i] = *label.Value
			}

			// Test output pair against metric.values to ensure we've selected
			// the right one. We check this to ensure the below check means
			// anything at all.
			if actualPair != pair {
				t.Fatalf("unexpected pair association in metric map: %v != %v", actualPair, pair)
			}

			if actual != float64(expected[pair]) {
				t.Fatalf("incorrect counter value for %v: %v != %v", pair, actual, expected[pair])
			}
		}
	}

	if total != len(expected) {
		t.Fatalf("unexpected number of metrics: %v != %v", total, len(expected))
	}

	vec.Reset()

	if len(vec.children) > 0 {
		t.Fatalf("reset failed")
	}
}

func TestCounterVecEndToEndWithCollision(t *testing.T) {
	vec := NewCounterVec(
		CounterOpts{
			Name: "test",
			Help: "helpless",
		},
		[]string{"labelname"},
	)
	vec.WithLabelValues("77kepQFQ8Kl").Inc()
	vec.WithLabelValues("!0IC=VloaY").Add(2)

	m := &dto.Metric{}
	if err := vec.WithLabelValues("77kepQFQ8Kl").Write(m); err != nil {
		t.Fatal(err)
	}
	if got, want := m.GetLabel()[0].GetValue(), "77kepQFQ8Kl"; got != want {
		t.Errorf("got label value %q, want %q", got, want)
	}
	if got, want := m.GetCounter().GetValue(), 1.; got != want {
		t.Errorf("got value %f, want %f", got, want)
	}
	m.Reset()
	if err := vec.WithLabelValues("!0IC=VloaY").Write(m); err != nil {
		t.Fatal(err)
	}
	if got, want := m.GetLabel()[0].GetValue(), "!0IC=VloaY"; got != want {
		t.Errorf("got label value %q, want %q", got, want)
	}
	if got, want := m.GetCounter().GetValue(), 2.; got != want {
		t.Errorf("got value %f, want %f", got, want)
	}
}

func BenchmarkMetricVecWithLabelValuesBasic(b *testing.B) {
	benchmarkMetricVecWithLabelValues(b, map[string][]string{
		"l1": []string{"onevalue"},
		"l2": []string{"twovalue"},
	})
}

func BenchmarkMetricVecWithLabelValues2Keys10ValueCardinality(b *testing.B) {
	benchmarkMetricVecWithLabelValuesCardinality(b, 2, 10)
}

func BenchmarkMetricVecWithLabelValues4Keys10ValueCardinality(b *testing.B) {
	benchmarkMetricVecWithLabelValuesCardinality(b, 4, 10)
}

func BenchmarkMetricVecWithLabelValues2Keys100ValueCardinality(b *testing.B) {
	benchmarkMetricVecWithLabelValuesCardinality(b, 2, 100)
}

func BenchmarkMetricVecWithLabelValues10Keys100ValueCardinality(b *testing.B) {
	benchmarkMetricVecWithLabelValuesCardinality(b, 10, 100)
}

func BenchmarkMetricVecWithLabelValues10Keys1000ValueCardinality(b *testing.B) {
	benchmarkMetricVecWithLabelValuesCardinality(b, 10, 1000)
}

func benchmarkMetricVecWithLabelValuesCardinality(b *testing.B, nkeys, nvalues int) {
	labels := map[string][]string{}

	for i := 0; i < nkeys; i++ {
		var (
			k  = fmt.Sprintf("key-%v", i)
			vs = make([]string, 0, nvalues)
		)
		for j := 0; j < nvalues; j++ {
			vs = append(vs, fmt.Sprintf("value-%v", j))
		}
		labels[k] = vs
	}

	benchmarkMetricVecWithLabelValues(b, labels)
}

func benchmarkMetricVecWithLabelValues(b *testing.B, labels map[string][]string) {
	var keys []string
	for k := range labels { // Map order dependent, who cares though.
		keys = append(keys, k)
	}

	values := make([]string, len(labels)) // Value cache for permutations.
	vec := NewUntypedVec(
		UntypedOpts{
			Name: "test",
			Help: "helpless",
		},
		keys,
	)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Varies input across provide map entries based on key size.
		for j, k := range keys {
			candidates := labels[k]
			values[j] = candidates[i%len(candidates)]
		}

		vec.WithLabelValues(values...)
	}
}
