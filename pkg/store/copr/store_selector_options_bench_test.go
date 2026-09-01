package copr

import (
	"fmt"
	"testing"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/tikv/client-go/v2/tikv"
)

var benchmarkStoreSelectorOptionsSink []tikv.StoreSelectorOption

func BenchmarkStoreSelectorOptionsRandomRanges(b *testing.B) {
	redirect := uint64(42)
	cases := []struct {
		name     string
		labels   []*metapb.StoreLabel
		redirect *uint64
	}{
		{name: "none"},
		{name: "labels", labels: []*metapb.StoreLabel{{Key: "zone", Value: "z1"}}},
		{name: "redirect", redirect: &redirect},
		{name: "both", labels: []*metapb.StoreLabel{{Key: "zone", Value: "z1"}}, redirect: &redirect},
	}
	factories := []struct {
		name string
		fn   func([]*metapb.StoreLabel, *uint64) []tikv.StoreSelectorOption
	}{
		{name: "legacy", fn: legacyStoreSelectorOptions},
		{name: "conditional", fn: conditionalStoreSelectorOptions},
	}

	for _, tc := range cases {
		for _, factory := range factories {
			name := fmt.Sprintf("%s/%s", tc.name, factory.name)
			b.Run(name, func(b *testing.B) {
				ops := factory.fn(tc.labels, tc.redirect)
				expected := 0
				if len(tc.labels) > 0 {
					expected++
				}
				if tc.redirect != nil {
					expected++
				}
				if len(ops) != expected {
					b.Fatalf("unexpected option count: %d", len(ops))
				}

				b.ReportAllocs()
				b.ResetTimer()
				for range b.N {
					ops = factory.fn(tc.labels, tc.redirect)
				}
				b.StopTimer()

				benchmarkStoreSelectorOptionsSink = ops
			})
		}
	}
}

func legacyStoreSelectorOptions(
	labels []*metapb.StoreLabel,
	redirect *uint64,
) []tikv.StoreSelectorOption {
	ops := make([]tikv.StoreSelectorOption, 0, 2)
	if len(labels) > 0 {
		ops = append(ops, tikv.WithMatchLabels(labels))
	}
	if redirect != nil {
		ops = append(ops, tikv.WithMatchStores([]uint64{*redirect}))
	}
	return ops
}

func conditionalStoreSelectorOptions(
	labels []*metapb.StoreLabel,
	redirect *uint64,
) []tikv.StoreSelectorOption {
	return buildStoreSelectorOptions(labels, redirect)
}
