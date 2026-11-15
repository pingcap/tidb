// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package pkdbspfreshbulk

import (
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

func BenchmarkDecodeSortedRun(b *testing.B) {
	for _, valueBytes := range []int{32, 684, 4096} {
		b.Run(fmt.Sprintf("value_%d", valueBytes), func(b *testing.B) {
			const recordCount = 10000
			records := make([]SortedRunRecord, recordCount)
			for i := range records {
				key := make([]byte, 32)
				binary.BigEndian.PutUint64(key[len(key)-8:], uint64(i))
				value := make([]byte, valueBytes)
				for j := range value {
					value[j] = byte(i + j)
				}
				records[i] = SortedRunRecord{Key: key, Value: value}
			}
			header := sortedRunHeader("bench", "tikv-worker", "non_root", 17, records)
			header.LeafEntryCount = recordCount
			encoded, err := EncodeSortedRun(header, records)
			if err != nil {
				b.Fatal(err)
			}
			stages := []string{"envelope", "header", "records", "validate"}
			before := make([]float64, len(stages))
			for i, stage := range stages {
				before[i] = artifactStageSeconds(b, metrics.SPFreshBulkSortedRunDecodeDuration, stage)
			}
			b.SetBytes(int64(len(encoded)))
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				decodedHeader, decoded, err := DecodeSortedRun(encoded)
				if err != nil {
					b.Fatal(err)
				}
				if decodedHeader.RecordCount != recordCount || len(decoded) != recordCount ||
					decoded[len(decoded)-1].Value[valueBytes-1] != byte((recordCount-1+valueBytes-1)%256) {
					b.Fatal("decoded records changed")
				}
			}
			b.StopTimer()
			for i, stage := range stages {
				b.ReportMetric((artifactStageSeconds(b, metrics.SPFreshBulkSortedRunDecodeDuration, stage)-before[i])*1e9/float64(b.N), stage+"-ns/op")
			}
		})
	}
}

func artifactStageSeconds(b *testing.B, histogram *prometheus.HistogramVec, stage string) float64 {
	b.Helper()
	metric := &dto.Metric{}
	if err := histogram.WithLabelValues(stage).(prometheus.Metric).Write(metric); err != nil {
		b.Fatal(err)
	}
	return metric.GetHistogram().GetSampleSum()
}

func BenchmarkDecodeRaBitQFragment(b *testing.B) {
	for _, dimension := range []uint32{64, 768, 4096} {
		b.Run(fmt.Sprintf("dimension_%d", dimension), func(b *testing.B) {
			const recordCount = 10000
			header, records := rabitqFragmentSample(dimension, recordCount)
			encoded, err := EncodeRaBitQFragment(header, records)
			if err != nil {
				b.Fatal(err)
			}
			stages := []string{"envelope", "header", "records", "validate"}
			before := make([]float64, len(stages))
			for i, stage := range stages {
				before[i] = artifactStageSeconds(b, metrics.SPFreshBulkRaBitQFragmentDecodeDuration, stage)
			}
			b.SetBytes(int64(len(encoded)))
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				decodedHeader, decoded, err := DecodeRaBitQFragment(encoded)
				if err != nil {
					b.Fatal(err)
				}
				last := decoded[len(decoded)-1]
				if decodedHeader.RecordCount != recordCount || len(decoded) != recordCount ||
					last.SourceDigestSHA256 != records[recordCount-1].SourceDigestSHA256 ||
					last.EncodedVector[len(last.EncodedVector)-1] != records[recordCount-1].EncodedVector[len(last.EncodedVector)-1] {
					b.Fatal("decoded records changed")
				}
			}
			b.StopTimer()
			for i, stage := range stages {
				b.ReportMetric((artifactStageSeconds(b, metrics.SPFreshBulkRaBitQFragmentDecodeDuration, stage)-before[i])*1e9/float64(b.N), stage+"-ns/op")
			}
		})
	}
}
