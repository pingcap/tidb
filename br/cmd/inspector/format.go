// Copyright 2025 PingCAP, Inc. Licensed under Apache-2.0.

package main

import "fmt"

func formatBytes(n uint64) string {
	const (
		kb = 1 << 10
		mb = 1 << 20
		gb = 1 << 30
		tb = 1 << 40
	)
	switch {
	case n >= tb:
		return fmt.Sprintf("%.2f TB", float64(n)/tb)
	case n >= gb:
		return fmt.Sprintf("%.2f GB", float64(n)/gb)
	case n >= mb:
		return fmt.Sprintf("%.2f MB", float64(n)/mb)
	case n >= kb:
		return fmt.Sprintf("%.2f KB", float64(n)/kb)
	default:
		return fmt.Sprintf("%d B", n)
	}
}
