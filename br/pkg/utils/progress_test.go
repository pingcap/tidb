// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package utils

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type testWriter struct {
	fn func(string)
}

func (t *testWriter) Write(p []byte) (int, error) {
	t.fn(string(p))
	return len(p), nil
}

func TestProgress(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	var p string
	pCh2 := make(chan string, 2)
	progress2 := NewProgressPrinter("test", 2, false)
	progress2.goPrintProgress(ctx, nil, &testWriter{
		fn: func(p string) { pCh2 <- p },
	})
	progress2.Inc()
	time.Sleep(2 * time.Second)
	p = <-pCh2
	require.Contains(t, p, `"P":"50.00%"`)
	progress2.Inc()
	time.Sleep(2 * time.Second)
	p = <-pCh2
	require.Contains(t, p, `"P":"100.00%"`)
	progress2.Inc()
	time.Sleep(2 * time.Second)
	p = <-pCh2
	require.Contains(t, p, `"P":"100.00%"`)
	progress2.Close()

	pCh4 := make(chan string, 4)
	progress4 := NewProgressPrinter("test", 4, false)
	progress4.goPrintProgress(ctx, nil, &testWriter{
		fn: func(p string) { pCh4 <- p },
	})
	progress4.Inc()
	time.Sleep(2 * time.Second)
	p = <-pCh4
	require.Contains(t, p, `"P":"25.00%"`)
	progress4.Inc()
	progress4.Close()
	time.Sleep(2 * time.Second)
	p = <-pCh4
	require.Contains(t, p, `"P":"100.00%"`)

	pCh8 := make(chan string, 8)
	progress8 := NewProgressPrinter("test", 8, false)
	progress8.goPrintProgress(ctx, nil, &testWriter{
		fn: func(p string) { pCh8 <- p },
	})
	progress8.Inc()
	progress8.Inc()
	time.Sleep(2 * time.Second)
	p = <-pCh8
	require.Contains(t, p, `"P":"25.00%"`)

	// Cancel should stop progress at the current position.
	cancel()
	p = <-pCh8
	require.Contains(t, p, `"P":"25.00%"`)
	progress8.Close()
}
