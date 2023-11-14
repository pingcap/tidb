// Copyright 2023 PingCAP, Inc. Licensed under Apache-2.0.

package prefetch

import (
	"bytes"
	"io"
)

type PrefetchReader struct {
	r            io.ReadCloser
	prefetchSize int
	curBuf       *bytes.Reader
	bufCh        chan []byte
	err          error // after bufCh is closed
}

func NewPrefetchReader(r io.ReadCloser, prefetchSize int) io.ReadCloser {
	ret := &PrefetchReader{
		r:            r,
		prefetchSize: prefetchSize,
		bufCh:        make(chan []byte, 1),
		err:          nil,
	}
	go ret.run()
	return ret
}

func (r *PrefetchReader) run() {
	for {
		buf := make([]byte, r.prefetchSize)
		n, err := r.r.Read(buf)
		buf = buf[:n]
		r.bufCh <- buf
		if err != nil {
			r.err = err
			close(r.bufCh)
			return
		}
	}
}

func (r *PrefetchReader) Read(data []byte) (int, error) {
	total := 0
	for {
		if r.curBuf == nil {
			b, ok := <-r.bufCh
			if !ok {
				if total > 0 {
					return total, nil
				}
				return 0, r.err
			}

			r.curBuf = bytes.NewReader(b)
		}

		expected := len(data)
		n, err := r.curBuf.Read(data)
		total += n
		if n == expected {
			return total, nil
		}

		data = data[n:]
		if err == io.EOF {
			r.curBuf = nil
			continue
		}
	}
}

func (r *PrefetchReader) Close() error {
	return r.r.Close()
}
