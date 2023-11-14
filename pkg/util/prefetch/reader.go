// Copyright 2023 PingCAP, Inc. Licensed under Apache-2.0.

package prefetch

import (
	"bytes"
	"io"
)

type PrefetchReader struct {
	r            io.ReadCloser
	curBufReader *bytes.Reader
	buf          [2][]byte
	bufIdx       int
	bufCh        chan []byte
	err          error // after bufCh is closed
}

func NewPrefetchReader(r io.ReadCloser, prefetchSize int) io.ReadCloser {
	ret := &PrefetchReader{
		r:     r,
		bufCh: make(chan []byte, 0),
		err:   nil,
	}
	ret.buf[0] = make([]byte, prefetchSize)
	ret.buf[1] = make([]byte, prefetchSize)
	go ret.run()
	return ret
}

func (r *PrefetchReader) run() {
	for {
		r.bufIdx = (r.bufIdx + 1) % 2
		buf := r.buf[r.bufIdx]
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
		if r.curBufReader == nil {
			b, ok := <-r.bufCh
			if !ok {
				if total > 0 {
					return total, nil
				}
				return 0, r.err
			}

			r.curBufReader = bytes.NewReader(b)
		}

		expected := len(data)
		n, err := r.curBufReader.Read(data)
		total += n
		if n == expected {
			return total, nil
		}

		data = data[n:]
		if err == io.EOF {
			r.curBufReader = nil
			continue
		}
	}
}

func (r *PrefetchReader) Close() error {
	return r.r.Close()
}
