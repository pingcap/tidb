// Copyright 2015 PingCAP, Inc.
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

// Copyright 2013 The Go-MySQL-Driver Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

// The MIT License (MIT)
//
// Copyright (c) 2014 wandoulabs
// Copyright (c) 2014 siddontang
//
// Permission is hereby granted, free of charge, to any person obtaining a copy of
// this software and associated documentation files (the "Software"), to deal in
// the Software without restriction, including without limitation the rights to
// use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
// the Software, and to permit persons to whom the Software is furnished to do so,
// subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.

package util

import (
	"bytes"
	"io"
	"math"
	"net/http"
	"strconv"

	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/parser/charset"
)

// ParseNullTermString parses a null terminated string.
func ParseNullTermString(b []byte) (str []byte, remain []byte) {
	off := bytes.IndexByte(b, 0)
	if off == -1 {
		return nil, b
	}
	return b[:off], b[off+1:]
}

// ParseLengthEncodedInt parses a length encoded integer.
func ParseLengthEncodedInt(b []byte) (num uint64, isNull bool, n int) {
	switch b[0] {
	// 251: NULL
	case 0xfb:
		n = 1
		isNull = true
		return

	// 252: value of following 2
	case 0xfc:
		num = uint64(b[1]) | uint64(b[2])<<8
		n = 3
		return

	// 253: value of following 3
	case 0xfd:
		num = uint64(b[1]) | uint64(b[2])<<8 | uint64(b[3])<<16
		n = 4
		return

	// 254: value of following 8
	case 0xfe:
		num = uint64(b[1]) | uint64(b[2])<<8 | uint64(b[3])<<16 |
			uint64(b[4])<<24 | uint64(b[5])<<32 | uint64(b[6])<<40 |
			uint64(b[7])<<48 | uint64(b[8])<<56
		n = 9
		return
	}

	// https://dev.mysql.com/doc/internals/en/integer.html#length-encoded-integer: If the first byte of a packet is a length-encoded integer and its byte value is 0xfe, you must check the length of the packet to verify that it has enough space for a 8-byte integer.
	// TODO: 0xff is undefined

	// 0-250: value of first byte
	num = uint64(b[0])
	n = 1
	return
}

// ParseLengthEncodedBytes parses a length encoded byte slice.
func ParseLengthEncodedBytes(b []byte) ([]byte, bool, int, error) {
	// Get length
	num, isNull, n := ParseLengthEncodedInt(b)
	if num < 1 {
		return nil, isNull, n, nil
	}

	n += int(num)

	// Check data length
	if len(b) >= n {
		return b[n-int(num) : n], false, n, nil
	}

	return nil, false, n, io.EOF
}

// InputDecoder is used to decode input.
type InputDecoder struct {
	encoding charset.Encoding
}

// NewInputDecoder creates a new InputDecoder.
func NewInputDecoder(chs string) *InputDecoder {
	return &InputDecoder{
		encoding: charset.FindEncodingTakeUTF8AsNoop(chs),
	}
}

// DecodeInput decodes input.
func (i *InputDecoder) DecodeInput(src []byte) []byte {
	result, err := i.encoding.Transform(nil, src, charset.OpDecode)
	if err != nil {
		return src
	}
	return result
}

// LengthEncodedIntSize returns the size of length encoded integer.
func LengthEncodedIntSize(n uint64) int {
	switch {
	case n <= 250:
		return 1

	case n <= 0xffff:
		return 3

	case n <= 0xffffff:
		return 4
	}

	return 9
}

const (
	expFormatBig     = 1e15
	expFormatSmall   = 1e-15
	defaultMySQLPrec = 5
)

// AppendFormatFloat appends a float64 to dst in MySQL format.
func AppendFormatFloat(in []byte, fVal float64, prec, bitSize int) []byte {
	absVal := math.Abs(fVal)
	if absVal > math.MaxFloat64 || math.IsNaN(absVal) {
		return []byte{'0'}
	}
	isEFormat := false
	if bitSize == 32 {
		isEFormat = float32(absVal) >= expFormatBig || (float32(absVal) != 0 && float32(absVal) < expFormatSmall)
	} else {
		isEFormat = absVal >= expFormatBig || (absVal != 0 && absVal < expFormatSmall)
	}
	var out []byte
	if isEFormat {
		if bitSize == 32 {
			prec = defaultMySQLPrec
		}
		out = strconv.AppendFloat(in, fVal, 'e', prec, bitSize)
		valStr := out[len(in):]
		// remove the '+' from the string for compatibility.
		plusPos := bytes.IndexByte(valStr, '+')
		if plusPos > 0 {
			plusPosInOut := len(in) + plusPos
			out = append(out[:plusPosInOut], out[plusPosInOut+1:]...)
		}
		// remove extra '0'
		ePos := bytes.IndexByte(valStr, 'e')
		pointPos := bytes.IndexByte(valStr, '.')
		ePosInOut := len(in) + ePos
		pointPosInOut := len(in) + pointPos
		validPos := ePosInOut
		for i := ePosInOut - 1; i >= pointPosInOut; i-- {
			if !(out[i] == '0' || out[i] == '.') {
				break
			}
			validPos = i
		}
		out = append(out[:validPos], out[ePosInOut:]...)
	} else {
		out = strconv.AppendFloat(in, fVal, 'f', prec, bitSize)
	}
	return out
}

// CorsHandler adds Cors Header if `cors` config is set.
type CorsHandler struct {
	handler http.Handler
	cfg     *config.Config
}

// NewCorsHandler creates a new CorsHandler.
func NewCorsHandler(handler http.Handler, cfg *config.Config) http.Handler {
	return CorsHandler{handler: handler, cfg: cfg}
}

// ServeHTTP implements http.Handler interface.
func (h CorsHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if h.cfg.Cors != "" {
		w.Header().Set("Access-Control-Allow-Origin", h.cfg.Cors)
		w.Header().Set("Access-Control-Allow-Methods", "GET")
	}
	h.handler.ServeHTTP(w, req)
}

// NewTestConfig creates a new config for test.
func NewTestConfig() *config.Config {
	cfg := config.NewConfig()
	cfg.Host = "127.0.0.1"
	cfg.Status.StatusHost = "127.0.0.1"
	cfg.Security.AutoTLS = false
	cfg.Socket = ""
	return cfg
}
