// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package utils

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"io"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/logutil"
	"github.com/pingcap/tidb/kv"
	"go.uber.org/zap"
)

// ParseKey parse key by given format.
func ParseKey(format, key string) ([]byte, error) {
	switch format {
	case "raw":
		return []byte(key), nil
	case "escaped":
		return unescapedKey(key)
	case "hex":
		key, err := hex.DecodeString(key)
		if err != nil {
			return nil, errors.Trace(err)
		}
		return key, nil
	}
	return nil, errors.Annotate(berrors.ErrInvalidArgument, "unknown format")
}

// Ref PD: https://github.com/pingcap/pd/blob/master/tools/pd-ctl/pdctl/command/region_command.go#L334
func unescapedKey(text string) ([]byte, error) {
	var buf []byte
	r := bytes.NewBuffer([]byte(text))
	for {
		c, err := r.ReadByte()
		if err != nil {
			if errors.Cause(err) != io.EOF { // nolint:errorlint
				return nil, errors.Trace(err)
			}
			break
		}
		if c != '\\' {
			buf = append(buf, c)
			continue
		}
		n := r.Next(1)
		if len(n) == 0 {
			return nil, io.EOF
		}
		// See: https://golang.org/ref/spec#Rune_literals
		if idx := strings.IndexByte(`abfnrtv\'"`, n[0]); idx != -1 {
			buf = append(buf, []byte("\a\b\f\n\r\t\v\\'\"")[idx])
			continue
		}

		switch n[0] {
		case 'x':
			fmt.Sscanf(string(r.Next(2)), "%02x", &c)
			buf = append(buf, c)
		default:
			n = append(n, r.Next(2)...)
			_, err := fmt.Sscanf(string(n), "%03o", &c)
			if err != nil {
				return nil, errors.Trace(err)
			}
			buf = append(buf, c)
		}
	}
	return buf, nil
}

// CompareEndKey compared two keys that BOTH represent the EXCLUSIVE ending of some range. An empty end key is the very
// end, so an empty key is greater than any other keys.
// Please note that this function is not applicable if any one argument is not an EXCLUSIVE ending of a range.
func CompareEndKey(a, b []byte) int {
	// NOTE: maybe CompareBytesExt(a, true, b, true)?
	if len(a) == 0 {
		if len(b) == 0 {
			return 0
		}
		return 1
	}

	if len(b) == 0 {
		return -1
	}

	return bytes.Compare(a, b)
}

// CompareBytesExt compare two byte sequences.
// different from `bytes.Compare`, we can provide whether to treat the key as inf when meet empty key to this.
func CompareBytesExt(a []byte, aEmptyAsInf bool, b []byte, bEmptyAsInf bool) int {
	// Inf = Inf
	if len(a) == 0 && aEmptyAsInf && len(b) == 0 && bEmptyAsInf {
		return 0
	}
	// Inf > anything
	if len(a) == 0 && aEmptyAsInf {
		return 1
	}
	// anything < Inf
	if len(b) == 0 && bEmptyAsInf {
		return -1
	}
	return bytes.Compare(a, b)
}

type failedToClampReason int

const (
	successClamp failedToClampReason = iota
	// ToClamp :              |_________|
	// Range:       |______|
	leftNotOverlapped
	// ToClamp :    |_________|
	// Range:                    |______|
	rightNotOverlapped
	buggyUnknown
)

func clampInOneRange(rng kv.KeyRange, clampIn kv.KeyRange) (kv.KeyRange, failedToClampReason) {
	possibleFailureReason := buggyUnknown
	if bytes.Compare(rng.StartKey, clampIn.StartKey) < 0 {
		rng.StartKey = clampIn.StartKey
		possibleFailureReason = leftNotOverlapped
	}
	if CompareBytesExt(rng.EndKey, true, clampIn.EndKey, true) > 0 {
		rng.EndKey = clampIn.EndKey
		possibleFailureReason = rightNotOverlapped
	}
	// We treat empty region as "failed" too.
	if CompareBytesExt(rng.StartKey, false, rng.EndKey, true) >= 0 {
		return kv.KeyRange{}, possibleFailureReason
	}
	return rng, successClamp
}

// ClampRanges clamps the `ranges` into `toClampIn`.
// Example:
// ranges:    |___________|    |________________|
// toClampIn:   |_____| |____|   |________________|
// result:      |_____| |_|      |______________|
// we are assuming the arguments are sorted by the start key and no overlaps.
// you can call CollapseRanges to get key ranges fits this requirements.
// Note: this algorithm is pretty like the `checkIntervalIsSubset`, can we get them together?
func ClampRanges(ranges []kv.KeyRange, toClampIn []kv.KeyRange) []kv.KeyRange {
	currentClamping := 0
	currentClampTarget := 0
	clamped := make([]kv.KeyRange, 0, len(ranges))
	for currentClampTarget < len(toClampIn) && currentClamping < len(ranges) {
		cin := toClampIn[currentClampTarget]
		crg := ranges[currentClamping]
		rng, result := clampInOneRange(crg, cin)
		switch result {
		case successClamp:
			clamped = append(clamped, rng)
			// Not fully consumed the clamp range.
			if CompareBytesExt(crg.EndKey, true, cin.EndKey, true) <= 0 {
				currentClamping++
			} else {
				ranges[currentClamping].StartKey = cin.EndKey
			}
		case leftNotOverlapped:
			currentClamping++
		case rightNotOverlapped:
			currentClampTarget++
		case buggyUnknown:
			log.L().DPanic("Unreachable path reached",
				zap.Stringer("over-ranges", logutil.StringifyKeys(ranges)),
				zap.Stringer("clamp-into", logutil.StringifyKeys(toClampIn)),
				zap.Stringer("current-clamping", logutil.StringifyRange(crg)),
				zap.Stringer("current-target", logutil.StringifyRange(cin)),
			)
		}
	}
	return clamped
}
