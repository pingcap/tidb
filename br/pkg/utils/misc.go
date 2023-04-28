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
// See the License for the specific language governing permissions and
// limitations under the License.

package utils

import (
	"context"
	"crypto/tls"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/metapb"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/types"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	// storeDisconnectionDuration is the max duration of a store to be treated as living.
	// when a store doesn't send heartbeat for 100s, it is probably offline, and most of leaders should be transformed.
	// (How about network partition between TiKV and PD? Even that is rare.)
	// Also note that the offline threshold in PD is 20s, see
	// https://github.com/tikv/pd/blob/c40e319f50822678cda71ae62ee2fd70a9cac010/pkg/core/store.go#L523
	storeDisconnectionDuration = 100 * time.Second
)

// IsTypeCompatible checks whether type target is compatible with type src
// they're compatible if
// - same null/not null and unsigned flag(maybe we can allow src not null flag, target null flag later)
// - have same evaluation type
// - target's flen and decimal should be bigger or equals to src's
// - elements in target is superset of elements in src if they're enum or set type
// - same charset and collate if they're string types
func IsTypeCompatible(src types.FieldType, target types.FieldType) bool {
	if mysql.HasNotNullFlag(src.GetFlag()) != mysql.HasNotNullFlag(target.GetFlag()) {
		return false
	}
	if mysql.HasUnsignedFlag(src.GetFlag()) != mysql.HasUnsignedFlag(target.GetFlag()) {
		return false
	}
	srcEType, dstEType := src.EvalType(), target.EvalType()
	if srcEType != dstEType {
		return false
	}

	getFLenAndDecimal := func(tp types.FieldType) (int, int) {
		// ref FieldType.CompactStr
		defaultFlen, defaultDecimal := mysql.GetDefaultFieldLengthAndDecimal(tp.GetType())
		flen, decimal := tp.GetFlen(), tp.GetDecimal()
		if flen == types.UnspecifiedLength {
			flen = defaultFlen
		}
		if decimal == types.UnspecifiedLength {
			decimal = defaultDecimal
		}
		return flen, decimal
	}
	srcFLen, srcDecimal := getFLenAndDecimal(src)
	targetFLen, targetDecimal := getFLenAndDecimal(target)
	if srcFLen > targetFLen || srcDecimal > targetDecimal {
		return false
	}

	// if they're not enum or set type, elems will be empty
	// and if they're not string types, charset and collate will be empty,
	// so we check them anyway.
	srcElems := src.GetElems()
	targetElems := target.GetElems()
	if len(srcElems) > len(targetElems) {
		return false
	}
	targetElemSet := make(map[string]struct{})
	for _, item := range targetElems {
		targetElemSet[item] = struct{}{}
	}
	for _, item := range srcElems {
		if _, ok := targetElemSet[item]; !ok {
			return false
		}
	}
	return src.GetCharset() == target.GetCharset() &&
		src.GetCollate() == target.GetCollate()
}

func GRPCConn(ctx context.Context, storeAddr string, tlsConf *tls.Config, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
	secureOpt := grpc.WithTransportCredentials(insecure.NewCredentials())
	if tlsConf != nil {
		secureOpt = grpc.WithTransportCredentials(credentials.NewTLS(tlsConf))
	}
	opts = append(opts,
		secureOpt,
		grpc.WithBlock(),
		grpc.FailOnNonTempDialError(true),
	)

	gctx, cancel := context.WithTimeout(ctx, time.Second*5)
	connection, err := grpc.DialContext(gctx, storeAddr, opts...)
	cancel()
	if err != nil {
		return nil, errors.Trace(err)
	}
	return connection, nil
}

// CheckStoreLiveness checks whether a store is still alive.
// Some versions of PD may not set the store state in the gRPC response.
// We need to check it manually.
func CheckStoreLiveness(s *metapb.Store) error {
	if s.State != metapb.StoreState_Up {
		return errors.Annotatef(berrors.ErrKVStorage, "the store state isn't up, it is %s", s.State)
	}
	// If the field isn't present (the default value), skip this check.
	if s.GetLastHeartbeat() > 0 {
		lastHeartBeat := time.Unix(0, s.GetLastHeartbeat())
		if sinceLastHB := time.Since(lastHeartBeat); sinceLastHB > storeDisconnectionDuration {
			return errors.Annotatef(berrors.ErrKVStorage, "the store last heartbeat is too far, at %s", sinceLastHB)
		}
	}
	return nil
}
