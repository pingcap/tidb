// Copyright 2026 PingCAP, Inc.
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

package kvsource

import (
	"context"

	"github.com/pingcap/tidb/dumpling/dumpservice"
)

// Scanner reads logical TiDB keys from a single service-owned snapshot.
type Scanner interface {
	Shards(ctx context.Context, startKey, endKey []byte) ([]dumpservice.Range, error)
	Scan(ctx context.Context, startKey, endKey []byte) (*dumpservice.Scan, error)
}

func scanRange(
	ctx context.Context,
	scanner Scanner,
	startKey, endKey []byte,
	emit func(key, value []byte) error,
) error {
	scan, err := scanner.Scan(ctx, startKey, endKey)
	if err != nil {
		return err
	}
	defer func() { _ = scan.Close() }()
	var keyBuffer, valueBuffer []byte
	for {
		key, value, end, err := scan.ReadRow(keyBuffer, valueBuffer)
		if err != nil {
			return err
		}
		if end {
			return nil
		}
		if err := emit(key, value); err != nil {
			return err
		}
		keyBuffer = key
		valueBuffer = value
	}
}
