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

package ddl

import (
	"testing"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tidb/pkg/ddl/placement"
	"github.com/stretchr/testify/require"
)

// store builds a synthetic *metapb.Store for the version-check tests.
// Pass an empty version to simulate a store that hasn't reported one yet.
func storeAt(id uint64, version string, isTiFlash bool) *metapb.Store {
	s := &metapb.Store{Id: id, Version: version, Address: "mock"}
	if isTiFlash {
		s.Labels = []*metapb.StoreLabel{{Key: placement.EngineLabelKey, Value: placement.EngineLabelTiFlash}}
	}
	return s
}

func TestCheckStoresMeetDescIndexMinVersion(t *testing.T) {
	const minVer = "9.0.0"
	const failClosed = false // production semantics

	t.Run("all TiKV stores are new enough", func(t *testing.T) {
		stores := []*metapb.Store{
			storeAt(1, "9.0.0", false),
			storeAt(2, "9.1.0", false),
			storeAt(3, "v9.0.0", false), // leading-v normalisation
		}
		require.NoError(t, checkStoresMeetDescIndexMinVersion(stores, minVer, failClosed))
	})

	t.Run("an old TiKV store fails the gate", func(t *testing.T) {
		stores := []*metapb.Store{
			storeAt(1, "9.0.0", false),
			storeAt(2, "8.5.0", false),
		}
		err := checkStoresMeetDescIndexMinVersion(stores, minVer, failClosed)
		require.Error(t, err)
		require.Contains(t, err.Error(), "store 2")
		require.Contains(t, err.Error(), "8.5.0")
		require.Contains(t, err.Error(), minVer)
		require.Contains(t, err.Error(), "upgrade TiKV")
	})

	t.Run("TiFlash stores are excluded from the check", func(t *testing.T) {
		// A TiFlash store on an old version must not block the gate; the
		// check is for TiKV only because TiKV alone runs the coprocessor
		// path that needs the new decoder.
		stores := []*metapb.Store{
			storeAt(1, "9.0.0", false),
			storeAt(2, "1.0.0", true), // ancient TiFlash, ignored
		}
		require.NoError(t, checkStoresMeetDescIndexMinVersion(stores, minVer, failClosed))
	})

	t.Run("tombstone stores are skipped", func(t *testing.T) {
		old := storeAt(2, "8.0.0", false)
		old.State = metapb.StoreState_Tombstone
		stores := []*metapb.Store{
			storeAt(1, "9.0.0", false),
			old,
		}
		require.NoError(t, checkStoresMeetDescIndexMinVersion(stores, minVer, failClosed))
	})

	t.Run("empty version fails closed in production", func(t *testing.T) {
		// A store that hasn't reported a clean semver yet cannot prove it
		// can decode descending-order keys. The gate must reject the DDL
		// rather than fall through and risk silent corruption. The operator
		// can retry once PD has reconciled.
		stores := []*metapb.Store{
			storeAt(1, "9.0.0", false),
			storeAt(2, "", false),
		}
		err := checkStoresMeetDescIndexMinVersion(stores, minVer, failClosed)
		require.Error(t, err)
		require.Contains(t, err.Error(), "store 2")
		require.Contains(t, err.Error(), "has not reported a version")
	})

	t.Run("empty version is tolerated in tests", func(t *testing.T) {
		// In `intest` builds the mock store fixture reports empty versions
		// for every replica, so the gate must let DDL through to allow
		// integration tests to run.
		stores := []*metapb.Store{
			storeAt(1, "9.0.0", false),
			storeAt(2, "", false),
		}
		require.NoError(t, checkStoresMeetDescIndexMinVersion(stores, minVer, true))
	})

	t.Run("garbage version always fails closed", func(t *testing.T) {
		// Unparsable version strings are unambiguously a bug or a
		// misconfigured store; never tolerate them, even in tests.
		stores := []*metapb.Store{
			storeAt(1, "9.0.0", false),
			storeAt(2, "not-a-semver", false),
		}
		err := checkStoresMeetDescIndexMinVersion(stores, minVer, true)
		require.Error(t, err)
		require.Contains(t, err.Error(), "store 2")
		require.Contains(t, err.Error(), "unparsable")
	})

	t.Run("malformed minVersion is reported", func(t *testing.T) {
		err := checkStoresMeetDescIndexMinVersion(nil, "definitely-not-semver", failClosed)
		require.Error(t, err)
	})

	t.Run("pre-release at the floor base version is accepted", func(t *testing.T) {
		// Strict semver treats `9.0.0-beta.2` < `9.0.0`, but a nightly /
		// release-candidate cluster cut from the branch that contains the
		// feature does carry the new decoder. Reject only when the base
		// (Major.Minor.Patch) is below the floor; pre-release tags at the
		// floor base pass. tiup playground reports versions like
		// `9.0.0-beta.2` so this is what the e2e runner relies on.
		stores := []*metapb.Store{
			storeAt(1, "9.0.0-beta.2", false),
			storeAt(2, "9.0.0-rc.1", false),
		}
		require.NoError(t, checkStoresMeetDescIndexMinVersion(stores, minVer, failClosed))
	})

	t.Run("pre-release below the floor base still fails", func(t *testing.T) {
		// `8.5.0-beta.2`'s base (8.5.0) is below the floor (9.0.0), so the
		// pre-release relaxation must NOT let it through. This guards
		// against accidentally degrading the check to "any pre-release
		// passes".
		stores := []*metapb.Store{
			storeAt(1, "8.5.0-beta.2", false),
		}
		err := checkStoresMeetDescIndexMinVersion(stores, minVer, failClosed)
		require.Error(t, err)
		require.Contains(t, err.Error(), "store 1")
		require.Contains(t, err.Error(), "8.5.0-beta.2")
	})
}
