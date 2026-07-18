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

//! Source-backed tests for NTILE state transitions.

use tidb_exec::ntile::{Ntile, NtilePartialState};

fn drain(tile: &mut Ntile, rows: usize) -> Vec<Option<u64>> {
    (0..rows).map(|_| tile.next_value()).collect()
}

#[test]
fn ntile_partial_state_and_null_divisor_match_source() {
    // Source: pkg/executor/aggfuncs/func_ntile.go:23-25, :45-46, :70-74.
    // Direct Go coverage: pkg/executor/aggfuncs/func_ntile_test.go:25
    // (TestMemNtile).
    assert_eq!(
        Ntile::partial_state_size(),
        std::mem::size_of::<NtilePartialState>()
    );
    assert_eq!(Ntile::partial_state_size(), 5 * std::mem::size_of::<u64>());

    let mut tile = Ntile::new(0);
    tile.update(3);
    assert_eq!(drain(&mut tile, 3), vec![None, None, None]);
}

#[test]
fn ntile_group_vectors_match_source_algorithm() {
    // The output vectors mirror the NTILE cases in the source window suite
    // (`pkg/executor/aggfuncs/window_func_test.go`) while this leaf keeps the
    // unclaimed direct memory anchor above.
    let mut tile = Ntile::new(3);
    tile.update(4);
    assert_eq!(
        drain(&mut tile, 4),
        vec![Some(1), Some(1), Some(2), Some(3)]
    );

    let mut tile = Ntile::new(5);
    tile.update(3);
    assert_eq!(drain(&mut tile, 3), vec![Some(1), Some(2), Some(3)]);

    let mut tile = Ntile::new(3);
    tile.update(11);
    assert_eq!(
        drain(&mut tile, 11),
        vec![
            Some(1),
            Some(1),
            Some(1),
            Some(1),
            Some(2),
            Some(2),
            Some(2),
            Some(2),
            Some(3),
            Some(3),
            Some(3),
        ]
    );
}

#[test]
fn ntile_batch_update_and_reset_match_source_state() {
    let mut tile = Ntile::new(3);
    tile.update(1);
    tile.update(3);
    assert_eq!(
        drain(&mut tile, 4),
        vec![Some(1), Some(1), Some(2), Some(3)]
    );

    tile.reset();
    tile.update(4);
    assert_eq!(
        drain(&mut tile, 4),
        vec![Some(1), Some(1), Some(2), Some(3)]
    );
}
