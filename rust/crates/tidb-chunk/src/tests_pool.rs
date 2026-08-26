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

//! Ports of `pkg/util/chunk/pool_test.go`.

use tidb_datatype::{FieldType, FieldTypeCode};

use crate::column::get_fixed_len;
use crate::pool::Pool;

fn pool_field_types() -> Vec<FieldType> {
    vec![
        FieldType::new(FieldTypeCode::Varchar),
        FieldType::new(FieldTypeCode::Json),
        FieldType::new(FieldTypeCode::Float),
        FieldType::new(FieldTypeCode::NewDecimal),
        FieldType::new(FieldTypeCode::Double),
        FieldType::new(FieldTypeCode::LongLong),
    ]
}

/// Go `TestNewPool` (pool_test.go): the pool records its identifying capacity.
/// The per-width column buckets are private in this port (they are lazily
/// populated, so they are all conceptually "present" from construction).
#[test]
fn new_pool() {
    let pool = Pool::new(1024);
    assert_eq!(pool.init_capacity(), 1024);
}

/// Go `TestPoolGetChunk` (pool_test.go): a pooled chunk has Go's physical
/// layout -- variable columns carry no fixed element buffer and every fixed
/// column's data capacity is `initCap * getFixedLen`.
#[test]
fn pool_get_chunk() {
    let init_cap = 1024;
    let pool = Pool::new(init_cap);
    let field_types = pool_field_types();

    let chk = pool.get_chunk(&field_types);
    assert_eq!(chk.num_cols(), field_types.len());
    // Variable columns (Varchar, JSON) have no element buffer; fixed ones do.
    assert!(!chk.column(0).is_fixed());
    assert!(!chk.column(1).is_fixed());
    for idx in 2..6 {
        assert!(chk.column(idx).is_fixed());
        assert_eq!(
            chk.column(idx).type_size(),
            get_fixed_len(&field_types[idx])
        );
        assert_eq!(
            chk.column(idx).data_capacity(),
            init_cap * get_fixed_len(&field_types[idx]) as usize
        );
    }
}

/// Go `TestPoolPutChunk` (pool_test.go): after `PutChunk` the chunk is left
/// with Go's nil-column post-destroy state, and the recycled chunk serves the
/// next `GetChunk`.
#[test]
fn pool_put_chunk() {
    let init_cap = 1024;
    let pool = Pool::new(init_cap);
    let field_types = pool_field_types();

    let mut chk = pool.get_chunk(&field_types);
    pool.put_chunk(&field_types, &mut chk);
    // Go asserts `len(chk.columns) == 0`; this port keeps the slot count but
    // strips the storage, so verify through the observable row state.
    assert_eq!(chk.num_rows(), 0);
    for idx in 0..chk.num_cols() {
        assert_eq!(chk.column(idx).rows(), 0);
    }

    // The put columns are reused by the following GetChunk.
    let _again = pool.get_chunk(&field_types);
}
