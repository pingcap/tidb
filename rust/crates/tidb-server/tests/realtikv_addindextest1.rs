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

//! Go `tests/realtikvtest/addindextest1`, over [`crate::realtikvtest`]:
//! `disttask_test.go`'s `TestUseClusterIdInGlobalSortPath`.

/// Go `TestUseClusterIdInGlobalSortPath`.
#[test]
fn test_use_cluster_id_in_global_sort_path() {
    let store = realtikv!();
    *tidb_vardef::CLOUD_STORAGE_URI.write().unwrap() = "s3://bucket/path/to/folder?access-key=aaaaa&secret-access-key=bbbbb&endpoint=http://abc.com&force-path-style=false&region=Beijing&provider=aws".to_owned();
    let path = tidb_dxf_handle::get_cloud_storage_uri(Some(store.tikv_store().cluster_id()));
    assert_eq!(
        path,
        format!(
            "s3://bucket/path/to/folder/dxf/{}/?access-key=aaaaa&secret-access-key=bbbbb&endpoint=http://abc.com&force-path-style=false&region=Beijing&provider=aws",
            store.tikv_store().cluster_id()
        )
    );
    // Without a cluster ID.
    assert_eq!(
        tidb_dxf_handle::get_cloud_storage_uri(None),
        "s3://bucket/path/to/folder/dxf/?access-key=aaaaa&secret-access-key=bbbbb&endpoint=http://abc.com&force-path-style=false&region=Beijing&provider=aws"
    );
    tidb_vardef::CLOUD_STORAGE_URI.write().unwrap().clear();
}
