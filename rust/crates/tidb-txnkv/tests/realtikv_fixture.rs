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

//! The realtikv tests' raw requests over the node's store: what a test sends
//! outside any transaction to leave the cluster in a state, or to inspect it.

#![allow(dead_code)]

use std::time::Duration;

use tidb_txnkv::driver::tikv_store::TikvStore;
use tikv_client::proto::kvrpcpb;

/// The node's store over the playground's PD.
pub(crate) fn connect(pd_address: &str) -> TikvStore {
    TikvStore::connect(vec![pd_address.to_owned()], tikv_client::Config::default())
        .expect("the node's store")
}

/// The leader's address and the request context for the region holding
/// `key`, from the store's region cache.
pub(crate) fn route(store: &TikvStore, key: &[u8]) -> (String, kvrpcpb::Context) {
    use tikv_client::PdClient as _;
    let pd = store.store().pd_client();
    let key = key.to_vec();
    let (address, region) = store
        .block_on(async move {
            let region = pd.region_for_key(&key.into()).await?;
            let routed = pd.clone().map_region_to_store(region).await?;
            Ok::<_, tikv_client::Error>((routed.target.clone(), routed.region_with_leader))
        })
        .expect("route the fixture key");
    (
        address,
        kvrpcpb::Context {
            region_id: region.region.id,
            region_epoch: region.region.region_epoch.clone(),
            peer: region.leader.clone(),
            request_source: "realtikv_fixture".to_owned(),
            request_origin: kvrpcpb::RequestOrigin::TiDb as i32,
            cluster_id: store.cluster_id(),
            ..kvrpcpb::Context::default()
        },
    )
}

/// One raw request to the store at `address`.
pub(crate) fn send<Req, Resp>(
    store: &TikvStore,
    address: &str,
    mut request: Req,
    timeout: Duration,
) -> Resp
where
    Req: tikv_client::tikv::Request + Send + Sync,
    Resp: Send + 'static,
{
    store
        .block_on(async {
            store
                .store()
                .send_request(address, &mut request, timeout)
                .await
                .map(|response| response.downcast::<Resp>().ok().map(|response| *response))
        })
        .expect("send the fixture request")
        .expect("unexpected fixture response type")
}

/// A timestamp from the store's oracle.
pub(crate) fn timestamp(store: &TikvStore) -> u64 {
    store
        .get_timestamp_async()
        .wait()
        .expect("allocate a fixture timestamp")
}
