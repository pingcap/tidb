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

//! Go's `ctx` on a transaction's RPCs, as an RPC interceptor on the
//! client-rust transaction: each physical request of the current call --
//! reads, lock and commit RPCs, their retries and the lock resolution they
//! trigger -- fails once the call is cancelled or past its deadline, is
//! bounded by the time left, and is counted by TiDB's TopSQL KV-exec counter
//! (Go `KvExecCounter.RPCInterceptor`). Work client-rust detaches from the
//! call (secondary commits, keep-alive) runs without it, as client-go runs it
//! on `store.Ctx()`.
//!
//! It also records the pages a scan's regions answered, for ANALYZE's
//! per-region fragments.

use std::any::Any;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use futures::future::BoxFuture;
use tikv_client::proto::kvrpcpb;
use tikv_client::tikv::Request;
use tikv_client::{RpcDispatchResult, RpcInterceptor, RpcNext};

use tidb_txnkv::rpc::UnaryCallContext;

/// One page a scan's region answered.
#[derive(Clone, Debug)]
pub(crate) struct ScanPage {
    pub(crate) region: tikv_client::RegionVerId,
    pub(crate) end_key: Vec<u8>,
    pub(crate) pairs: Vec<(Vec<u8>, Vec<u8>)>,
}

/// The transaction's current call and scan-page capture, shared with its
/// interceptor.
#[derive(Clone, Default)]
pub(crate) struct CallSlot {
    call: Arc<Mutex<Option<UnaryCallContext>>>,
    pages: Arc<Mutex<Option<Vec<ScanPage>>>>,
}

impl CallSlot {
    /// Binds the RPCs that follow to `call`.
    pub(crate) fn set(&self, call: &UnaryCallContext) {
        *self.call.lock().unwrap_or_else(PoisonError::into_inner) = Some(call.clone());
    }

    /// Starts (`true`) or stops recording scan pages, returning what was
    /// recorded.
    pub(crate) fn capture_scan_pages(&self, capture: bool) -> Vec<ScanPage> {
        let mut pages = self.pages.lock().unwrap_or_else(PoisonError::into_inner);
        let recorded = pages.take().unwrap_or_default();
        if capture {
            *pages = Some(Vec::new());
        }
        recorded
    }

    /// The interceptor that enforces the bound call.
    pub(crate) fn interceptor(&self) -> tikv_client::RpcInterceptorHandle {
        Arc::new(CallInterceptor(self.clone()))
    }
}

struct CallInterceptor(CallSlot);

fn deadline_exceeded() -> tikv_client::Error {
    tikv_client::Error::StringError("context deadline exceeded".to_owned())
}

impl CallInterceptor {
    fn record_page(&self, request: &dyn Request, result: &RpcDispatchResult) {
        let mut pages = self.0.pages.lock().unwrap_or_else(PoisonError::into_inner);
        let Some(pages) = pages.as_mut() else {
            return;
        };
        let Ok(response) = result else {
            return;
        };
        let response = response.as_ref() as &dyn Any;
        let page = if let Some(scan) = request.as_any().downcast_ref::<kvrpcpb::ScanRequest>() {
            let Some(response) = response.downcast_ref::<kvrpcpb::ScanResponse>() else {
                return;
            };
            if response.region_error.is_some() || response.error.is_some() {
                return;
            }
            (
                scan.context.as_ref(),
                scan.end_key.clone(),
                response
                    .pairs
                    .iter()
                    .filter(|pair| pair.error.is_none())
                    .map(|pair| (pair.key.clone(), pair.value.clone()))
                    .collect(),
            )
        } else if let Some(get) = request.as_any().downcast_ref::<kvrpcpb::GetRequest>() {
            // A scan pair met a lock: Go's scanner resolves it and reads the
            // key again with a point Get (`scanner.go` `getData`), whose
            // answer serves the row.
            let Some(response) = response.downcast_ref::<kvrpcpb::GetResponse>() else {
                return;
            };
            if response.region_error.is_some() || response.error.is_some() || response.not_found {
                return;
            }
            (
                get.context.as_ref(),
                Vec::new(),
                vec![(get.key.clone(), response.value.clone())],
            )
        } else {
            return;
        };
        let (context, end_key, pairs) = page;
        let context = context.cloned().unwrap_or_default();
        let epoch = context.region_epoch.unwrap_or_default();
        pages.push(ScanPage {
            region: tikv_client::tikv::new_region_ver_id(
                context.region_id,
                epoch.conf_ver,
                epoch.version,
            ),
            end_key,
            pairs,
        });
    }
}

impl RpcInterceptor for CallInterceptor {
    fn name(&self) -> &str {
        "tidb-call-context"
    }

    fn wrap<'a>(
        &'a self,
        target: &'a str,
        request: &'a dyn Request,
        next: RpcNext<'a>,
    ) -> BoxFuture<'a, RpcDispatchResult> {
        Box::pin(async move {
            let call = self
                .0
                .call
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .clone();
            let result = match call {
                None => next().await,
                Some(call) => {
                    if call.cancellation().is_cancelled() {
                        return Err(tikv_client::Error::ContextCanceled);
                    }
                    let remaining = call.timeout();
                    if remaining.is_zero() {
                        return Err(deadline_exceeded());
                    }
                    if let Some(counter) = call.kv_exec_counter() {
                        counter.mark(target);
                    }
                    if remaining == Duration::MAX {
                        next().await
                    } else {
                        tokio::time::timeout(remaining, next())
                            .await
                            .unwrap_or_else(|_| Err(deadline_exceeded()))
                    }
                }
            };
            self.record_page(request, &result);
            result
        })
    }
}
