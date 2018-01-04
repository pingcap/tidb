// This file is generated. Do not edit
// @generated

// https://github.com/Manishearth/rust-clippy/issues/702
#![allow(unknown_lints)]
#![allow(clippy)]

#![cfg_attr(rustfmt, rustfmt_skip)]

#![allow(box_pointers)]
#![allow(dead_code)]
#![allow(missing_docs)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(trivial_casts)]
#![allow(unsafe_code)]
#![allow(unused_imports)]
#![allow(unused_results)]

const METHOD_TIKV_KV_GET: ::grpcio::Method<super::kvrpcpb::GetRequest, super::kvrpcpb::GetResponse> = ::grpcio::Method {
    ty: ::grpcio::MethodType::Unary,
    name: "/tikvpb.Tikv/KvGet",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

const METHOD_TIKV_KV_SCAN: ::grpcio::Method<super::kvrpcpb::ScanRequest, super::kvrpcpb::ScanResponse> = ::grpcio::Method {
    ty: ::grpcio::MethodType::Unary,
    name: "/tikvpb.Tikv/KvScan",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

const METHOD_TIKV_KV_PREWRITE: ::grpcio::Method<super::kvrpcpb::PrewriteRequest, super::kvrpcpb::PrewriteResponse> = ::grpcio::Method {
    ty: ::grpcio::MethodType::Unary,
    name: "/tikvpb.Tikv/KvPrewrite",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

const METHOD_TIKV_KV_COMMIT: ::grpcio::Method<super::kvrpcpb::CommitRequest, super::kvrpcpb::CommitResponse> = ::grpcio::Method {
    ty: ::grpcio::MethodType::Unary,
    name: "/tikvpb.Tikv/KvCommit",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

const METHOD_TIKV_KV_IMPORT: ::grpcio::Method<super::kvrpcpb::ImportRequest, super::kvrpcpb::ImportResponse> = ::grpcio::Method {
    ty: ::grpcio::MethodType::Unary,
    name: "/tikvpb.Tikv/KvImport",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

const METHOD_TIKV_KV_CLEANUP: ::grpcio::Method<super::kvrpcpb::CleanupRequest, super::kvrpcpb::CleanupResponse> = ::grpcio::Method {
    ty: ::grpcio::MethodType::Unary,
    name: "/tikvpb.Tikv/KvCleanup",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

const METHOD_TIKV_KV_BATCH_GET: ::grpcio::Method<super::kvrpcpb::BatchGetRequest, super::kvrpcpb::BatchGetResponse> = ::grpcio::Method {
    ty: ::grpcio::MethodType::Unary,
    name: "/tikvpb.Tikv/KvBatchGet",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

const METHOD_TIKV_KV_BATCH_ROLLBACK: ::grpcio::Method<super::kvrpcpb::BatchRollbackRequest, super::kvrpcpb::BatchRollbackResponse> = ::grpcio::Method {
    ty: ::grpcio::MethodType::Unary,
    name: "/tikvpb.Tikv/KvBatchRollback",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

const METHOD_TIKV_KV_SCAN_LOCK: ::grpcio::Method<super::kvrpcpb::ScanLockRequest, super::kvrpcpb::ScanLockResponse> = ::grpcio::Method {
    ty: ::grpcio::MethodType::Unary,
    name: "/tikvpb.Tikv/KvScanLock",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

const METHOD_TIKV_KV_RESOLVE_LOCK: ::grpcio::Method<super::kvrpcpb::ResolveLockRequest, super::kvrpcpb::ResolveLockResponse> = ::grpcio::Method {
    ty: ::grpcio::MethodType::Unary,
    name: "/tikvpb.Tikv/KvResolveLock",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

const METHOD_TIKV_KV_GC: ::grpcio::Method<super::kvrpcpb::GCRequest, super::kvrpcpb::GCResponse> = ::grpcio::Method {
    ty: ::grpcio::MethodType::Unary,
    name: "/tikvpb.Tikv/KvGC",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

const METHOD_TIKV_KV_DELETE_RANGE: ::grpcio::Method<super::kvrpcpb::DeleteRangeRequest, super::kvrpcpb::DeleteRangeResponse> = ::grpcio::Method {
    ty: ::grpcio::MethodType::Unary,
    name: "/tikvpb.Tikv/KvDeleteRange",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

const METHOD_TIKV_RAW_GET: ::grpcio::Method<super::kvrpcpb::RawGetRequest, super::kvrpcpb::RawGetResponse> = ::grpcio::Method {
    ty: ::grpcio::MethodType::Unary,
    name: "/tikvpb.Tikv/RawGet",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

const METHOD_TIKV_RAW_PUT: ::grpcio::Method<super::kvrpcpb::RawPutRequest, super::kvrpcpb::RawPutResponse> = ::grpcio::Method {
    ty: ::grpcio::MethodType::Unary,
    name: "/tikvpb.Tikv/RawPut",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

const METHOD_TIKV_RAW_DELETE: ::grpcio::Method<super::kvrpcpb::RawDeleteRequest, super::kvrpcpb::RawDeleteResponse> = ::grpcio::Method {
    ty: ::grpcio::MethodType::Unary,
    name: "/tikvpb.Tikv/RawDelete",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

const METHOD_TIKV_RAW_SCAN: ::grpcio::Method<super::kvrpcpb::RawScanRequest, super::kvrpcpb::RawScanResponse> = ::grpcio::Method {
    ty: ::grpcio::MethodType::Unary,
    name: "/tikvpb.Tikv/RawScan",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

const METHOD_TIKV_COPROCESSOR: ::grpcio::Method<super::coprocessor::Request, super::coprocessor::Response> = ::grpcio::Method {
    ty: ::grpcio::MethodType::Unary,
    name: "/tikvpb.Tikv/Coprocessor",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

const METHOD_TIKV_COPROCESSOR_STREAM: ::grpcio::Method<super::coprocessor::Request, super::coprocessor::Response> = ::grpcio::Method {
    ty: ::grpcio::MethodType::ServerStreaming,
    name: "/tikvpb.Tikv/CoprocessorStream",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

const METHOD_TIKV_RAFT: ::grpcio::Method<super::raft_serverpb::RaftMessage, super::raft_serverpb::Done> = ::grpcio::Method {
    ty: ::grpcio::MethodType::ClientStreaming,
    name: "/tikvpb.Tikv/Raft",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

const METHOD_TIKV_SNAPSHOT: ::grpcio::Method<super::raft_serverpb::SnapshotChunk, super::raft_serverpb::Done> = ::grpcio::Method {
    ty: ::grpcio::MethodType::ClientStreaming,
    name: "/tikvpb.Tikv/Snapshot",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

const METHOD_TIKV_SPLIT_REGION: ::grpcio::Method<super::kvrpcpb::SplitRegionRequest, super::kvrpcpb::SplitRegionResponse> = ::grpcio::Method {
    ty: ::grpcio::MethodType::Unary,
    name: "/tikvpb.Tikv/SplitRegion",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

const METHOD_TIKV_MVCC_GET_BY_KEY: ::grpcio::Method<super::kvrpcpb::MvccGetByKeyRequest, super::kvrpcpb::MvccGetByKeyResponse> = ::grpcio::Method {
    ty: ::grpcio::MethodType::Unary,
    name: "/tikvpb.Tikv/MvccGetByKey",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

const METHOD_TIKV_MVCC_GET_BY_START_TS: ::grpcio::Method<super::kvrpcpb::MvccGetByStartTsRequest, super::kvrpcpb::MvccGetByStartTsResponse> = ::grpcio::Method {
    ty: ::grpcio::MethodType::Unary,
    name: "/tikvpb.Tikv/MvccGetByStartTs",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

pub struct TikvClient {
    client: ::grpcio::Client,
}

impl TikvClient {
    pub fn new(channel: ::grpcio::Channel) -> Self {
        TikvClient {
            client: ::grpcio::Client::new(channel),
        }
    }

    pub fn kv_get_opt(&self, req: super::kvrpcpb::GetRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<super::kvrpcpb::GetResponse> {
        self.client.unary_call(&METHOD_TIKV_KV_GET, req, opt)
    }

    pub fn kv_get(&self, req: super::kvrpcpb::GetRequest) -> ::grpcio::Result<super::kvrpcpb::GetResponse> {
        self.kv_get_opt(req, ::grpcio::CallOption::default())
    }

    pub fn kv_get_async_opt(&self, req: super::kvrpcpb::GetRequest, opt: ::grpcio::CallOption) -> ::grpcio::ClientUnaryReceiver<super::kvrpcpb::GetResponse> {
        self.client.unary_call_async(&METHOD_TIKV_KV_GET, req, opt)
    }

    pub fn kv_get_async(&self, req: super::kvrpcpb::GetRequest) -> ::grpcio::ClientUnaryReceiver<super::kvrpcpb::GetResponse> {
        self.kv_get_async_opt(req, ::grpcio::CallOption::default())
    }

    pub fn kv_scan_opt(&self, req: super::kvrpcpb::ScanRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<super::kvrpcpb::ScanResponse> {
        self.client.unary_call(&METHOD_TIKV_KV_SCAN, req, opt)
    }

    pub fn kv_scan(&self, req: super::kvrpcpb::ScanRequest) -> ::grpcio::Result<super::kvrpcpb::ScanResponse> {
        self.kv_scan_opt(req, ::grpcio::CallOption::default())
    }

    pub fn kv_scan_async_opt(&self, req: super::kvrpcpb::ScanRequest, opt: ::grpcio::CallOption) -> ::grpcio::ClientUnaryReceiver<super::kvrpcpb::ScanResponse> {
        self.client.unary_call_async(&METHOD_TIKV_KV_SCAN, req, opt)
    }

    pub fn kv_scan_async(&self, req: super::kvrpcpb::ScanRequest) -> ::grpcio::ClientUnaryReceiver<super::kvrpcpb::ScanResponse> {
        self.kv_scan_async_opt(req, ::grpcio::CallOption::default())
    }

    pub fn kv_prewrite_opt(&self, req: super::kvrpcpb::PrewriteRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<super::kvrpcpb::PrewriteResponse> {
        self.client.unary_call(&METHOD_TIKV_KV_PREWRITE, req, opt)
    }

    pub fn kv_prewrite(&self, req: super::kvrpcpb::PrewriteRequest) -> ::grpcio::Result<super::kvrpcpb::PrewriteResponse> {
        self.kv_prewrite_opt(req, ::grpcio::CallOption::default())
    }

    pub fn kv_prewrite_async_opt(&self, req: super::kvrpcpb::PrewriteRequest, opt: ::grpcio::CallOption) -> ::grpcio::ClientUnaryReceiver<super::kvrpcpb::PrewriteResponse> {
        self.client.unary_call_async(&METHOD_TIKV_KV_PREWRITE, req, opt)
    }

    pub fn kv_prewrite_async(&self, req: super::kvrpcpb::PrewriteRequest) -> ::grpcio::ClientUnaryReceiver<super::kvrpcpb::PrewriteResponse> {
        self.kv_prewrite_async_opt(req, ::grpcio::CallOption::default())
    }

    pub fn kv_commit_opt(&self, req: super::kvrpcpb::CommitRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<super::kvrpcpb::CommitResponse> {
        self.client.unary_call(&METHOD_TIKV_KV_COMMIT, req, opt)
    }

    pub fn kv_commit(&self, req: super::kvrpcpb::CommitRequest) -> ::grpcio::Result<super::kvrpcpb::CommitResponse> {
        self.kv_commit_opt(req, ::grpcio::CallOption::default())
    }

    pub fn kv_commit_async_opt(&self, req: super::kvrpcpb::CommitRequest, opt: ::grpcio::CallOption) -> ::grpcio::ClientUnaryReceiver<super::kvrpcpb::CommitResponse> {
        self.client.unary_call_async(&METHOD_TIKV_KV_COMMIT, req, opt)
    }

    pub fn kv_commit_async(&self, req: super::kvrpcpb::CommitRequest) -> ::grpcio::ClientUnaryReceiver<super::kvrpcpb::CommitResponse> {
        self.kv_commit_async_opt(req, ::grpcio::CallOption::default())
    }

    pub fn kv_import_opt(&self, req: super::kvrpcpb::ImportRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<super::kvrpcpb::ImportResponse> {
        self.client.unary_call(&METHOD_TIKV_KV_IMPORT, req, opt)
    }

    pub fn kv_import(&self, req: super::kvrpcpb::ImportRequest) -> ::grpcio::Result<super::kvrpcpb::ImportResponse> {
        self.kv_import_opt(req, ::grpcio::CallOption::default())
    }

    pub fn kv_import_async_opt(&self, req: super::kvrpcpb::ImportRequest, opt: ::grpcio::CallOption) -> ::grpcio::ClientUnaryReceiver<super::kvrpcpb::ImportResponse> {
        self.client.unary_call_async(&METHOD_TIKV_KV_IMPORT, req, opt)
    }

    pub fn kv_import_async(&self, req: super::kvrpcpb::ImportRequest) -> ::grpcio::ClientUnaryReceiver<super::kvrpcpb::ImportResponse> {
        self.kv_import_async_opt(req, ::grpcio::CallOption::default())
    }

    pub fn kv_cleanup_opt(&self, req: super::kvrpcpb::CleanupRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<super::kvrpcpb::CleanupResponse> {
        self.client.unary_call(&METHOD_TIKV_KV_CLEANUP, req, opt)
    }

    pub fn kv_cleanup(&self, req: super::kvrpcpb::CleanupRequest) -> ::grpcio::Result<super::kvrpcpb::CleanupResponse> {
        self.kv_cleanup_opt(req, ::grpcio::CallOption::default())
    }

    pub fn kv_cleanup_async_opt(&self, req: super::kvrpcpb::CleanupRequest, opt: ::grpcio::CallOption) -> ::grpcio::ClientUnaryReceiver<super::kvrpcpb::CleanupResponse> {
        self.client.unary_call_async(&METHOD_TIKV_KV_CLEANUP, req, opt)
    }

    pub fn kv_cleanup_async(&self, req: super::kvrpcpb::CleanupRequest) -> ::grpcio::ClientUnaryReceiver<super::kvrpcpb::CleanupResponse> {
        self.kv_cleanup_async_opt(req, ::grpcio::CallOption::default())
    }

    pub fn kv_batch_get_opt(&self, req: super::kvrpcpb::BatchGetRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<super::kvrpcpb::BatchGetResponse> {
        self.client.unary_call(&METHOD_TIKV_KV_BATCH_GET, req, opt)
    }

    pub fn kv_batch_get(&self, req: super::kvrpcpb::BatchGetRequest) -> ::grpcio::Result<super::kvrpcpb::BatchGetResponse> {
        self.kv_batch_get_opt(req, ::grpcio::CallOption::default())
    }

    pub fn kv_batch_get_async_opt(&self, req: super::kvrpcpb::BatchGetRequest, opt: ::grpcio::CallOption) -> ::grpcio::ClientUnaryReceiver<super::kvrpcpb::BatchGetResponse> {
        self.client.unary_call_async(&METHOD_TIKV_KV_BATCH_GET, req, opt)
    }

    pub fn kv_batch_get_async(&self, req: super::kvrpcpb::BatchGetRequest) -> ::grpcio::ClientUnaryReceiver<super::kvrpcpb::BatchGetResponse> {
        self.kv_batch_get_async_opt(req, ::grpcio::CallOption::default())
    }

    pub fn kv_batch_rollback_opt(&self, req: super::kvrpcpb::BatchRollbackRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<super::kvrpcpb::BatchRollbackResponse> {
        self.client.unary_call(&METHOD_TIKV_KV_BATCH_ROLLBACK, req, opt)
    }

    pub fn kv_batch_rollback(&self, req: super::kvrpcpb::BatchRollbackRequest) -> ::grpcio::Result<super::kvrpcpb::BatchRollbackResponse> {
        self.kv_batch_rollback_opt(req, ::grpcio::CallOption::default())
    }

    pub fn kv_batch_rollback_async_opt(&self, req: super::kvrpcpb::BatchRollbackRequest, opt: ::grpcio::CallOption) -> ::grpcio::ClientUnaryReceiver<super::kvrpcpb::BatchRollbackResponse> {
        self.client.unary_call_async(&METHOD_TIKV_KV_BATCH_ROLLBACK, req, opt)
    }

    pub fn kv_batch_rollback_async(&self, req: super::kvrpcpb::BatchRollbackRequest) -> ::grpcio::ClientUnaryReceiver<super::kvrpcpb::BatchRollbackResponse> {
        self.kv_batch_rollback_async_opt(req, ::grpcio::CallOption::default())
    }

    pub fn kv_scan_lock_opt(&self, req: super::kvrpcpb::ScanLockRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<super::kvrpcpb::ScanLockResponse> {
        self.client.unary_call(&METHOD_TIKV_KV_SCAN_LOCK, req, opt)
    }

    pub fn kv_scan_lock(&self, req: super::kvrpcpb::ScanLockRequest) -> ::grpcio::Result<super::kvrpcpb::ScanLockResponse> {
        self.kv_scan_lock_opt(req, ::grpcio::CallOption::default())
    }

    pub fn kv_scan_lock_async_opt(&self, req: super::kvrpcpb::ScanLockRequest, opt: ::grpcio::CallOption) -> ::grpcio::ClientUnaryReceiver<super::kvrpcpb::ScanLockResponse> {
        self.client.unary_call_async(&METHOD_TIKV_KV_SCAN_LOCK, req, opt)
    }

    pub fn kv_scan_lock_async(&self, req: super::kvrpcpb::ScanLockRequest) -> ::grpcio::ClientUnaryReceiver<super::kvrpcpb::ScanLockResponse> {
        self.kv_scan_lock_async_opt(req, ::grpcio::CallOption::default())
    }

    pub fn kv_resolve_lock_opt(&self, req: super::kvrpcpb::ResolveLockRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<super::kvrpcpb::ResolveLockResponse> {
        self.client.unary_call(&METHOD_TIKV_KV_RESOLVE_LOCK, req, opt)
    }

    pub fn kv_resolve_lock(&self, req: super::kvrpcpb::ResolveLockRequest) -> ::grpcio::Result<super::kvrpcpb::ResolveLockResponse> {
        self.kv_resolve_lock_opt(req, ::grpcio::CallOption::default())
    }

    pub fn kv_resolve_lock_async_opt(&self, req: super::kvrpcpb::ResolveLockRequest, opt: ::grpcio::CallOption) -> ::grpcio::ClientUnaryReceiver<super::kvrpcpb::ResolveLockResponse> {
        self.client.unary_call_async(&METHOD_TIKV_KV_RESOLVE_LOCK, req, opt)
    }

    pub fn kv_resolve_lock_async(&self, req: super::kvrpcpb::ResolveLockRequest) -> ::grpcio::ClientUnaryReceiver<super::kvrpcpb::ResolveLockResponse> {
        self.kv_resolve_lock_async_opt(req, ::grpcio::CallOption::default())
    }

    pub fn kv_gc_opt(&self, req: super::kvrpcpb::GCRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<super::kvrpcpb::GCResponse> {
        self.client.unary_call(&METHOD_TIKV_KV_GC, req, opt)
    }

    pub fn kv_gc(&self, req: super::kvrpcpb::GCRequest) -> ::grpcio::Result<super::kvrpcpb::GCResponse> {
        self.kv_gc_opt(req, ::grpcio::CallOption::default())
    }

    pub fn kv_gc_async_opt(&self, req: super::kvrpcpb::GCRequest, opt: ::grpcio::CallOption) -> ::grpcio::ClientUnaryReceiver<super::kvrpcpb::GCResponse> {
        self.client.unary_call_async(&METHOD_TIKV_KV_GC, req, opt)
    }

    pub fn kv_gc_async(&self, req: super::kvrpcpb::GCRequest) -> ::grpcio::ClientUnaryReceiver<super::kvrpcpb::GCResponse> {
        self.kv_gc_async_opt(req, ::grpcio::CallOption::default())
    }

    pub fn kv_delete_range_opt(&self, req: super::kvrpcpb::DeleteRangeRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<super::kvrpcpb::DeleteRangeResponse> {
        self.client.unary_call(&METHOD_TIKV_KV_DELETE_RANGE, req, opt)
    }

    pub fn kv_delete_range(&self, req: super::kvrpcpb::DeleteRangeRequest) -> ::grpcio::Result<super::kvrpcpb::DeleteRangeResponse> {
        self.kv_delete_range_opt(req, ::grpcio::CallOption::default())
    }

    pub fn kv_delete_range_async_opt(&self, req: super::kvrpcpb::DeleteRangeRequest, opt: ::grpcio::CallOption) -> ::grpcio::ClientUnaryReceiver<super::kvrpcpb::DeleteRangeResponse> {
        self.client.unary_call_async(&METHOD_TIKV_KV_DELETE_RANGE, req, opt)
    }

    pub fn kv_delete_range_async(&self, req: super::kvrpcpb::DeleteRangeRequest) -> ::grpcio::ClientUnaryReceiver<super::kvrpcpb::DeleteRangeResponse> {
        self.kv_delete_range_async_opt(req, ::grpcio::CallOption::default())
    }

    pub fn raw_get_opt(&self, req: super::kvrpcpb::RawGetRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<super::kvrpcpb::RawGetResponse> {
        self.client.unary_call(&METHOD_TIKV_RAW_GET, req, opt)
    }

    pub fn raw_get(&self, req: super::kvrpcpb::RawGetRequest) -> ::grpcio::Result<super::kvrpcpb::RawGetResponse> {
        self.raw_get_opt(req, ::grpcio::CallOption::default())
    }

    pub fn raw_get_async_opt(&self, req: super::kvrpcpb::RawGetRequest, opt: ::grpcio::CallOption) -> ::grpcio::ClientUnaryReceiver<super::kvrpcpb::RawGetResponse> {
        self.client.unary_call_async(&METHOD_TIKV_RAW_GET, req, opt)
    }

    pub fn raw_get_async(&self, req: super::kvrpcpb::RawGetRequest) -> ::grpcio::ClientUnaryReceiver<super::kvrpcpb::RawGetResponse> {
        self.raw_get_async_opt(req, ::grpcio::CallOption::default())
    }

    pub fn raw_put_opt(&self, req: super::kvrpcpb::RawPutRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<super::kvrpcpb::RawPutResponse> {
        self.client.unary_call(&METHOD_TIKV_RAW_PUT, req, opt)
    }

    pub fn raw_put(&self, req: super::kvrpcpb::RawPutRequest) -> ::grpcio::Result<super::kvrpcpb::RawPutResponse> {
        self.raw_put_opt(req, ::grpcio::CallOption::default())
    }

    pub fn raw_put_async_opt(&self, req: super::kvrpcpb::RawPutRequest, opt: ::grpcio::CallOption) -> ::grpcio::ClientUnaryReceiver<super::kvrpcpb::RawPutResponse> {
        self.client.unary_call_async(&METHOD_TIKV_RAW_PUT, req, opt)
    }

    pub fn raw_put_async(&self, req: super::kvrpcpb::RawPutRequest) -> ::grpcio::ClientUnaryReceiver<super::kvrpcpb::RawPutResponse> {
        self.raw_put_async_opt(req, ::grpcio::CallOption::default())
    }

    pub fn raw_delete_opt(&self, req: super::kvrpcpb::RawDeleteRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<super::kvrpcpb::RawDeleteResponse> {
        self.client.unary_call(&METHOD_TIKV_RAW_DELETE, req, opt)
    }

    pub fn raw_delete(&self, req: super::kvrpcpb::RawDeleteRequest) -> ::grpcio::Result<super::kvrpcpb::RawDeleteResponse> {
        self.raw_delete_opt(req, ::grpcio::CallOption::default())
    }

    pub fn raw_delete_async_opt(&self, req: super::kvrpcpb::RawDeleteRequest, opt: ::grpcio::CallOption) -> ::grpcio::ClientUnaryReceiver<super::kvrpcpb::RawDeleteResponse> {
        self.client.unary_call_async(&METHOD_TIKV_RAW_DELETE, req, opt)
    }

    pub fn raw_delete_async(&self, req: super::kvrpcpb::RawDeleteRequest) -> ::grpcio::ClientUnaryReceiver<super::kvrpcpb::RawDeleteResponse> {
        self.raw_delete_async_opt(req, ::grpcio::CallOption::default())
    }

    pub fn raw_scan_opt(&self, req: super::kvrpcpb::RawScanRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<super::kvrpcpb::RawScanResponse> {
        self.client.unary_call(&METHOD_TIKV_RAW_SCAN, req, opt)
    }

    pub fn raw_scan(&self, req: super::kvrpcpb::RawScanRequest) -> ::grpcio::Result<super::kvrpcpb::RawScanResponse> {
        self.raw_scan_opt(req, ::grpcio::CallOption::default())
    }

    pub fn raw_scan_async_opt(&self, req: super::kvrpcpb::RawScanRequest, opt: ::grpcio::CallOption) -> ::grpcio::ClientUnaryReceiver<super::kvrpcpb::RawScanResponse> {
        self.client.unary_call_async(&METHOD_TIKV_RAW_SCAN, req, opt)
    }

    pub fn raw_scan_async(&self, req: super::kvrpcpb::RawScanRequest) -> ::grpcio::ClientUnaryReceiver<super::kvrpcpb::RawScanResponse> {
        self.raw_scan_async_opt(req, ::grpcio::CallOption::default())
    }

    pub fn coprocessor_opt(&self, req: super::coprocessor::Request, opt: ::grpcio::CallOption) -> ::grpcio::Result<super::coprocessor::Response> {
        self.client.unary_call(&METHOD_TIKV_COPROCESSOR, req, opt)
    }

    pub fn coprocessor(&self, req: super::coprocessor::Request) -> ::grpcio::Result<super::coprocessor::Response> {
        self.coprocessor_opt(req, ::grpcio::CallOption::default())
    }

    pub fn coprocessor_async_opt(&self, req: super::coprocessor::Request, opt: ::grpcio::CallOption) -> ::grpcio::ClientUnaryReceiver<super::coprocessor::Response> {
        self.client.unary_call_async(&METHOD_TIKV_COPROCESSOR, req, opt)
    }

    pub fn coprocessor_async(&self, req: super::coprocessor::Request) -> ::grpcio::ClientUnaryReceiver<super::coprocessor::Response> {
        self.coprocessor_async_opt(req, ::grpcio::CallOption::default())
    }

    pub fn coprocessor_stream_opt(&self, req: super::coprocessor::Request, opt: ::grpcio::CallOption) -> ::grpcio::ClientSStreamReceiver<super::coprocessor::Response> {
        self.client.server_streaming(&METHOD_TIKV_COPROCESSOR_STREAM, req, opt)
    }

    pub fn coprocessor_stream(&self, req: super::coprocessor::Request) -> ::grpcio::ClientSStreamReceiver<super::coprocessor::Response> {
        self.coprocessor_stream_opt(req, ::grpcio::CallOption::default())
    }

    pub fn raft_opt(&self, opt: ::grpcio::CallOption) -> (::grpcio::ClientCStreamSender<super::raft_serverpb::RaftMessage>, ::grpcio::ClientCStreamReceiver<super::raft_serverpb::Done>) {
        self.client.client_streaming(&METHOD_TIKV_RAFT, opt)
    }

    pub fn raft(&self) -> (::grpcio::ClientCStreamSender<super::raft_serverpb::RaftMessage>, ::grpcio::ClientCStreamReceiver<super::raft_serverpb::Done>) {
        self.raft_opt(::grpcio::CallOption::default())
    }

    pub fn snapshot_opt(&self, opt: ::grpcio::CallOption) -> (::grpcio::ClientCStreamSender<super::raft_serverpb::SnapshotChunk>, ::grpcio::ClientCStreamReceiver<super::raft_serverpb::Done>) {
        self.client.client_streaming(&METHOD_TIKV_SNAPSHOT, opt)
    }

    pub fn snapshot(&self) -> (::grpcio::ClientCStreamSender<super::raft_serverpb::SnapshotChunk>, ::grpcio::ClientCStreamReceiver<super::raft_serverpb::Done>) {
        self.snapshot_opt(::grpcio::CallOption::default())
    }

    pub fn split_region_opt(&self, req: super::kvrpcpb::SplitRegionRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<super::kvrpcpb::SplitRegionResponse> {
        self.client.unary_call(&METHOD_TIKV_SPLIT_REGION, req, opt)
    }

    pub fn split_region(&self, req: super::kvrpcpb::SplitRegionRequest) -> ::grpcio::Result<super::kvrpcpb::SplitRegionResponse> {
        self.split_region_opt(req, ::grpcio::CallOption::default())
    }

    pub fn split_region_async_opt(&self, req: super::kvrpcpb::SplitRegionRequest, opt: ::grpcio::CallOption) -> ::grpcio::ClientUnaryReceiver<super::kvrpcpb::SplitRegionResponse> {
        self.client.unary_call_async(&METHOD_TIKV_SPLIT_REGION, req, opt)
    }

    pub fn split_region_async(&self, req: super::kvrpcpb::SplitRegionRequest) -> ::grpcio::ClientUnaryReceiver<super::kvrpcpb::SplitRegionResponse> {
        self.split_region_async_opt(req, ::grpcio::CallOption::default())
    }

    pub fn mvcc_get_by_key_opt(&self, req: super::kvrpcpb::MvccGetByKeyRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<super::kvrpcpb::MvccGetByKeyResponse> {
        self.client.unary_call(&METHOD_TIKV_MVCC_GET_BY_KEY, req, opt)
    }

    pub fn mvcc_get_by_key(&self, req: super::kvrpcpb::MvccGetByKeyRequest) -> ::grpcio::Result<super::kvrpcpb::MvccGetByKeyResponse> {
        self.mvcc_get_by_key_opt(req, ::grpcio::CallOption::default())
    }

    pub fn mvcc_get_by_key_async_opt(&self, req: super::kvrpcpb::MvccGetByKeyRequest, opt: ::grpcio::CallOption) -> ::grpcio::ClientUnaryReceiver<super::kvrpcpb::MvccGetByKeyResponse> {
        self.client.unary_call_async(&METHOD_TIKV_MVCC_GET_BY_KEY, req, opt)
    }

    pub fn mvcc_get_by_key_async(&self, req: super::kvrpcpb::MvccGetByKeyRequest) -> ::grpcio::ClientUnaryReceiver<super::kvrpcpb::MvccGetByKeyResponse> {
        self.mvcc_get_by_key_async_opt(req, ::grpcio::CallOption::default())
    }

    pub fn mvcc_get_by_start_ts_opt(&self, req: super::kvrpcpb::MvccGetByStartTsRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<super::kvrpcpb::MvccGetByStartTsResponse> {
        self.client.unary_call(&METHOD_TIKV_MVCC_GET_BY_START_TS, req, opt)
    }

    pub fn mvcc_get_by_start_ts(&self, req: super::kvrpcpb::MvccGetByStartTsRequest) -> ::grpcio::Result<super::kvrpcpb::MvccGetByStartTsResponse> {
        self.mvcc_get_by_start_ts_opt(req, ::grpcio::CallOption::default())
    }

    pub fn mvcc_get_by_start_ts_async_opt(&self, req: super::kvrpcpb::MvccGetByStartTsRequest, opt: ::grpcio::CallOption) -> ::grpcio::ClientUnaryReceiver<super::kvrpcpb::MvccGetByStartTsResponse> {
        self.client.unary_call_async(&METHOD_TIKV_MVCC_GET_BY_START_TS, req, opt)
    }

    pub fn mvcc_get_by_start_ts_async(&self, req: super::kvrpcpb::MvccGetByStartTsRequest) -> ::grpcio::ClientUnaryReceiver<super::kvrpcpb::MvccGetByStartTsResponse> {
        self.mvcc_get_by_start_ts_async_opt(req, ::grpcio::CallOption::default())
    }
    pub fn spawn<F>(&self, f: F) where F: ::futures::Future<Item = (), Error = ()> + Send + 'static {
        self.client.spawn(f)
    }
}

pub trait Tikv {
    fn kv_get(&self, ctx: ::grpcio::RpcContext, req: super::kvrpcpb::GetRequest, sink: ::grpcio::UnarySink<super::kvrpcpb::GetResponse>);
    fn kv_scan(&self, ctx: ::grpcio::RpcContext, req: super::kvrpcpb::ScanRequest, sink: ::grpcio::UnarySink<super::kvrpcpb::ScanResponse>);
    fn kv_prewrite(&self, ctx: ::grpcio::RpcContext, req: super::kvrpcpb::PrewriteRequest, sink: ::grpcio::UnarySink<super::kvrpcpb::PrewriteResponse>);
    fn kv_commit(&self, ctx: ::grpcio::RpcContext, req: super::kvrpcpb::CommitRequest, sink: ::grpcio::UnarySink<super::kvrpcpb::CommitResponse>);
    fn kv_import(&self, ctx: ::grpcio::RpcContext, req: super::kvrpcpb::ImportRequest, sink: ::grpcio::UnarySink<super::kvrpcpb::ImportResponse>);
    fn kv_cleanup(&self, ctx: ::grpcio::RpcContext, req: super::kvrpcpb::CleanupRequest, sink: ::grpcio::UnarySink<super::kvrpcpb::CleanupResponse>);
    fn kv_batch_get(&self, ctx: ::grpcio::RpcContext, req: super::kvrpcpb::BatchGetRequest, sink: ::grpcio::UnarySink<super::kvrpcpb::BatchGetResponse>);
    fn kv_batch_rollback(&self, ctx: ::grpcio::RpcContext, req: super::kvrpcpb::BatchRollbackRequest, sink: ::grpcio::UnarySink<super::kvrpcpb::BatchRollbackResponse>);
    fn kv_scan_lock(&self, ctx: ::grpcio::RpcContext, req: super::kvrpcpb::ScanLockRequest, sink: ::grpcio::UnarySink<super::kvrpcpb::ScanLockResponse>);
    fn kv_resolve_lock(&self, ctx: ::grpcio::RpcContext, req: super::kvrpcpb::ResolveLockRequest, sink: ::grpcio::UnarySink<super::kvrpcpb::ResolveLockResponse>);
    fn kv_gc(&self, ctx: ::grpcio::RpcContext, req: super::kvrpcpb::GCRequest, sink: ::grpcio::UnarySink<super::kvrpcpb::GCResponse>);
    fn kv_delete_range(&self, ctx: ::grpcio::RpcContext, req: super::kvrpcpb::DeleteRangeRequest, sink: ::grpcio::UnarySink<super::kvrpcpb::DeleteRangeResponse>);
    fn raw_get(&self, ctx: ::grpcio::RpcContext, req: super::kvrpcpb::RawGetRequest, sink: ::grpcio::UnarySink<super::kvrpcpb::RawGetResponse>);
    fn raw_put(&self, ctx: ::grpcio::RpcContext, req: super::kvrpcpb::RawPutRequest, sink: ::grpcio::UnarySink<super::kvrpcpb::RawPutResponse>);
    fn raw_delete(&self, ctx: ::grpcio::RpcContext, req: super::kvrpcpb::RawDeleteRequest, sink: ::grpcio::UnarySink<super::kvrpcpb::RawDeleteResponse>);
    fn raw_scan(&self, ctx: ::grpcio::RpcContext, req: super::kvrpcpb::RawScanRequest, sink: ::grpcio::UnarySink<super::kvrpcpb::RawScanResponse>);
    fn coprocessor(&self, ctx: ::grpcio::RpcContext, req: super::coprocessor::Request, sink: ::grpcio::UnarySink<super::coprocessor::Response>);
    fn coprocessor_stream(&self, ctx: ::grpcio::RpcContext, req: super::coprocessor::Request, sink: ::grpcio::ServerStreamingSink<super::coprocessor::Response>);
    fn raft(&self, ctx: ::grpcio::RpcContext, stream: ::grpcio::RequestStream<super::raft_serverpb::RaftMessage>, sink: ::grpcio::ClientStreamingSink<super::raft_serverpb::Done>);
    fn snapshot(&self, ctx: ::grpcio::RpcContext, stream: ::grpcio::RequestStream<super::raft_serverpb::SnapshotChunk>, sink: ::grpcio::ClientStreamingSink<super::raft_serverpb::Done>);
    fn split_region(&self, ctx: ::grpcio::RpcContext, req: super::kvrpcpb::SplitRegionRequest, sink: ::grpcio::UnarySink<super::kvrpcpb::SplitRegionResponse>);
    fn mvcc_get_by_key(&self, ctx: ::grpcio::RpcContext, req: super::kvrpcpb::MvccGetByKeyRequest, sink: ::grpcio::UnarySink<super::kvrpcpb::MvccGetByKeyResponse>);
    fn mvcc_get_by_start_ts(&self, ctx: ::grpcio::RpcContext, req: super::kvrpcpb::MvccGetByStartTsRequest, sink: ::grpcio::UnarySink<super::kvrpcpb::MvccGetByStartTsResponse>);
}

pub fn create_tikv<S: Tikv + Send + Clone + 'static>(s: S) -> ::grpcio::Service {
    let mut builder = ::grpcio::ServiceBuilder::new();
    let instance = s.clone();
    builder = builder.add_unary_handler(&METHOD_TIKV_KV_GET, move |ctx, req, resp| {
        instance.kv_get(ctx, req, resp)
    });
    let instance = s.clone();
    builder = builder.add_unary_handler(&METHOD_TIKV_KV_SCAN, move |ctx, req, resp| {
        instance.kv_scan(ctx, req, resp)
    });
    let instance = s.clone();
    builder = builder.add_unary_handler(&METHOD_TIKV_KV_PREWRITE, move |ctx, req, resp| {
        instance.kv_prewrite(ctx, req, resp)
    });
    let instance = s.clone();
    builder = builder.add_unary_handler(&METHOD_TIKV_KV_COMMIT, move |ctx, req, resp| {
        instance.kv_commit(ctx, req, resp)
    });
    let instance = s.clone();
    builder = builder.add_unary_handler(&METHOD_TIKV_KV_IMPORT, move |ctx, req, resp| {
        instance.kv_import(ctx, req, resp)
    });
    let instance = s.clone();
    builder = builder.add_unary_handler(&METHOD_TIKV_KV_CLEANUP, move |ctx, req, resp| {
        instance.kv_cleanup(ctx, req, resp)
    });
    let instance = s.clone();
    builder = builder.add_unary_handler(&METHOD_TIKV_KV_BATCH_GET, move |ctx, req, resp| {
        instance.kv_batch_get(ctx, req, resp)
    });
    let instance = s.clone();
    builder = builder.add_unary_handler(&METHOD_TIKV_KV_BATCH_ROLLBACK, move |ctx, req, resp| {
        instance.kv_batch_rollback(ctx, req, resp)
    });
    let instance = s.clone();
    builder = builder.add_unary_handler(&METHOD_TIKV_KV_SCAN_LOCK, move |ctx, req, resp| {
        instance.kv_scan_lock(ctx, req, resp)
    });
    let instance = s.clone();
    builder = builder.add_unary_handler(&METHOD_TIKV_KV_RESOLVE_LOCK, move |ctx, req, resp| {
        instance.kv_resolve_lock(ctx, req, resp)
    });
    let instance = s.clone();
    builder = builder.add_unary_handler(&METHOD_TIKV_KV_GC, move |ctx, req, resp| {
        instance.kv_gc(ctx, req, resp)
    });
    let instance = s.clone();
    builder = builder.add_unary_handler(&METHOD_TIKV_KV_DELETE_RANGE, move |ctx, req, resp| {
        instance.kv_delete_range(ctx, req, resp)
    });
    let instance = s.clone();
    builder = builder.add_unary_handler(&METHOD_TIKV_RAW_GET, move |ctx, req, resp| {
        instance.raw_get(ctx, req, resp)
    });
    let instance = s.clone();
    builder = builder.add_unary_handler(&METHOD_TIKV_RAW_PUT, move |ctx, req, resp| {
        instance.raw_put(ctx, req, resp)
    });
    let instance = s.clone();
    builder = builder.add_unary_handler(&METHOD_TIKV_RAW_DELETE, move |ctx, req, resp| {
        instance.raw_delete(ctx, req, resp)
    });
    let instance = s.clone();
    builder = builder.add_unary_handler(&METHOD_TIKV_RAW_SCAN, move |ctx, req, resp| {
        instance.raw_scan(ctx, req, resp)
    });
    let instance = s.clone();
    builder = builder.add_unary_handler(&METHOD_TIKV_COPROCESSOR, move |ctx, req, resp| {
        instance.coprocessor(ctx, req, resp)
    });
    let instance = s.clone();
    builder = builder.add_server_streaming_handler(&METHOD_TIKV_COPROCESSOR_STREAM, move |ctx, req, resp| {
        instance.coprocessor_stream(ctx, req, resp)
    });
    let instance = s.clone();
    builder = builder.add_client_streaming_handler(&METHOD_TIKV_RAFT, move |ctx, req, resp| {
        instance.raft(ctx, req, resp)
    });
    let instance = s.clone();
    builder = builder.add_client_streaming_handler(&METHOD_TIKV_SNAPSHOT, move |ctx, req, resp| {
        instance.snapshot(ctx, req, resp)
    });
    let instance = s.clone();
    builder = builder.add_unary_handler(&METHOD_TIKV_SPLIT_REGION, move |ctx, req, resp| {
        instance.split_region(ctx, req, resp)
    });
    let instance = s.clone();
    builder = builder.add_unary_handler(&METHOD_TIKV_MVCC_GET_BY_KEY, move |ctx, req, resp| {
        instance.mvcc_get_by_key(ctx, req, resp)
    });
    let instance = s.clone();
    builder = builder.add_unary_handler(&METHOD_TIKV_MVCC_GET_BY_START_TS, move |ctx, req, resp| {
        instance.mvcc_get_by_start_ts(ctx, req, resp)
    });
    builder.build()
}
