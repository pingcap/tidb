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

const METHOD_DEBUG_GET: ::grpcio::Method<super::debugpb::GetRequest, super::debugpb::GetResponse> = ::grpcio::Method {
    ty: ::grpcio::MethodType::Unary,
    name: "/debugpb.Debug/Get",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

const METHOD_DEBUG_RAFT_LOG: ::grpcio::Method<super::debugpb::RaftLogRequest, super::debugpb::RaftLogResponse> = ::grpcio::Method {
    ty: ::grpcio::MethodType::Unary,
    name: "/debugpb.Debug/RaftLog",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

const METHOD_DEBUG_REGION_INFO: ::grpcio::Method<super::debugpb::RegionInfoRequest, super::debugpb::RegionInfoResponse> = ::grpcio::Method {
    ty: ::grpcio::MethodType::Unary,
    name: "/debugpb.Debug/RegionInfo",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

const METHOD_DEBUG_REGION_SIZE: ::grpcio::Method<super::debugpb::RegionSizeRequest, super::debugpb::RegionSizeResponse> = ::grpcio::Method {
    ty: ::grpcio::MethodType::Unary,
    name: "/debugpb.Debug/RegionSize",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

const METHOD_DEBUG_SCAN_MVCC: ::grpcio::Method<super::debugpb::ScanMvccRequest, super::debugpb::ScanMvccResponse> = ::grpcio::Method {
    ty: ::grpcio::MethodType::ServerStreaming,
    name: "/debugpb.Debug/ScanMvcc",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

const METHOD_DEBUG_COMPACT: ::grpcio::Method<super::debugpb::CompactRequest, super::debugpb::CompactResponse> = ::grpcio::Method {
    ty: ::grpcio::MethodType::Unary,
    name: "/debugpb.Debug/Compact",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

const METHOD_DEBUG_INJECT_FAIL_POINT: ::grpcio::Method<super::debugpb::InjectFailPointRequest, super::debugpb::InjectFailPointResponse> = ::grpcio::Method {
    ty: ::grpcio::MethodType::Unary,
    name: "/debugpb.Debug/InjectFailPoint",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

const METHOD_DEBUG_RECOVER_FAIL_POINT: ::grpcio::Method<super::debugpb::RecoverFailPointRequest, super::debugpb::RecoverFailPointResponse> = ::grpcio::Method {
    ty: ::grpcio::MethodType::Unary,
    name: "/debugpb.Debug/RecoverFailPoint",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

const METHOD_DEBUG_LIST_FAIL_POINTS: ::grpcio::Method<super::debugpb::ListFailPointsRequest, super::debugpb::ListFailPointsResponse> = ::grpcio::Method {
    ty: ::grpcio::MethodType::Unary,
    name: "/debugpb.Debug/ListFailPoints",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

pub struct DebugClient {
    client: ::grpcio::Client,
}

impl DebugClient {
    pub fn new(channel: ::grpcio::Channel) -> Self {
        DebugClient {
            client: ::grpcio::Client::new(channel),
        }
    }

    pub fn get_opt(&self, req: super::debugpb::GetRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<super::debugpb::GetResponse> {
        self.client.unary_call(&METHOD_DEBUG_GET, req, opt)
    }

    pub fn get(&self, req: super::debugpb::GetRequest) -> ::grpcio::Result<super::debugpb::GetResponse> {
        self.get_opt(req, ::grpcio::CallOption::default())
    }

    pub fn get_async_opt(&self, req: super::debugpb::GetRequest, opt: ::grpcio::CallOption) -> ::grpcio::ClientUnaryReceiver<super::debugpb::GetResponse> {
        self.client.unary_call_async(&METHOD_DEBUG_GET, req, opt)
    }

    pub fn get_async(&self, req: super::debugpb::GetRequest) -> ::grpcio::ClientUnaryReceiver<super::debugpb::GetResponse> {
        self.get_async_opt(req, ::grpcio::CallOption::default())
    }

    pub fn raft_log_opt(&self, req: super::debugpb::RaftLogRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<super::debugpb::RaftLogResponse> {
        self.client.unary_call(&METHOD_DEBUG_RAFT_LOG, req, opt)
    }

    pub fn raft_log(&self, req: super::debugpb::RaftLogRequest) -> ::grpcio::Result<super::debugpb::RaftLogResponse> {
        self.raft_log_opt(req, ::grpcio::CallOption::default())
    }

    pub fn raft_log_async_opt(&self, req: super::debugpb::RaftLogRequest, opt: ::grpcio::CallOption) -> ::grpcio::ClientUnaryReceiver<super::debugpb::RaftLogResponse> {
        self.client.unary_call_async(&METHOD_DEBUG_RAFT_LOG, req, opt)
    }

    pub fn raft_log_async(&self, req: super::debugpb::RaftLogRequest) -> ::grpcio::ClientUnaryReceiver<super::debugpb::RaftLogResponse> {
        self.raft_log_async_opt(req, ::grpcio::CallOption::default())
    }

    pub fn region_info_opt(&self, req: super::debugpb::RegionInfoRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<super::debugpb::RegionInfoResponse> {
        self.client.unary_call(&METHOD_DEBUG_REGION_INFO, req, opt)
    }

    pub fn region_info(&self, req: super::debugpb::RegionInfoRequest) -> ::grpcio::Result<super::debugpb::RegionInfoResponse> {
        self.region_info_opt(req, ::grpcio::CallOption::default())
    }

    pub fn region_info_async_opt(&self, req: super::debugpb::RegionInfoRequest, opt: ::grpcio::CallOption) -> ::grpcio::ClientUnaryReceiver<super::debugpb::RegionInfoResponse> {
        self.client.unary_call_async(&METHOD_DEBUG_REGION_INFO, req, opt)
    }

    pub fn region_info_async(&self, req: super::debugpb::RegionInfoRequest) -> ::grpcio::ClientUnaryReceiver<super::debugpb::RegionInfoResponse> {
        self.region_info_async_opt(req, ::grpcio::CallOption::default())
    }

    pub fn region_size_opt(&self, req: super::debugpb::RegionSizeRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<super::debugpb::RegionSizeResponse> {
        self.client.unary_call(&METHOD_DEBUG_REGION_SIZE, req, opt)
    }

    pub fn region_size(&self, req: super::debugpb::RegionSizeRequest) -> ::grpcio::Result<super::debugpb::RegionSizeResponse> {
        self.region_size_opt(req, ::grpcio::CallOption::default())
    }

    pub fn region_size_async_opt(&self, req: super::debugpb::RegionSizeRequest, opt: ::grpcio::CallOption) -> ::grpcio::ClientUnaryReceiver<super::debugpb::RegionSizeResponse> {
        self.client.unary_call_async(&METHOD_DEBUG_REGION_SIZE, req, opt)
    }

    pub fn region_size_async(&self, req: super::debugpb::RegionSizeRequest) -> ::grpcio::ClientUnaryReceiver<super::debugpb::RegionSizeResponse> {
        self.region_size_async_opt(req, ::grpcio::CallOption::default())
    }

    pub fn scan_mvcc_opt(&self, req: super::debugpb::ScanMvccRequest, opt: ::grpcio::CallOption) -> ::grpcio::ClientSStreamReceiver<super::debugpb::ScanMvccResponse> {
        self.client.server_streaming(&METHOD_DEBUG_SCAN_MVCC, req, opt)
    }

    pub fn scan_mvcc(&self, req: super::debugpb::ScanMvccRequest) -> ::grpcio::ClientSStreamReceiver<super::debugpb::ScanMvccResponse> {
        self.scan_mvcc_opt(req, ::grpcio::CallOption::default())
    }

    pub fn compact_opt(&self, req: super::debugpb::CompactRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<super::debugpb::CompactResponse> {
        self.client.unary_call(&METHOD_DEBUG_COMPACT, req, opt)
    }

    pub fn compact(&self, req: super::debugpb::CompactRequest) -> ::grpcio::Result<super::debugpb::CompactResponse> {
        self.compact_opt(req, ::grpcio::CallOption::default())
    }

    pub fn compact_async_opt(&self, req: super::debugpb::CompactRequest, opt: ::grpcio::CallOption) -> ::grpcio::ClientUnaryReceiver<super::debugpb::CompactResponse> {
        self.client.unary_call_async(&METHOD_DEBUG_COMPACT, req, opt)
    }

    pub fn compact_async(&self, req: super::debugpb::CompactRequest) -> ::grpcio::ClientUnaryReceiver<super::debugpb::CompactResponse> {
        self.compact_async_opt(req, ::grpcio::CallOption::default())
    }

    pub fn inject_fail_point_opt(&self, req: super::debugpb::InjectFailPointRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<super::debugpb::InjectFailPointResponse> {
        self.client.unary_call(&METHOD_DEBUG_INJECT_FAIL_POINT, req, opt)
    }

    pub fn inject_fail_point(&self, req: super::debugpb::InjectFailPointRequest) -> ::grpcio::Result<super::debugpb::InjectFailPointResponse> {
        self.inject_fail_point_opt(req, ::grpcio::CallOption::default())
    }

    pub fn inject_fail_point_async_opt(&self, req: super::debugpb::InjectFailPointRequest, opt: ::grpcio::CallOption) -> ::grpcio::ClientUnaryReceiver<super::debugpb::InjectFailPointResponse> {
        self.client.unary_call_async(&METHOD_DEBUG_INJECT_FAIL_POINT, req, opt)
    }

    pub fn inject_fail_point_async(&self, req: super::debugpb::InjectFailPointRequest) -> ::grpcio::ClientUnaryReceiver<super::debugpb::InjectFailPointResponse> {
        self.inject_fail_point_async_opt(req, ::grpcio::CallOption::default())
    }

    pub fn recover_fail_point_opt(&self, req: super::debugpb::RecoverFailPointRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<super::debugpb::RecoverFailPointResponse> {
        self.client.unary_call(&METHOD_DEBUG_RECOVER_FAIL_POINT, req, opt)
    }

    pub fn recover_fail_point(&self, req: super::debugpb::RecoverFailPointRequest) -> ::grpcio::Result<super::debugpb::RecoverFailPointResponse> {
        self.recover_fail_point_opt(req, ::grpcio::CallOption::default())
    }

    pub fn recover_fail_point_async_opt(&self, req: super::debugpb::RecoverFailPointRequest, opt: ::grpcio::CallOption) -> ::grpcio::ClientUnaryReceiver<super::debugpb::RecoverFailPointResponse> {
        self.client.unary_call_async(&METHOD_DEBUG_RECOVER_FAIL_POINT, req, opt)
    }

    pub fn recover_fail_point_async(&self, req: super::debugpb::RecoverFailPointRequest) -> ::grpcio::ClientUnaryReceiver<super::debugpb::RecoverFailPointResponse> {
        self.recover_fail_point_async_opt(req, ::grpcio::CallOption::default())
    }

    pub fn list_fail_points_opt(&self, req: super::debugpb::ListFailPointsRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<super::debugpb::ListFailPointsResponse> {
        self.client.unary_call(&METHOD_DEBUG_LIST_FAIL_POINTS, req, opt)
    }

    pub fn list_fail_points(&self, req: super::debugpb::ListFailPointsRequest) -> ::grpcio::Result<super::debugpb::ListFailPointsResponse> {
        self.list_fail_points_opt(req, ::grpcio::CallOption::default())
    }

    pub fn list_fail_points_async_opt(&self, req: super::debugpb::ListFailPointsRequest, opt: ::grpcio::CallOption) -> ::grpcio::ClientUnaryReceiver<super::debugpb::ListFailPointsResponse> {
        self.client.unary_call_async(&METHOD_DEBUG_LIST_FAIL_POINTS, req, opt)
    }

    pub fn list_fail_points_async(&self, req: super::debugpb::ListFailPointsRequest) -> ::grpcio::ClientUnaryReceiver<super::debugpb::ListFailPointsResponse> {
        self.list_fail_points_async_opt(req, ::grpcio::CallOption::default())
    }
    pub fn spawn<F>(&self, f: F) where F: ::futures::Future<Item = (), Error = ()> + Send + 'static {
        self.client.spawn(f)
    }
}

pub trait Debug {
    fn get(&self, ctx: ::grpcio::RpcContext, req: super::debugpb::GetRequest, sink: ::grpcio::UnarySink<super::debugpb::GetResponse>);
    fn raft_log(&self, ctx: ::grpcio::RpcContext, req: super::debugpb::RaftLogRequest, sink: ::grpcio::UnarySink<super::debugpb::RaftLogResponse>);
    fn region_info(&self, ctx: ::grpcio::RpcContext, req: super::debugpb::RegionInfoRequest, sink: ::grpcio::UnarySink<super::debugpb::RegionInfoResponse>);
    fn region_size(&self, ctx: ::grpcio::RpcContext, req: super::debugpb::RegionSizeRequest, sink: ::grpcio::UnarySink<super::debugpb::RegionSizeResponse>);
    fn scan_mvcc(&self, ctx: ::grpcio::RpcContext, req: super::debugpb::ScanMvccRequest, sink: ::grpcio::ServerStreamingSink<super::debugpb::ScanMvccResponse>);
    fn compact(&self, ctx: ::grpcio::RpcContext, req: super::debugpb::CompactRequest, sink: ::grpcio::UnarySink<super::debugpb::CompactResponse>);
    fn inject_fail_point(&self, ctx: ::grpcio::RpcContext, req: super::debugpb::InjectFailPointRequest, sink: ::grpcio::UnarySink<super::debugpb::InjectFailPointResponse>);
    fn recover_fail_point(&self, ctx: ::grpcio::RpcContext, req: super::debugpb::RecoverFailPointRequest, sink: ::grpcio::UnarySink<super::debugpb::RecoverFailPointResponse>);
    fn list_fail_points(&self, ctx: ::grpcio::RpcContext, req: super::debugpb::ListFailPointsRequest, sink: ::grpcio::UnarySink<super::debugpb::ListFailPointsResponse>);
}

pub fn create_debug<S: Debug + Send + Clone + 'static>(s: S) -> ::grpcio::Service {
    let mut builder = ::grpcio::ServiceBuilder::new();
    let instance = s.clone();
    builder = builder.add_unary_handler(&METHOD_DEBUG_GET, move |ctx, req, resp| {
        instance.get(ctx, req, resp)
    });
    let instance = s.clone();
    builder = builder.add_unary_handler(&METHOD_DEBUG_RAFT_LOG, move |ctx, req, resp| {
        instance.raft_log(ctx, req, resp)
    });
    let instance = s.clone();
    builder = builder.add_unary_handler(&METHOD_DEBUG_REGION_INFO, move |ctx, req, resp| {
        instance.region_info(ctx, req, resp)
    });
    let instance = s.clone();
    builder = builder.add_unary_handler(&METHOD_DEBUG_REGION_SIZE, move |ctx, req, resp| {
        instance.region_size(ctx, req, resp)
    });
    let instance = s.clone();
    builder = builder.add_server_streaming_handler(&METHOD_DEBUG_SCAN_MVCC, move |ctx, req, resp| {
        instance.scan_mvcc(ctx, req, resp)
    });
    let instance = s.clone();
    builder = builder.add_unary_handler(&METHOD_DEBUG_COMPACT, move |ctx, req, resp| {
        instance.compact(ctx, req, resp)
    });
    let instance = s.clone();
    builder = builder.add_unary_handler(&METHOD_DEBUG_INJECT_FAIL_POINT, move |ctx, req, resp| {
        instance.inject_fail_point(ctx, req, resp)
    });
    let instance = s.clone();
    builder = builder.add_unary_handler(&METHOD_DEBUG_RECOVER_FAIL_POINT, move |ctx, req, resp| {
        instance.recover_fail_point(ctx, req, resp)
    });
    let instance = s.clone();
    builder = builder.add_unary_handler(&METHOD_DEBUG_LIST_FAIL_POINTS, move |ctx, req, resp| {
        instance.list_fail_points(ctx, req, resp)
    });
    builder.build()
}
