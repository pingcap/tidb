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

const METHOD_PD_GET_MEMBERS: ::grpcio::Method<super::pdpb::GetMembersRequest, super::pdpb::GetMembersResponse> = ::grpcio::Method {
    ty: ::grpcio::MethodType::Unary,
    name: "/pdpb.PD/GetMembers",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

const METHOD_PD_TSO: ::grpcio::Method<super::pdpb::TsoRequest, super::pdpb::TsoResponse> = ::grpcio::Method {
    ty: ::grpcio::MethodType::Duplex,
    name: "/pdpb.PD/Tso",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

const METHOD_PD_BOOTSTRAP: ::grpcio::Method<super::pdpb::BootstrapRequest, super::pdpb::BootstrapResponse> = ::grpcio::Method {
    ty: ::grpcio::MethodType::Unary,
    name: "/pdpb.PD/Bootstrap",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

const METHOD_PD_IS_BOOTSTRAPPED: ::grpcio::Method<super::pdpb::IsBootstrappedRequest, super::pdpb::IsBootstrappedResponse> = ::grpcio::Method {
    ty: ::grpcio::MethodType::Unary,
    name: "/pdpb.PD/IsBootstrapped",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

const METHOD_PD_ALLOC_ID: ::grpcio::Method<super::pdpb::AllocIDRequest, super::pdpb::AllocIDResponse> = ::grpcio::Method {
    ty: ::grpcio::MethodType::Unary,
    name: "/pdpb.PD/AllocID",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

const METHOD_PD_GET_STORE: ::grpcio::Method<super::pdpb::GetStoreRequest, super::pdpb::GetStoreResponse> = ::grpcio::Method {
    ty: ::grpcio::MethodType::Unary,
    name: "/pdpb.PD/GetStore",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

const METHOD_PD_PUT_STORE: ::grpcio::Method<super::pdpb::PutStoreRequest, super::pdpb::PutStoreResponse> = ::grpcio::Method {
    ty: ::grpcio::MethodType::Unary,
    name: "/pdpb.PD/PutStore",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

const METHOD_PD_STORE_HEARTBEAT: ::grpcio::Method<super::pdpb::StoreHeartbeatRequest, super::pdpb::StoreHeartbeatResponse> = ::grpcio::Method {
    ty: ::grpcio::MethodType::Unary,
    name: "/pdpb.PD/StoreHeartbeat",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

const METHOD_PD_REGION_HEARTBEAT: ::grpcio::Method<super::pdpb::RegionHeartbeatRequest, super::pdpb::RegionHeartbeatResponse> = ::grpcio::Method {
    ty: ::grpcio::MethodType::Duplex,
    name: "/pdpb.PD/RegionHeartbeat",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

const METHOD_PD_GET_REGION: ::grpcio::Method<super::pdpb::GetRegionRequest, super::pdpb::GetRegionResponse> = ::grpcio::Method {
    ty: ::grpcio::MethodType::Unary,
    name: "/pdpb.PD/GetRegion",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

const METHOD_PD_GET_REGION_BY_ID: ::grpcio::Method<super::pdpb::GetRegionByIDRequest, super::pdpb::GetRegionResponse> = ::grpcio::Method {
    ty: ::grpcio::MethodType::Unary,
    name: "/pdpb.PD/GetRegionByID",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

const METHOD_PD_ASK_SPLIT: ::grpcio::Method<super::pdpb::AskSplitRequest, super::pdpb::AskSplitResponse> = ::grpcio::Method {
    ty: ::grpcio::MethodType::Unary,
    name: "/pdpb.PD/AskSplit",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

const METHOD_PD_REPORT_SPLIT: ::grpcio::Method<super::pdpb::ReportSplitRequest, super::pdpb::ReportSplitResponse> = ::grpcio::Method {
    ty: ::grpcio::MethodType::Unary,
    name: "/pdpb.PD/ReportSplit",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

const METHOD_PD_GET_CLUSTER_CONFIG: ::grpcio::Method<super::pdpb::GetClusterConfigRequest, super::pdpb::GetClusterConfigResponse> = ::grpcio::Method {
    ty: ::grpcio::MethodType::Unary,
    name: "/pdpb.PD/GetClusterConfig",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

const METHOD_PD_PUT_CLUSTER_CONFIG: ::grpcio::Method<super::pdpb::PutClusterConfigRequest, super::pdpb::PutClusterConfigResponse> = ::grpcio::Method {
    ty: ::grpcio::MethodType::Unary,
    name: "/pdpb.PD/PutClusterConfig",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

pub struct PdClient {
    client: ::grpcio::Client,
}

impl PdClient {
    pub fn new(channel: ::grpcio::Channel) -> Self {
        PdClient {
            client: ::grpcio::Client::new(channel),
        }
    }

    pub fn get_members_opt(&self, req: super::pdpb::GetMembersRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<super::pdpb::GetMembersResponse> {
        self.client.unary_call(&METHOD_PD_GET_MEMBERS, req, opt)
    }

    pub fn get_members(&self, req: super::pdpb::GetMembersRequest) -> ::grpcio::Result<super::pdpb::GetMembersResponse> {
        self.get_members_opt(req, ::grpcio::CallOption::default())
    }

    pub fn get_members_async_opt(&self, req: super::pdpb::GetMembersRequest, opt: ::grpcio::CallOption) -> ::grpcio::ClientUnaryReceiver<super::pdpb::GetMembersResponse> {
        self.client.unary_call_async(&METHOD_PD_GET_MEMBERS, req, opt)
    }

    pub fn get_members_async(&self, req: super::pdpb::GetMembersRequest) -> ::grpcio::ClientUnaryReceiver<super::pdpb::GetMembersResponse> {
        self.get_members_async_opt(req, ::grpcio::CallOption::default())
    }

    pub fn tso_opt(&self, opt: ::grpcio::CallOption) -> (::grpcio::ClientDuplexSender<super::pdpb::TsoRequest>, ::grpcio::ClientDuplexReceiver<super::pdpb::TsoResponse>) {
        self.client.duplex_streaming(&METHOD_PD_TSO, opt)
    }

    pub fn tso(&self) -> (::grpcio::ClientDuplexSender<super::pdpb::TsoRequest>, ::grpcio::ClientDuplexReceiver<super::pdpb::TsoResponse>) {
        self.tso_opt(::grpcio::CallOption::default())
    }

    pub fn bootstrap_opt(&self, req: super::pdpb::BootstrapRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<super::pdpb::BootstrapResponse> {
        self.client.unary_call(&METHOD_PD_BOOTSTRAP, req, opt)
    }

    pub fn bootstrap(&self, req: super::pdpb::BootstrapRequest) -> ::grpcio::Result<super::pdpb::BootstrapResponse> {
        self.bootstrap_opt(req, ::grpcio::CallOption::default())
    }

    pub fn bootstrap_async_opt(&self, req: super::pdpb::BootstrapRequest, opt: ::grpcio::CallOption) -> ::grpcio::ClientUnaryReceiver<super::pdpb::BootstrapResponse> {
        self.client.unary_call_async(&METHOD_PD_BOOTSTRAP, req, opt)
    }

    pub fn bootstrap_async(&self, req: super::pdpb::BootstrapRequest) -> ::grpcio::ClientUnaryReceiver<super::pdpb::BootstrapResponse> {
        self.bootstrap_async_opt(req, ::grpcio::CallOption::default())
    }

    pub fn is_bootstrapped_opt(&self, req: super::pdpb::IsBootstrappedRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<super::pdpb::IsBootstrappedResponse> {
        self.client.unary_call(&METHOD_PD_IS_BOOTSTRAPPED, req, opt)
    }

    pub fn is_bootstrapped(&self, req: super::pdpb::IsBootstrappedRequest) -> ::grpcio::Result<super::pdpb::IsBootstrappedResponse> {
        self.is_bootstrapped_opt(req, ::grpcio::CallOption::default())
    }

    pub fn is_bootstrapped_async_opt(&self, req: super::pdpb::IsBootstrappedRequest, opt: ::grpcio::CallOption) -> ::grpcio::ClientUnaryReceiver<super::pdpb::IsBootstrappedResponse> {
        self.client.unary_call_async(&METHOD_PD_IS_BOOTSTRAPPED, req, opt)
    }

    pub fn is_bootstrapped_async(&self, req: super::pdpb::IsBootstrappedRequest) -> ::grpcio::ClientUnaryReceiver<super::pdpb::IsBootstrappedResponse> {
        self.is_bootstrapped_async_opt(req, ::grpcio::CallOption::default())
    }

    pub fn alloc_id_opt(&self, req: super::pdpb::AllocIDRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<super::pdpb::AllocIDResponse> {
        self.client.unary_call(&METHOD_PD_ALLOC_ID, req, opt)
    }

    pub fn alloc_id(&self, req: super::pdpb::AllocIDRequest) -> ::grpcio::Result<super::pdpb::AllocIDResponse> {
        self.alloc_id_opt(req, ::grpcio::CallOption::default())
    }

    pub fn alloc_id_async_opt(&self, req: super::pdpb::AllocIDRequest, opt: ::grpcio::CallOption) -> ::grpcio::ClientUnaryReceiver<super::pdpb::AllocIDResponse> {
        self.client.unary_call_async(&METHOD_PD_ALLOC_ID, req, opt)
    }

    pub fn alloc_id_async(&self, req: super::pdpb::AllocIDRequest) -> ::grpcio::ClientUnaryReceiver<super::pdpb::AllocIDResponse> {
        self.alloc_id_async_opt(req, ::grpcio::CallOption::default())
    }

    pub fn get_store_opt(&self, req: super::pdpb::GetStoreRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<super::pdpb::GetStoreResponse> {
        self.client.unary_call(&METHOD_PD_GET_STORE, req, opt)
    }

    pub fn get_store(&self, req: super::pdpb::GetStoreRequest) -> ::grpcio::Result<super::pdpb::GetStoreResponse> {
        self.get_store_opt(req, ::grpcio::CallOption::default())
    }

    pub fn get_store_async_opt(&self, req: super::pdpb::GetStoreRequest, opt: ::grpcio::CallOption) -> ::grpcio::ClientUnaryReceiver<super::pdpb::GetStoreResponse> {
        self.client.unary_call_async(&METHOD_PD_GET_STORE, req, opt)
    }

    pub fn get_store_async(&self, req: super::pdpb::GetStoreRequest) -> ::grpcio::ClientUnaryReceiver<super::pdpb::GetStoreResponse> {
        self.get_store_async_opt(req, ::grpcio::CallOption::default())
    }

    pub fn put_store_opt(&self, req: super::pdpb::PutStoreRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<super::pdpb::PutStoreResponse> {
        self.client.unary_call(&METHOD_PD_PUT_STORE, req, opt)
    }

    pub fn put_store(&self, req: super::pdpb::PutStoreRequest) -> ::grpcio::Result<super::pdpb::PutStoreResponse> {
        self.put_store_opt(req, ::grpcio::CallOption::default())
    }

    pub fn put_store_async_opt(&self, req: super::pdpb::PutStoreRequest, opt: ::grpcio::CallOption) -> ::grpcio::ClientUnaryReceiver<super::pdpb::PutStoreResponse> {
        self.client.unary_call_async(&METHOD_PD_PUT_STORE, req, opt)
    }

    pub fn put_store_async(&self, req: super::pdpb::PutStoreRequest) -> ::grpcio::ClientUnaryReceiver<super::pdpb::PutStoreResponse> {
        self.put_store_async_opt(req, ::grpcio::CallOption::default())
    }

    pub fn store_heartbeat_opt(&self, req: super::pdpb::StoreHeartbeatRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<super::pdpb::StoreHeartbeatResponse> {
        self.client.unary_call(&METHOD_PD_STORE_HEARTBEAT, req, opt)
    }

    pub fn store_heartbeat(&self, req: super::pdpb::StoreHeartbeatRequest) -> ::grpcio::Result<super::pdpb::StoreHeartbeatResponse> {
        self.store_heartbeat_opt(req, ::grpcio::CallOption::default())
    }

    pub fn store_heartbeat_async_opt(&self, req: super::pdpb::StoreHeartbeatRequest, opt: ::grpcio::CallOption) -> ::grpcio::ClientUnaryReceiver<super::pdpb::StoreHeartbeatResponse> {
        self.client.unary_call_async(&METHOD_PD_STORE_HEARTBEAT, req, opt)
    }

    pub fn store_heartbeat_async(&self, req: super::pdpb::StoreHeartbeatRequest) -> ::grpcio::ClientUnaryReceiver<super::pdpb::StoreHeartbeatResponse> {
        self.store_heartbeat_async_opt(req, ::grpcio::CallOption::default())
    }

    pub fn region_heartbeat_opt(&self, opt: ::grpcio::CallOption) -> (::grpcio::ClientDuplexSender<super::pdpb::RegionHeartbeatRequest>, ::grpcio::ClientDuplexReceiver<super::pdpb::RegionHeartbeatResponse>) {
        self.client.duplex_streaming(&METHOD_PD_REGION_HEARTBEAT, opt)
    }

    pub fn region_heartbeat(&self) -> (::grpcio::ClientDuplexSender<super::pdpb::RegionHeartbeatRequest>, ::grpcio::ClientDuplexReceiver<super::pdpb::RegionHeartbeatResponse>) {
        self.region_heartbeat_opt(::grpcio::CallOption::default())
    }

    pub fn get_region_opt(&self, req: super::pdpb::GetRegionRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<super::pdpb::GetRegionResponse> {
        self.client.unary_call(&METHOD_PD_GET_REGION, req, opt)
    }

    pub fn get_region(&self, req: super::pdpb::GetRegionRequest) -> ::grpcio::Result<super::pdpb::GetRegionResponse> {
        self.get_region_opt(req, ::grpcio::CallOption::default())
    }

    pub fn get_region_async_opt(&self, req: super::pdpb::GetRegionRequest, opt: ::grpcio::CallOption) -> ::grpcio::ClientUnaryReceiver<super::pdpb::GetRegionResponse> {
        self.client.unary_call_async(&METHOD_PD_GET_REGION, req, opt)
    }

    pub fn get_region_async(&self, req: super::pdpb::GetRegionRequest) -> ::grpcio::ClientUnaryReceiver<super::pdpb::GetRegionResponse> {
        self.get_region_async_opt(req, ::grpcio::CallOption::default())
    }

    pub fn get_region_by_id_opt(&self, req: super::pdpb::GetRegionByIDRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<super::pdpb::GetRegionResponse> {
        self.client.unary_call(&METHOD_PD_GET_REGION_BY_ID, req, opt)
    }

    pub fn get_region_by_id(&self, req: super::pdpb::GetRegionByIDRequest) -> ::grpcio::Result<super::pdpb::GetRegionResponse> {
        self.get_region_by_id_opt(req, ::grpcio::CallOption::default())
    }

    pub fn get_region_by_id_async_opt(&self, req: super::pdpb::GetRegionByIDRequest, opt: ::grpcio::CallOption) -> ::grpcio::ClientUnaryReceiver<super::pdpb::GetRegionResponse> {
        self.client.unary_call_async(&METHOD_PD_GET_REGION_BY_ID, req, opt)
    }

    pub fn get_region_by_id_async(&self, req: super::pdpb::GetRegionByIDRequest) -> ::grpcio::ClientUnaryReceiver<super::pdpb::GetRegionResponse> {
        self.get_region_by_id_async_opt(req, ::grpcio::CallOption::default())
    }

    pub fn ask_split_opt(&self, req: super::pdpb::AskSplitRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<super::pdpb::AskSplitResponse> {
        self.client.unary_call(&METHOD_PD_ASK_SPLIT, req, opt)
    }

    pub fn ask_split(&self, req: super::pdpb::AskSplitRequest) -> ::grpcio::Result<super::pdpb::AskSplitResponse> {
        self.ask_split_opt(req, ::grpcio::CallOption::default())
    }

    pub fn ask_split_async_opt(&self, req: super::pdpb::AskSplitRequest, opt: ::grpcio::CallOption) -> ::grpcio::ClientUnaryReceiver<super::pdpb::AskSplitResponse> {
        self.client.unary_call_async(&METHOD_PD_ASK_SPLIT, req, opt)
    }

    pub fn ask_split_async(&self, req: super::pdpb::AskSplitRequest) -> ::grpcio::ClientUnaryReceiver<super::pdpb::AskSplitResponse> {
        self.ask_split_async_opt(req, ::grpcio::CallOption::default())
    }

    pub fn report_split_opt(&self, req: super::pdpb::ReportSplitRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<super::pdpb::ReportSplitResponse> {
        self.client.unary_call(&METHOD_PD_REPORT_SPLIT, req, opt)
    }

    pub fn report_split(&self, req: super::pdpb::ReportSplitRequest) -> ::grpcio::Result<super::pdpb::ReportSplitResponse> {
        self.report_split_opt(req, ::grpcio::CallOption::default())
    }

    pub fn report_split_async_opt(&self, req: super::pdpb::ReportSplitRequest, opt: ::grpcio::CallOption) -> ::grpcio::ClientUnaryReceiver<super::pdpb::ReportSplitResponse> {
        self.client.unary_call_async(&METHOD_PD_REPORT_SPLIT, req, opt)
    }

    pub fn report_split_async(&self, req: super::pdpb::ReportSplitRequest) -> ::grpcio::ClientUnaryReceiver<super::pdpb::ReportSplitResponse> {
        self.report_split_async_opt(req, ::grpcio::CallOption::default())
    }

    pub fn get_cluster_config_opt(&self, req: super::pdpb::GetClusterConfigRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<super::pdpb::GetClusterConfigResponse> {
        self.client.unary_call(&METHOD_PD_GET_CLUSTER_CONFIG, req, opt)
    }

    pub fn get_cluster_config(&self, req: super::pdpb::GetClusterConfigRequest) -> ::grpcio::Result<super::pdpb::GetClusterConfigResponse> {
        self.get_cluster_config_opt(req, ::grpcio::CallOption::default())
    }

    pub fn get_cluster_config_async_opt(&self, req: super::pdpb::GetClusterConfigRequest, opt: ::grpcio::CallOption) -> ::grpcio::ClientUnaryReceiver<super::pdpb::GetClusterConfigResponse> {
        self.client.unary_call_async(&METHOD_PD_GET_CLUSTER_CONFIG, req, opt)
    }

    pub fn get_cluster_config_async(&self, req: super::pdpb::GetClusterConfigRequest) -> ::grpcio::ClientUnaryReceiver<super::pdpb::GetClusterConfigResponse> {
        self.get_cluster_config_async_opt(req, ::grpcio::CallOption::default())
    }

    pub fn put_cluster_config_opt(&self, req: super::pdpb::PutClusterConfigRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<super::pdpb::PutClusterConfigResponse> {
        self.client.unary_call(&METHOD_PD_PUT_CLUSTER_CONFIG, req, opt)
    }

    pub fn put_cluster_config(&self, req: super::pdpb::PutClusterConfigRequest) -> ::grpcio::Result<super::pdpb::PutClusterConfigResponse> {
        self.put_cluster_config_opt(req, ::grpcio::CallOption::default())
    }

    pub fn put_cluster_config_async_opt(&self, req: super::pdpb::PutClusterConfigRequest, opt: ::grpcio::CallOption) -> ::grpcio::ClientUnaryReceiver<super::pdpb::PutClusterConfigResponse> {
        self.client.unary_call_async(&METHOD_PD_PUT_CLUSTER_CONFIG, req, opt)
    }

    pub fn put_cluster_config_async(&self, req: super::pdpb::PutClusterConfigRequest) -> ::grpcio::ClientUnaryReceiver<super::pdpb::PutClusterConfigResponse> {
        self.put_cluster_config_async_opt(req, ::grpcio::CallOption::default())
    }
    pub fn spawn<F>(&self, f: F) where F: ::futures::Future<Item = (), Error = ()> + Send + 'static {
        self.client.spawn(f)
    }
}

pub trait Pd {
    fn get_members(&self, ctx: ::grpcio::RpcContext, req: super::pdpb::GetMembersRequest, sink: ::grpcio::UnarySink<super::pdpb::GetMembersResponse>);
    fn tso(&self, ctx: ::grpcio::RpcContext, stream: ::grpcio::RequestStream<super::pdpb::TsoRequest>, sink: ::grpcio::DuplexSink<super::pdpb::TsoResponse>);
    fn bootstrap(&self, ctx: ::grpcio::RpcContext, req: super::pdpb::BootstrapRequest, sink: ::grpcio::UnarySink<super::pdpb::BootstrapResponse>);
    fn is_bootstrapped(&self, ctx: ::grpcio::RpcContext, req: super::pdpb::IsBootstrappedRequest, sink: ::grpcio::UnarySink<super::pdpb::IsBootstrappedResponse>);
    fn alloc_id(&self, ctx: ::grpcio::RpcContext, req: super::pdpb::AllocIDRequest, sink: ::grpcio::UnarySink<super::pdpb::AllocIDResponse>);
    fn get_store(&self, ctx: ::grpcio::RpcContext, req: super::pdpb::GetStoreRequest, sink: ::grpcio::UnarySink<super::pdpb::GetStoreResponse>);
    fn put_store(&self, ctx: ::grpcio::RpcContext, req: super::pdpb::PutStoreRequest, sink: ::grpcio::UnarySink<super::pdpb::PutStoreResponse>);
    fn store_heartbeat(&self, ctx: ::grpcio::RpcContext, req: super::pdpb::StoreHeartbeatRequest, sink: ::grpcio::UnarySink<super::pdpb::StoreHeartbeatResponse>);
    fn region_heartbeat(&self, ctx: ::grpcio::RpcContext, stream: ::grpcio::RequestStream<super::pdpb::RegionHeartbeatRequest>, sink: ::grpcio::DuplexSink<super::pdpb::RegionHeartbeatResponse>);
    fn get_region(&self, ctx: ::grpcio::RpcContext, req: super::pdpb::GetRegionRequest, sink: ::grpcio::UnarySink<super::pdpb::GetRegionResponse>);
    fn get_region_by_id(&self, ctx: ::grpcio::RpcContext, req: super::pdpb::GetRegionByIDRequest, sink: ::grpcio::UnarySink<super::pdpb::GetRegionResponse>);
    fn ask_split(&self, ctx: ::grpcio::RpcContext, req: super::pdpb::AskSplitRequest, sink: ::grpcio::UnarySink<super::pdpb::AskSplitResponse>);
    fn report_split(&self, ctx: ::grpcio::RpcContext, req: super::pdpb::ReportSplitRequest, sink: ::grpcio::UnarySink<super::pdpb::ReportSplitResponse>);
    fn get_cluster_config(&self, ctx: ::grpcio::RpcContext, req: super::pdpb::GetClusterConfigRequest, sink: ::grpcio::UnarySink<super::pdpb::GetClusterConfigResponse>);
    fn put_cluster_config(&self, ctx: ::grpcio::RpcContext, req: super::pdpb::PutClusterConfigRequest, sink: ::grpcio::UnarySink<super::pdpb::PutClusterConfigResponse>);
}

pub fn create_pd<S: Pd + Send + Clone + 'static>(s: S) -> ::grpcio::Service {
    let mut builder = ::grpcio::ServiceBuilder::new();
    let instance = s.clone();
    builder = builder.add_unary_handler(&METHOD_PD_GET_MEMBERS, move |ctx, req, resp| {
        instance.get_members(ctx, req, resp)
    });
    let instance = s.clone();
    builder = builder.add_duplex_streaming_handler(&METHOD_PD_TSO, move |ctx, req, resp| {
        instance.tso(ctx, req, resp)
    });
    let instance = s.clone();
    builder = builder.add_unary_handler(&METHOD_PD_BOOTSTRAP, move |ctx, req, resp| {
        instance.bootstrap(ctx, req, resp)
    });
    let instance = s.clone();
    builder = builder.add_unary_handler(&METHOD_PD_IS_BOOTSTRAPPED, move |ctx, req, resp| {
        instance.is_bootstrapped(ctx, req, resp)
    });
    let instance = s.clone();
    builder = builder.add_unary_handler(&METHOD_PD_ALLOC_ID, move |ctx, req, resp| {
        instance.alloc_id(ctx, req, resp)
    });
    let instance = s.clone();
    builder = builder.add_unary_handler(&METHOD_PD_GET_STORE, move |ctx, req, resp| {
        instance.get_store(ctx, req, resp)
    });
    let instance = s.clone();
    builder = builder.add_unary_handler(&METHOD_PD_PUT_STORE, move |ctx, req, resp| {
        instance.put_store(ctx, req, resp)
    });
    let instance = s.clone();
    builder = builder.add_unary_handler(&METHOD_PD_STORE_HEARTBEAT, move |ctx, req, resp| {
        instance.store_heartbeat(ctx, req, resp)
    });
    let instance = s.clone();
    builder = builder.add_duplex_streaming_handler(&METHOD_PD_REGION_HEARTBEAT, move |ctx, req, resp| {
        instance.region_heartbeat(ctx, req, resp)
    });
    let instance = s.clone();
    builder = builder.add_unary_handler(&METHOD_PD_GET_REGION, move |ctx, req, resp| {
        instance.get_region(ctx, req, resp)
    });
    let instance = s.clone();
    builder = builder.add_unary_handler(&METHOD_PD_GET_REGION_BY_ID, move |ctx, req, resp| {
        instance.get_region_by_id(ctx, req, resp)
    });
    let instance = s.clone();
    builder = builder.add_unary_handler(&METHOD_PD_ASK_SPLIT, move |ctx, req, resp| {
        instance.ask_split(ctx, req, resp)
    });
    let instance = s.clone();
    builder = builder.add_unary_handler(&METHOD_PD_REPORT_SPLIT, move |ctx, req, resp| {
        instance.report_split(ctx, req, resp)
    });
    let instance = s.clone();
    builder = builder.add_unary_handler(&METHOD_PD_GET_CLUSTER_CONFIG, move |ctx, req, resp| {
        instance.get_cluster_config(ctx, req, resp)
    });
    let instance = s.clone();
    builder = builder.add_unary_handler(&METHOD_PD_PUT_CLUSTER_CONFIG, move |ctx, req, resp| {
        instance.put_cluster_config(ctx, req, resp)
    });
    builder.build()
}
