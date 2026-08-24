#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SpanSet {
    #[prost(uint64, tag = "1")]
    pub start_time_ns: u64,
    #[prost(uint64, tag = "2")]
    pub cycles_per_sec: u64,
    #[prost(message, repeated, tag = "3")]
    pub spans: ::prost::alloc::vec::Vec<Span>,
    #[prost(uint64, tag = "4")]
    pub create_time_ns: u64,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Root {}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Parent {
    #[prost(uint64, tag = "1")]
    pub id: u64,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Continue {
    #[prost(uint64, tag = "1")]
    pub id: u64,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Link {
    #[prost(oneof = "link::Link", tags = "1, 2, 3")]
    pub link: ::core::option::Option<link::Link>,
}
/// Nested message and enum types in `Link`.
pub mod link {
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Link {
        #[prost(message, tag = "1")]
        Root(super::Root),
        #[prost(message, tag = "2")]
        Parent(super::Parent),
        #[prost(message, tag = "3")]
        Continue(super::Continue),
    }
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Span {
    #[prost(uint64, tag = "1")]
    pub id: u64,
    #[prost(message, optional, tag = "2")]
    pub link: ::core::option::Option<Link>,
    #[prost(uint64, tag = "3")]
    pub begin_cycles: u64,
    #[prost(uint64, tag = "4")]
    pub end_cycles: u64,
    #[prost(uint32, tag = "5")]
    pub event: u32,
}
