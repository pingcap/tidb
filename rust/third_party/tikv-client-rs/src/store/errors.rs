// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::proto::coprocessor;
use crate::proto::kvrpcpb;
use crate::Error;

// Those that can have a single region error
pub trait HasRegionError {
    fn region_error(&mut self) -> Option<crate::proto::errorpb::Error>;
}

/// A concrete response accepted by client-go's `GenRegionErrorResp` switch.
///
/// Rust's typed transport does not need a dynamic response wrapper: callers
/// construct the response type selected by their request at compile time.
#[allow(dead_code)]
pub trait RegionErrorResponse: HasRegionError + Default {
    fn set_region_error(&mut self, error: crate::proto::errorpb::Error);

    fn from_region_error(error: crate::proto::errorpb::Error) -> Self {
        let mut response = Self::default();
        response.set_region_error(error);
        response
    }
}

// Those that can have multiple region errors
pub trait HasRegionErrors {
    fn region_errors(&mut self) -> Option<Vec<crate::proto::errorpb::Error>>;
}

pub trait HasKeyErrors {
    fn key_errors(&mut self) -> Option<Vec<Error>>;
}

impl<T: HasRegionError> HasRegionErrors for T {
    fn region_errors(&mut self) -> Option<Vec<crate::proto::errorpb::Error>> {
        self.region_error().map(|e| vec![e])
    }
}

macro_rules! has_region_error {
    ($type:ty) => {
        impl HasRegionError for $type {
            fn region_error(&mut self) -> Option<crate::proto::errorpb::Error> {
                self.region_error.take().map(|e| e.into())
            }
        }
    };
}

has_region_error!(kvrpcpb::GetResponse);
has_region_error!(kvrpcpb::ScanResponse);
has_region_error!(kvrpcpb::PrewriteResponse);
has_region_error!(kvrpcpb::CommitResponse);
has_region_error!(kvrpcpb::PessimisticLockResponse);
has_region_error!(kvrpcpb::ImportResponse);
has_region_error!(kvrpcpb::CleanupResponse);
has_region_error!(kvrpcpb::BatchRollbackResponse);
has_region_error!(kvrpcpb::PessimisticRollbackResponse);
has_region_error!(kvrpcpb::BatchGetResponse);
has_region_error!(kvrpcpb::ScanLockResponse);
has_region_error!(kvrpcpb::ResolveLockResponse);
has_region_error!(kvrpcpb::TxnHeartBeatResponse);
has_region_error!(kvrpcpb::CheckTxnStatusResponse);
has_region_error!(kvrpcpb::CheckSecondaryLocksResponse);
has_region_error!(kvrpcpb::DeleteRangeResponse);
has_region_error!(kvrpcpb::GcResponse);
has_region_error!(kvrpcpb::PrepareFlashbackToVersionResponse);
has_region_error!(kvrpcpb::FlashbackToVersionResponse);
has_region_error!(kvrpcpb::FlushResponse);
has_region_error!(kvrpcpb::BufferBatchGetResponse);
has_region_error!(kvrpcpb::UnsafeDestroyRangeResponse);
has_region_error!(kvrpcpb::MvccGetByKeyResponse);
has_region_error!(kvrpcpb::MvccGetByStartTsResponse);
has_region_error!(kvrpcpb::GetLockWaitInfoResponse);
has_region_error!(kvrpcpb::SplitRegionResponse);
has_region_error!(kvrpcpb::RawGetResponse);
has_region_error!(kvrpcpb::RawBatchGetResponse);
has_region_error!(kvrpcpb::RawGetKeyTtlResponse);
has_region_error!(kvrpcpb::RawPutResponse);
has_region_error!(kvrpcpb::RawBatchPutResponse);
has_region_error!(kvrpcpb::RawDeleteResponse);
has_region_error!(kvrpcpb::RawBatchDeleteResponse);
has_region_error!(kvrpcpb::RawDeleteRangeResponse);
has_region_error!(kvrpcpb::RawScanResponse);
has_region_error!(kvrpcpb::RawBatchScanResponse);
has_region_error!(kvrpcpb::RawCasResponse);
has_region_error!(kvrpcpb::RawCoprocessorResponse);
has_region_error!(kvrpcpb::RawChecksumResponse);
has_region_error!(kvrpcpb::GetHealthFeedbackResponse);

macro_rules! region_error_response {
    ($($type:ty),+ $(,)?) => {
        $(
            impl RegionErrorResponse for $type {
                fn set_region_error(&mut self, error: crate::proto::errorpb::Error) {
                    self.region_error = Some(error);
                }
            }
        )+
    };
}

region_error_response!(
    kvrpcpb::GetResponse,
    kvrpcpb::ScanResponse,
    kvrpcpb::PrewriteResponse,
    kvrpcpb::PessimisticLockResponse,
    kvrpcpb::PessimisticRollbackResponse,
    kvrpcpb::CommitResponse,
    kvrpcpb::CleanupResponse,
    kvrpcpb::BatchGetResponse,
    kvrpcpb::BatchRollbackResponse,
    kvrpcpb::ScanLockResponse,
    kvrpcpb::ResolveLockResponse,
    kvrpcpb::GcResponse,
    kvrpcpb::DeleteRangeResponse,
    kvrpcpb::RawGetResponse,
    kvrpcpb::RawBatchGetResponse,
    kvrpcpb::RawPutResponse,
    kvrpcpb::RawBatchPutResponse,
    kvrpcpb::RawDeleteResponse,
    kvrpcpb::RawBatchDeleteResponse,
    kvrpcpb::RawDeleteRangeResponse,
    kvrpcpb::RawScanResponse,
    kvrpcpb::UnsafeDestroyRangeResponse,
    kvrpcpb::RawGetKeyTtlResponse,
    kvrpcpb::RawCasResponse,
    kvrpcpb::RawChecksumResponse,
    kvrpcpb::MvccGetByKeyResponse,
    kvrpcpb::MvccGetByStartTsResponse,
    kvrpcpb::SplitRegionResponse,
    kvrpcpb::TxnHeartBeatResponse,
    kvrpcpb::CheckTxnStatusResponse,
    kvrpcpb::CheckSecondaryLocksResponse,
    kvrpcpb::FlashbackToVersionResponse,
    kvrpcpb::PrepareFlashbackToVersionResponse,
    kvrpcpb::FlushResponse,
    kvrpcpb::BufferBatchGetResponse,
    kvrpcpb::GetHealthFeedbackResponse,
);

impl HasRegionError for coprocessor::Response {
    fn region_error(&mut self) -> Option<crate::proto::errorpb::Error> {
        self.region_error.take()
    }
}

impl RegionErrorResponse for coprocessor::Response {
    fn set_region_error(&mut self, error: crate::proto::errorpb::Error) {
        self.region_error = Some(error);
    }
}

macro_rules! has_key_error {
    ($type:ty) => {
        impl HasKeyErrors for $type {
            fn key_errors(&mut self) -> Option<Vec<Error>> {
                self.error.take().map(|e| vec![e.into()])
            }
        }
    };
}

has_key_error!(kvrpcpb::GetResponse);
has_key_error!(kvrpcpb::CommitResponse);
has_key_error!(kvrpcpb::CleanupResponse);
has_key_error!(kvrpcpb::BatchRollbackResponse);
has_key_error!(kvrpcpb::ScanLockResponse);
has_key_error!(kvrpcpb::ResolveLockResponse);
has_key_error!(kvrpcpb::GcResponse);
has_key_error!(kvrpcpb::TxnHeartBeatResponse);
has_key_error!(kvrpcpb::CheckTxnStatusResponse);
has_key_error!(kvrpcpb::CheckSecondaryLocksResponse);

macro_rules! has_str_error {
    ($type:ty) => {
        impl HasKeyErrors for $type {
            fn key_errors(&mut self) -> Option<Vec<Error>> {
                if self.error.is_empty() {
                    None
                } else {
                    Some(vec![Error::KvError {
                        message: std::mem::take(&mut self.error),
                    }])
                }
            }
        }
    };
}

has_str_error!(kvrpcpb::RawGetResponse);
has_str_error!(kvrpcpb::RawGetKeyTtlResponse);
has_str_error!(kvrpcpb::RawPutResponse);
has_str_error!(kvrpcpb::RawBatchPutResponse);
has_str_error!(kvrpcpb::RawDeleteResponse);
has_str_error!(kvrpcpb::RawBatchDeleteResponse);
has_str_error!(kvrpcpb::RawDeleteRangeResponse);
has_str_error!(kvrpcpb::RawCasResponse);
has_str_error!(kvrpcpb::RawCoprocessorResponse);
has_str_error!(kvrpcpb::RawChecksumResponse);
has_str_error!(kvrpcpb::ImportResponse);
has_str_error!(kvrpcpb::DeleteRangeResponse);
has_str_error!(kvrpcpb::PrepareFlashbackToVersionResponse);
has_str_error!(kvrpcpb::FlashbackToVersionResponse);
has_str_error!(kvrpcpb::UnsafeDestroyRangeResponse);
has_str_error!(kvrpcpb::PhysicalScanLockResponse);
has_str_error!(kvrpcpb::MvccGetByKeyResponse);
has_str_error!(kvrpcpb::CheckLockObserverResponse);
has_str_error!(kvrpcpb::MvccGetByStartTsResponse);
has_str_error!(kvrpcpb::GetLockWaitInfoResponse);

impl HasKeyErrors for kvrpcpb::ScanResponse {
    fn key_errors(&mut self) -> Option<Vec<Error>> {
        self.error
            .take()
            .map(|error| vec![error.into()])
            .or_else(|| extract_errors(self.pairs.iter_mut().map(|pair| pair.error.take())))
    }
}

impl HasKeyErrors for kvrpcpb::BatchGetResponse {
    fn key_errors(&mut self) -> Option<Vec<Error>> {
        self.error
            .take()
            .map(|error| vec![error.into()])
            .or_else(|| extract_errors(self.pairs.iter_mut().map(|pair| pair.error.take())))
    }
}

impl HasKeyErrors for kvrpcpb::RawBatchGetResponse {
    fn key_errors(&mut self) -> Option<Vec<Error>> {
        extract_errors(self.pairs.iter_mut().map(|pair| pair.error.take()))
    }
}

impl HasKeyErrors for kvrpcpb::RawScanResponse {
    fn key_errors(&mut self) -> Option<Vec<Error>> {
        extract_errors(self.kvs.iter_mut().map(|pair| pair.error.take()))
    }
}

impl HasKeyErrors for kvrpcpb::RawBatchScanResponse {
    fn key_errors(&mut self) -> Option<Vec<Error>> {
        extract_errors(self.kvs.iter_mut().map(|pair| pair.error.take()))
    }
}

impl HasKeyErrors for kvrpcpb::PrewriteResponse {
    fn key_errors(&mut self) -> Option<Vec<Error>> {
        extract_errors(std::mem::take(&mut self.errors).into_iter().map(Some))
    }
}

impl HasKeyErrors for kvrpcpb::PessimisticLockResponse {
    fn key_errors(&mut self) -> Option<Vec<Error>> {
        extract_errors(std::mem::take(&mut self.errors).into_iter().map(Some))
    }
}

impl HasKeyErrors for kvrpcpb::PessimisticRollbackResponse {
    fn key_errors(&mut self) -> Option<Vec<Error>> {
        extract_errors(std::mem::take(&mut self.errors).into_iter().map(Some))
    }
}

impl HasKeyErrors for kvrpcpb::FlushResponse {
    fn key_errors(&mut self) -> Option<Vec<Error>> {
        extract_errors(std::mem::take(&mut self.errors).into_iter().map(Some))
    }
}

impl HasKeyErrors for kvrpcpb::BufferBatchGetResponse {
    fn key_errors(&mut self) -> Option<Vec<Error>> {
        self.error
            .take()
            .map(|error| vec![error.into()])
            .or_else(|| extract_errors(self.pairs.iter_mut().map(|pair| pair.error.take())))
    }
}

impl HasKeyErrors for kvrpcpb::SplitRegionResponse {
    fn key_errors(&mut self) -> Option<Vec<Error>> {
        extract_errors(std::mem::take(&mut self.errors).into_iter().map(Some))
    }
}

impl HasKeyErrors for kvrpcpb::StoreSafeTsResponse {
    fn key_errors(&mut self) -> Option<Vec<Error>> {
        None
    }
}

impl HasKeyErrors for coprocessor::Response {
    fn key_errors(&mut self) -> Option<Vec<Error>> {
        if self.other_error.is_empty() {
            None
        } else {
            Some(vec![Error::KvError {
                message: std::mem::take(&mut self.other_error),
            }])
        }
    }
}

impl<T: HasKeyErrors> HasKeyErrors for Result<T, Error> {
    fn key_errors(&mut self) -> Option<Vec<Error>> {
        match self {
            Ok(x) => x.key_errors(),
            Err(Error::MultipleKeyErrors(errs)) => Some(std::mem::take(errs)),
            Err(e) => Some(vec![std::mem::replace(
                e,
                Error::StringError("".to_string()), // placeholder, no use.
            )]),
        }
    }
}

impl<T: HasKeyErrors> HasKeyErrors for Vec<T> {
    fn key_errors(&mut self) -> Option<Vec<Error>> {
        for t in self {
            if let Some(e) = t.key_errors() {
                return Some(e);
            }
        }

        None
    }
}

impl<T: HasRegionError, E> HasRegionError for Result<T, E> {
    fn region_error(&mut self) -> Option<crate::proto::errorpb::Error> {
        self.as_mut().ok().and_then(|t| t.region_error())
    }
}

impl<T: HasRegionError> HasRegionErrors for Vec<T> {
    fn region_errors(&mut self) -> Option<Vec<crate::proto::errorpb::Error>> {
        let errors: Vec<_> = self.iter_mut().filter_map(|x| x.region_error()).collect();
        if errors.is_empty() {
            None
        } else {
            Some(errors)
        }
    }
}

fn extract_errors(
    error_iter: impl Iterator<Item = Option<kvrpcpb::KeyError>>,
) -> Option<Vec<Error>> {
    let errors: Vec<Error> = error_iter.flatten().map(Into::into).collect();
    if errors.is_empty() {
        None
    } else {
        Some(errors)
    }
}

#[cfg(test)]
mod test {
    use super::{HasKeyErrors, RegionErrorResponse};
    use crate::common::Error;
    use crate::internal_err;
    use crate::proto::coprocessor;
    use crate::proto::kvrpcpb;

    fn assert_synthetic_region_error<T>()
    where
        T: RegionErrorResponse,
    {
        let mut response = T::from_region_error(crate::proto::errorpb::Error::default());
        assert!(response.region_error().is_some());
        assert!(response.region_error().is_none());
    }

    #[test]
    fn source_gen_region_error_response_matrix_is_complete() {
        macro_rules! assert_responses {
            ($($response:ty),+ $(,)?) => {{
                let mut count = 0;
                $(
                    assert_synthetic_region_error::<$response>();
                    count += 1;
                )+
                count
            }};
        }

        let count = assert_responses!(
            kvrpcpb::GetResponse,
            kvrpcpb::ScanResponse,
            kvrpcpb::PrewriteResponse,
            kvrpcpb::PessimisticLockResponse,
            kvrpcpb::PessimisticRollbackResponse,
            kvrpcpb::CommitResponse,
            kvrpcpb::CleanupResponse,
            kvrpcpb::BatchGetResponse,
            kvrpcpb::BatchRollbackResponse,
            kvrpcpb::ScanLockResponse,
            kvrpcpb::ResolveLockResponse,
            kvrpcpb::GcResponse,
            kvrpcpb::DeleteRangeResponse,
            kvrpcpb::RawGetResponse,
            kvrpcpb::RawBatchGetResponse,
            kvrpcpb::RawPutResponse,
            kvrpcpb::RawBatchPutResponse,
            kvrpcpb::RawDeleteResponse,
            kvrpcpb::RawBatchDeleteResponse,
            kvrpcpb::RawDeleteRangeResponse,
            kvrpcpb::RawScanResponse,
            kvrpcpb::UnsafeDestroyRangeResponse,
            kvrpcpb::RawGetKeyTtlResponse,
            kvrpcpb::RawCasResponse,
            kvrpcpb::RawChecksumResponse,
            coprocessor::Response,
            kvrpcpb::MvccGetByKeyResponse,
            kvrpcpb::MvccGetByStartTsResponse,
            kvrpcpb::SplitRegionResponse,
            kvrpcpb::TxnHeartBeatResponse,
            kvrpcpb::CheckTxnStatusResponse,
            kvrpcpb::CheckSecondaryLocksResponse,
            kvrpcpb::FlashbackToVersionResponse,
            kvrpcpb::PrepareFlashbackToVersionResponse,
            kvrpcpb::FlushResponse,
            kvrpcpb::BufferBatchGetResponse,
            kvrpcpb::GetHealthFeedbackResponse,
        );
        assert_eq!(count, 37);
    }

    #[test]
    fn result_haslocks() {
        let mut resp: Result<_, Error> = Ok(kvrpcpb::CommitResponse::default());
        assert!(resp.key_errors().is_none());

        let mut resp: Result<_, Error> = Ok(kvrpcpb::CommitResponse {
            error: Some(kvrpcpb::KeyError::default()),
            ..Default::default()
        });
        assert!(resp.key_errors().is_some());

        let mut resp: Result<kvrpcpb::CommitResponse, _> = Err(internal_err!("some error"));
        assert!(resp.key_errors().is_some());
    }

    #[test]
    fn scan_response_error_precedes_pair_errors() {
        let mut response = kvrpcpb::ScanResponse {
            error: Some(kvrpcpb::KeyError {
                abort: "response error".to_owned(),
                ..Default::default()
            }),
            pairs: vec![kvrpcpb::KvPair {
                error: Some(kvrpcpb::KeyError {
                    abort: "pair error".to_owned(),
                    ..Default::default()
                }),
                ..Default::default()
            }],
            ..Default::default()
        };

        assert!(response.key_errors().is_some());
        assert!(response.pairs[0].error.is_some());
    }
}
