//! Remote endpoint classification shared by TiKV request routing.
//!
//! This is the native mapping of client-go's `tikvrpc/endpoint.go`.  The
//! command/request registry remains a separate unfinished `tikvrpc` slice.

use crate::proto::metapb;

pub const ENGINE_LABEL_KEY: &str = "engine";
pub const ENGINE_LABEL_TIFLASH: &str = "tiflash";
pub const ENGINE_LABEL_TIFLASH_COMPUTE: &str = "tiflash_compute";
#[allow(dead_code)]
pub const ENGINE_ROLE_LABEL_KEY: &str = "engine_role";
#[allow(dead_code)]
pub const ENGINE_ROLE_WRITE: &str = "write";

/// The kind of remote endpoint selected for an RPC.
#[allow(dead_code)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum EndpointType {
    #[default]
    TiKv,
    TiFlash,
    TiDb,
    TiFlashCompute,
}

#[allow(dead_code)]
impl EndpointType {
    pub const fn name(self) -> &'static str {
        match self {
            Self::TiKv => "tikv",
            Self::TiFlash => "tiflash",
            Self::TiDb => "tidb",
            Self::TiFlashCompute => "tiflash_compute",
        }
    }

    pub const fn is_tiflash_related(self) -> bool {
        matches!(self, Self::TiFlash | Self::TiFlashCompute)
    }

    /// Classifies a PD store using client-go's engine labels. Unknown and
    /// unlabelled stores are ordinary TiKV endpoints.
    pub fn from_store(store: &metapb::Store) -> Self {
        for label in &store.labels {
            if label.key == ENGINE_LABEL_KEY && label.value == ENGINE_LABEL_TIFLASH {
                return Self::TiFlash;
            }
            if label.key == ENGINE_LABEL_KEY && label.value == ENGINE_LABEL_TIFLASH_COMPUTE {
                return Self::TiFlashCompute;
            }
        }
        Self::TiKv
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn endpoint_names_and_tiflash_detection_match_client_go() {
        assert_eq!(EndpointType::TiKv.name(), "tikv");
        assert_eq!(EndpointType::TiFlash.name(), "tiflash");
        assert_eq!(EndpointType::TiDb.name(), "tidb");
        assert_eq!(EndpointType::TiFlashCompute.name(), "tiflash_compute");
        assert!(!EndpointType::TiKv.is_tiflash_related());
        assert!(EndpointType::TiFlash.is_tiflash_related());
        assert!(EndpointType::TiFlashCompute.is_tiflash_related());
    }

    #[test]
    fn store_engine_labels_select_the_source_endpoint_type() {
        let mut store = metapb::Store::default();
        assert_eq!(EndpointType::from_store(&store), EndpointType::TiKv);

        store.labels.push(metapb::StoreLabel {
            key: ENGINE_LABEL_KEY.to_owned(),
            value: ENGINE_LABEL_TIFLASH.to_owned(),
        });
        assert_eq!(EndpointType::from_store(&store), EndpointType::TiFlash);

        store.labels[0].value = ENGINE_LABEL_TIFLASH_COMPUTE.to_owned();
        assert_eq!(
            EndpointType::from_store(&store),
            EndpointType::TiFlashCompute
        );

        store.labels[0].value = "unknown".to_owned();
        assert_eq!(EndpointType::from_store(&store), EndpointType::TiKv);
    }
}
