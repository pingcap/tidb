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

//! Standalone-UniStore process mode from `pkg/kv/unistore.go`, plus the mock
//! PD discovery and keyspace catalog used by `pkg/store/mockstore/unistore`.

use std::sync::atomic::{AtomicBool, Ordering};

/// Whether this TiDB process uses standalone UniStore.
///
/// Atomic access preserves the source process-global behavior without exposing
/// a data race.
pub static STANDALONE_TIDB: AtomicBool = AtomicBool::new(false);

/// Returns the current standalone-UniStore mode.
#[must_use]
pub fn standalone_tidb() -> bool {
    STANDALONE_TIDB.load(Ordering::Acquire)
}

/// Updates standalone-UniStore mode during process configuration.
pub fn set_standalone_tidb(enabled: bool) {
    STANDALONE_TIDB.store(enabled, Ordering::Release);
}

#[cfg(test)]
mod mock_pd {
    use std::collections::HashMap;
    use std::fmt;

    use tonic::transport::Endpoint;

    /// Highest keyspace ID accepted by PD client-go.
    pub const MAX_KEYSPACE_ID: u32 = 0x00ff_ffff;
    /// Sentinel for a legacy request that has no keyspace metadata.
    pub const NULL_KEYSPACE_ID: u32 = u32::MAX;

    /// One in-memory PD service client used by the UniStore mock.
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct MockPdServiceClient {
        url: String,
    }

    impl MockPdServiceClient {
        /// Go `ServiceClient.GetURL`.
        #[must_use]
        pub fn url(&self) -> &str {
            &self.url
        }
    }

    /// UniStore's filtered, immutable mock PD service discovery.
    #[derive(Clone, Debug, Default, Eq, PartialEq)]
    pub struct MockPdServiceDiscovery {
        service_urls: Vec<String>,
        clients: Vec<MockPdServiceClient>,
    }

    impl MockPdServiceDiscovery {
        /// Go `NewMockPDServiceDiscovery`.
        #[must_use]
        pub fn new<I, S>(addresses: I) -> Self
        where
            I: IntoIterator<Item = S>,
            S: AsRef<str>,
        {
            let mut service_urls = Vec::new();
            let mut clients = Vec::new();
            for address in addresses {
                let address = address.as_ref();
                if let Some(url) = normalize_mock_pd_url(address) {
                    service_urls.push(address.to_owned());
                    clients.push(MockPdServiceClient { url });
                }
            }
            Self {
                service_urls,
                clients,
            }
        }

        /// Go `ServiceDiscovery.GetServiceURLs`.
        #[must_use]
        pub fn service_urls(&self) -> &[String] {
            &self.service_urls
        }

        /// Go `ServiceDiscovery.GetAllServiceClients`.
        #[must_use]
        pub fn all_service_clients(&self) -> &[MockPdServiceClient] {
            &self.clients
        }
    }

    fn normalize_mock_pd_url(address: &str) -> Option<String> {
        if address.is_empty()
            || address.starts_with('.')
            || address.chars().any(char::is_whitespace)
        {
            return None;
        }

        let validation_url = if let Some((scheme, rest)) = address.split_once("://") {
            if !matches!(
                scheme,
                "ftp" | "tcp" | "udp" | "ws" | "wss" | "http" | "https"
            ) {
                return None;
            }
            format!("http://{rest}")
        } else if address.contains(':') || address.contains('.') {
            format!("http://{address}")
        } else {
            return None;
        };
        Endpoint::from_shared(validation_url).ok()?;

        Some(if address.starts_with("http") {
            address.to_owned()
        } else {
            format!("http://{address}")
        })
    }

    /// One keyspace metadata entry managed by [`MockKeyspaceManager`].
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct PdKeyspace {
        /// Keyspace identity.
        pub id: u32,
        /// Unique keyspace name.
        pub name: String,
    }

    impl PdKeyspace {
        /// Constructs one mock keyspace metadata entry.
        #[must_use]
        pub fn new(id: u32, name: impl Into<String>) -> Self {
            Self {
                id,
                name: name.into(),
            }
        }
    }

    /// Input or lookup failure from [`MockKeyspaceManager`].
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub enum MockKeyspaceError {
        /// A metadata ID exceeds [`MAX_KEYSPACE_ID`], including the null sentinel.
        InvalidId(u32),
        /// Two entries use the same ID.
        DuplicateId(u32),
        /// Two entries use the same name.
        DuplicateName(String),
        /// Go `pdpb.ErrorType_ENTRY_NOT_FOUND`.
        EntryNotFound,
    }

    impl fmt::Display for MockKeyspaceError {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                Self::InvalidId(id) => write!(formatter, "invalid keyspace ID {id}"),
                Self::DuplicateId(id) => write!(formatter, "keyspace ID {id} duplicated"),
                Self::DuplicateName(name) => write!(formatter, "keyspace name {name} duplicated"),
                Self::EntryNotFound => formatter.write_str("ENTRY_NOT_FOUND"),
            }
        }
    }

    impl std::error::Error for MockKeyspaceError {}

    /// Sorted in-memory keyspace metadata used by the UniStore mock PD client.
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct MockKeyspaceManager {
        keyspaces: Vec<PdKeyspace>,
        keyspace_names: HashMap<String, u32>,
    }

    impl MockKeyspaceManager {
        /// Go `newMockKeyspaceManager`: validates uniqueness and sorts by ID.
        pub fn new(mut keyspaces: Vec<PdKeyspace>) -> Result<Self, MockKeyspaceError> {
            keyspaces.sort_unstable_by_key(|keyspace| keyspace.id);
            let mut keyspace_names = HashMap::with_capacity(keyspaces.len());
            for (index, keyspace) in keyspaces.iter().enumerate() {
                if keyspace.id > MAX_KEYSPACE_ID {
                    return Err(MockKeyspaceError::InvalidId(keyspace.id));
                }
                if index > 0 && keyspace.id == keyspaces[index - 1].id {
                    return Err(MockKeyspaceError::DuplicateId(keyspace.id));
                }
                if keyspace_names
                    .insert(keyspace.name.clone(), keyspace.id)
                    .is_some()
                {
                    return Err(MockKeyspaceError::DuplicateName(keyspace.name.clone()));
                }
            }
            Ok(Self {
                keyspaces,
                keyspace_names,
            })
        }

        /// Go `LoadKeyspace`.
        pub fn load(&self, name: &str) -> Result<&PdKeyspace, MockKeyspaceError> {
            let id = self
                .keyspace_names
                .get(name)
                .copied()
                .ok_or(MockKeyspaceError::EntryNotFound)?;
            let index = self
                .keyspaces
                .binary_search_by_key(&id, |keyspace| keyspace.id)
                .expect("keyspace metadata list and name map must agree");
            Ok(&self.keyspaces[index])
        }

        /// Go `LoadKeyspaceByID`.
        pub fn load_by_id(&self, id: u32) -> Result<&PdKeyspace, MockKeyspaceError> {
            self.keyspaces
                .binary_search_by_key(&id, |keyspace| keyspace.id)
                .map(|index| &self.keyspaces[index])
                .map_err(|_| MockKeyspaceError::EntryNotFound)
        }

        /// Go `GetAllKeyspaces`: starts at the first ID greater than or equal to
        /// `start_id`; a zero limit returns the complete remaining suffix.
        #[must_use]
        pub fn all(&self, start_id: u32, limit: u32) -> &[PdKeyspace] {
            let start = self
                .keyspaces
                .partition_point(|keyspace| keyspace.id < start_id);
            let limit = usize::try_from(limit).unwrap_or(usize::MAX);
            let end = if limit == 0 {
                self.keyspaces.len()
            } else {
                start.saturating_add(limit).min(self.keyspaces.len())
            };
            &self.keyspaces[start..end]
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        fn keyspaces(entries: &[(u32, &str)]) -> Vec<PdKeyspace> {
            entries
                .iter()
                .map(|(id, name)| PdKeyspace::new(*id, *name))
                .collect()
        }

        fn assert_entries(actual: &[PdKeyspace], expected: &[(u32, &str)]) {
            let actual: Vec<_> = actual
                .iter()
                .map(|keyspace| (keyspace.id, keyspace.name.as_str()))
                .collect();
            assert_eq!(actual, expected);
        }

        fn assert_manager(manager: &MockKeyspaceManager, expected: &[(u32, &str)]) {
            assert_eq!(manager.keyspaces.len(), expected.len());
            assert_eq!(manager.keyspace_names.len(), expected.len());
            assert_entries(&manager.keyspaces, expected);
            for (id, name) in expected {
                assert_eq!(manager.keyspace_names.get(*name), Some(id));
            }
        }

        fn assert_load(
            manager: &MockKeyspaceManager,
            name: &str,
            expected: Result<u32, MockKeyspaceError>,
        ) {
            match expected {
                Ok(id) => {
                    let keyspace = manager.load(name).unwrap();
                    assert_eq!(keyspace.id, id);
                    assert_eq!(keyspace.name, name);
                }
                Err(error) => assert_eq!(manager.load(name).unwrap_err(), error),
            }
        }

        fn assert_list(
            manager: &MockKeyspaceManager,
            start_id: u32,
            limit: u32,
            expected: &[(u32, &str)],
        ) {
            assert_entries(manager.all(start_id, limit), expected);
        }

        /// Source: `pkg/store/mockstore/unistore/pd_test.go::TestMockPDServiceDiscovery`.
        #[test]
        fn test_mock_pd_service_discovery() {
            let discovery = MockPdServiceDiscovery::new([
                "invalid_pd_address",
                "127.0.0.1:2379",
                "http://172.32.21.32:2379",
            ]);
            let clients = discovery.all_service_clients();
            assert_eq!(clients.len(), 2);
            assert_eq!(clients[0].url(), "http://127.0.0.1:2379");
            assert_eq!(clients[1].url(), "http://172.32.21.32:2379");
            assert_eq!(
                discovery.service_urls(),
                ["127.0.0.1:2379", "http://172.32.21.32:2379"]
            );
        }

        /// Source: `pkg/store/mockstore/unistore/pd_test.go::TestMockKeyspaceManager`.
        #[test]
        fn test_mock_keyspace_manager() {
            let manager = MockKeyspaceManager::new(vec![]).unwrap();
            assert_manager(&manager, &[]);
            assert_load(&manager, "DEFAULT", Err(MockKeyspaceError::EntryNotFound));
            assert_list(&manager, 0, 0, &[]);

            let manager = MockKeyspaceManager::new(keyspaces(&[(0, "DEFAULT")])).unwrap();
            assert_manager(&manager, &[(0, "DEFAULT")]);
            assert_load(&manager, "DEFAULT", Ok(0));
            assert_load(&manager, "ks1", Err(MockKeyspaceError::EntryNotFound));
            assert_list(&manager, 0, 0, &[(0, "DEFAULT")]);
            assert_list(&manager, 1, 0, &[]);

            let manager = MockKeyspaceManager::new(keyspaces(&[
                (1, "ks1"),
                (4, "ks4"),
                (2, "ks2"),
                (5, "ks5"),
                (3, "ks3"),
            ]))
            .unwrap();
            let first_five = &[(1, "ks1"), (2, "ks2"), (3, "ks3"), (4, "ks4"), (5, "ks5")];
            assert_manager(&manager, first_five);
            for (id, name) in first_five {
                assert_load(&manager, name, Ok(*id));
            }
            assert_load(&manager, "ks6", Err(MockKeyspaceError::EntryNotFound));
            assert_eq!(manager.load_by_id(3).unwrap().name, "ks3");
            assert_eq!(
                manager.load_by_id(6).unwrap_err(),
                MockKeyspaceError::EntryNotFound
            );
            assert_list(&manager, 0, 0, first_five);
            assert_list(&manager, 0, 3, &first_five[..3]);
            assert_list(&manager, 1, 0, first_five);
            assert_list(&manager, 3, 0, &first_five[2..]);
            assert_list(&manager, 3, 2, &first_five[2..4]);
            assert_list(&manager, 5, 0, &first_five[4..]);

            let manager = MockKeyspaceManager::new(keyspaces(&[
                (100, "ks100"),
                (1, "ks1"),
                (MAX_KEYSPACE_ID, "lastks"),
                (10, "ks10"),
            ]))
            .unwrap();
            let sparse = &[
                (1, "ks1"),
                (10, "ks10"),
                (100, "ks100"),
                (MAX_KEYSPACE_ID, "lastks"),
            ];
            assert_manager(&manager, sparse);
            assert_list(&manager, 0, 0, sparse);
            assert_list(&manager, 5, 0, &sparse[1..]);
            assert_list(&manager, 5, 1, &sparse[1..2]);
            assert_list(&manager, 10, 0, &sparse[1..]);
            assert_list(&manager, 11, 0, &sparse[2..]);
            assert_list(&manager, 99, 0, &sparse[2..]);
            assert_list(&manager, 101, 0, &sparse[3..]);
            assert_list(&manager, MAX_KEYSPACE_ID, 0, &sparse[3..]);

            assert_eq!(
                MockKeyspaceManager::new(keyspaces(&[
                    (1, "ks1"),
                    (2, "ks2"),
                    (3, "ks3"),
                    (1, "ks4"),
                ]))
                .unwrap_err(),
                MockKeyspaceError::DuplicateId(1)
            );
            assert_eq!(
                MockKeyspaceManager::new(keyspaces(&[
                    (1, "ks1"),
                    (2, "ks2"),
                    (3, "ks3"),
                    (4, "ks1"),
                ]))
                .unwrap_err(),
                MockKeyspaceError::DuplicateName("ks1".to_owned())
            );
            assert_eq!(
                MockKeyspaceManager::new(keyspaces(&[(0x0100_0000, "illegal")])).unwrap_err(),
                MockKeyspaceError::InvalidId(0x0100_0000)
            );
            assert_eq!(
                MockKeyspaceManager::new(keyspaces(&[(NULL_KEYSPACE_ID, "")])).unwrap_err(),
                MockKeyspaceError::InvalidId(NULL_KEYSPACE_ID)
            );
        }
    }
}
