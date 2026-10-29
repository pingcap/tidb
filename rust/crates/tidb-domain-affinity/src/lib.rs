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

//! Go `pkg/domain/affinity` (`interface.go`, `manager.go`, `manager_test.go`):
//! the package-level affinity group manager the DDL job workers ask to create
//! and delete PD affinity groups, and `SHOW AFFINITY` reads.
//!
//! The PD HTTP client is the [`PdAffinityClient`] seam, the subset of Go's
//! `pdhttp.Client` this package calls. With none (Go `InitManager(nil)`, a
//! store without PD such as unistore) the manager is the in-memory mock.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::{Arc, Mutex, PoisonError, RwLock};
use std::time::Duration;

/// Go `maxRetryTimes`: the retries for affinity group operations.
const MAX_RETRY_TIMES: usize = 3;
/// Go `retryInterval`: the sleep before the next retry.
const RETRY_INTERVAL: Duration = Duration::from_millis(200);
/// Go `maxAffinityGroupIDsQueryLen`: the URI budget for an `ids` query
/// before falling back to full-scan filtering.
const MAX_AFFINITY_GROUP_IDS_QUERY_LEN: usize = 4096;
/// Go `maxAffinityGroupIDsCount`.
const MAX_AFFINITY_GROUP_IDS_COUNT: usize = 100;

/// Go `pdhttp.AffinityGroupKeyRange`.
#[derive(Clone, Debug, Default, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct AffinityGroupKeyRange {
    /// Go `StartKey`, JSON-encoded as base64 like every Go `[]byte`.
    #[serde(with = "base64_bytes")]
    pub start_key: Vec<u8>,
    /// Go `EndKey`.
    #[serde(with = "base64_bytes")]
    pub end_key: Vec<u8>,
}

/// Go `pdhttp.AffinityGroup`.
#[derive(Clone, Debug, Default, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct AffinityGroup {
    /// Go `ID`.
    #[serde(default)]
    pub id: String,
    /// Go `CreateTimestamp`.
    #[serde(default)]
    pub create_timestamp: u64,
    /// Go `LeaderStoreID` (`omitempty`).
    #[serde(default, skip_serializing_if = "is_zero")]
    pub leader_store_id: u64,
    /// Go `VoterStoreIDs` (`omitempty`).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub voter_store_ids: Vec<u64>,
}

/// Go `pdhttp.AffinityGroupState`: the group and its runtime state.
#[derive(Clone, Debug, Default, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct AffinityGroupState {
    /// Go's embedded `AffinityGroup`.
    #[serde(flatten)]
    pub group: AffinityGroup,
    /// Go `Phase`: `pending`, `preparing` or `stable`.
    #[serde(default)]
    pub phase: String,
    /// Go `RangeCount`.
    #[serde(default)]
    pub range_count: i64,
    /// Go `RegionCount`.
    #[serde(default)]
    pub region_count: i64,
    /// Go `AffinityRegionCount`.
    #[serde(default)]
    pub affinity_region_count: i64,
}

impl AffinityGroupState {
    /// Go `IsPending`: still determining the store ids.
    #[must_use]
    pub fn is_pending(&self) -> bool {
        self.phase == "pending"
    }

    /// Go `IsPreparing`: scheduling regions onto the required peers.
    #[must_use]
    pub fn is_preparing(&self) -> bool {
        self.phase == "preparing"
    }

    /// Go `IsStable`: the scheduling is done.
    #[must_use]
    pub fn is_stable(&self) -> bool {
        self.phase == "stable"
    }
}

fn is_zero(value: &u64) -> bool {
    *value == 0
}

mod base64_bytes {
    use base64::Engine as _;

    pub(super) fn serialize<S: serde::Serializer>(
        bytes: &[u8],
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&base64::engine::general_purpose::STANDARD.encode(bytes))
    }

    pub(super) fn deserialize<'de, D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Vec<u8>, D::Error> {
        let text: Option<String> = serde::Deserialize::deserialize(deserializer)?;
        text.map_or(Ok(Vec::new()), |text| {
            base64::engine::general_purpose::STANDARD
                .decode(text)
                .map_err(serde::de::Error::custom)
        })
    }
}

/// Affinity groups keyed by id, Go's `map[string][]AffinityGroupKeyRange`.
pub type AffinityGroups = BTreeMap<String, Vec<AffinityGroupKeyRange>>;
/// Group states keyed by id, Go's `map[string]*AffinityGroupState`.
pub type AffinityGroupStates = HashMap<String, AffinityGroupState>;

/// An error from the PD HTTP client, as Go's error values carry it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PdHttpError {
    /// A plain error whose text is all Go's checks read, such as the PD
    /// client's `request pd http api failed with status: '...', body: '...'`.
    Message(String),
    /// A TiDB `*errors.Error` with its errno -- TiDB's injected PD response
    /// handler returns `ErrHTTPServiceError` without the HTTP status.
    Coded {
        /// The errno.
        code: u16,
        /// The message.
        message: String,
    },
}

impl std::fmt::Display for PdHttpError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Message(message) | Self::Coded { message, .. } => f.write_str(message),
        }
    }
}

impl std::error::Error for PdHttpError {}

/// The subset of Go's `pdhttp.Client` this package calls.
pub trait PdAffinityClient: Send + Sync {
    /// Go `CreateAffinityGroups(ctx, groups, opts...)`; `skip_exist_check` is
    /// `pdhttp.WithSkipExistCheck()`.
    fn create_affinity_groups(
        &self,
        groups: &AffinityGroups,
        skip_exist_check: bool,
    ) -> Result<AffinityGroupStates, PdHttpError>;
    /// Go `GetAffinityGroups(ctx, ids)`.
    fn get_affinity_groups(&self, ids: &[String]) -> Result<AffinityGroupStates, PdHttpError>;
    /// Go `GetAllAffinityGroups(ctx)`.
    fn get_all_affinity_groups(&self) -> Result<AffinityGroupStates, PdHttpError>;
    /// Go `BatchDeleteAffinityGroups(ctx, ids, force)`.
    fn batch_delete_affinity_groups(&self, ids: &[String], force: bool) -> Result<(), PdHttpError>;
}

/// Go `Manager`: affinity groups held by PD.
pub trait Manager: Send + Sync {
    /// Go `CreateAffinityGroupsIfNotExists`.
    fn create_affinity_groups_if_not_exists(
        &self,
        groups: &AffinityGroups,
    ) -> Result<(), PdHttpError>;
    /// Go `DeleteAffinityGroups`.
    fn delete_affinity_groups(&self, ids: &[String]) -> Result<(), PdHttpError>;
    /// Go `GetAffinityGroups`.
    fn get_affinity_groups(&self, ids: &[String]) -> Result<AffinityGroupStates, PdHttpError>;
}

/// Go `pdManager`: the manager over PD's HTTP API.
pub struct PdManager {
    client: Arc<dyn PdAffinityClient>,
}

impl PdManager {
    /// Go `NewPDManager`.
    #[must_use]
    pub fn new(client: Arc<dyn PdAffinityClient>) -> Self {
        Self { client }
    }

    /// Go `createAffinityGroupsIfNotExistsByFiltering`.
    fn create_affinity_groups_if_not_exists_by_filtering(
        &self,
        groups: &AffinityGroups,
    ) -> Result<(), PdHttpError> {
        // The map is ordered, so the ids are already Go's `sort.Strings`.
        let group_ids: Vec<String> = groups.keys().cloned().collect();
        let existing = self.get_affinity_groups(&group_ids)?;
        let to_create: AffinityGroups = groups
            .iter()
            .filter(|(id, _)| !existing.contains_key(*id))
            .map(|(id, ranges)| (id.clone(), ranges.clone()))
            .collect();
        if to_create.is_empty() {
            return Ok(());
        }
        self.client
            .create_affinity_groups(&to_create, false)
            .map(|_| ())
    }

    /// Go `getAffinityGroupsByScanningAll`.
    fn get_affinity_groups_by_scanning_all(
        &self,
        ids: &[String],
    ) -> Result<AffinityGroupStates, PdHttpError> {
        let all = self.client.get_all_affinity_groups()?;
        Ok(filter_affinity_groups(&all, ids))
    }
}

impl Manager for PdManager {
    /// Go: the `skip_exist_check` API first; an older PD that rejects it
    /// falls back to checking existing ids and creating only the missing
    /// groups, which keeps a DDL job retry safe in mixed-version clusters.
    fn create_affinity_groups_if_not_exists(
        &self,
        groups: &AffinityGroups,
    ) -> Result<(), PdHttpError> {
        if groups.is_empty() {
            return Ok(());
        }
        match self.client.create_affinity_groups(groups, true) {
            Ok(_) => Ok(()),
            Err(error) if should_fallback_create_affinity_groups(&error) => {
                self.create_affinity_groups_if_not_exists_by_filtering(groups)
            }
            Err(error) => Err(error),
        }
    }

    /// Go: delete with `force=true`.
    fn delete_affinity_groups(&self, ids: &[String]) -> Result<(), PdHttpError> {
        if ids.is_empty() {
            return Ok(());
        }
        self.client.batch_delete_affinity_groups(ids, true)
    }

    fn get_affinity_groups(&self, ids: &[String]) -> Result<AffinityGroupStates, PdHttpError> {
        if ids.is_empty() {
            return Ok(AffinityGroupStates::new());
        }
        if should_use_get_all_affinity_groups(ids) {
            return self.get_affinity_groups_by_scanning_all(ids);
        }
        match self.client.get_affinity_groups(ids) {
            // Older PD versions may ignore the `ids` query and return every
            // group; filtering keeps the requested-ids semantics stable.
            Ok(groups) => Ok(filter_affinity_groups(&groups, ids)),
            Err(error) if should_fallback_get_affinity_groups(&error) => {
                self.get_affinity_groups_by_scanning_all(ids)
            }
            Err(error) => Err(error),
        }
    }
}

/// Go `shouldUseGetAllAffinityGroups`.
fn should_use_get_all_affinity_groups(ids: &[String]) -> bool {
    ids.len() > MAX_AFFINITY_GROUP_IDS_COUNT
        || affinity_group_ids_escaped_query_len(ids) > MAX_AFFINITY_GROUP_IDS_QUERY_LEN
}

/// Go `affinityGroupIDsEscapedQueryLen`: the length of `ids=a&ids=b...`.
fn affinity_group_ids_escaped_query_len(ids: &[String]) -> usize {
    ids.iter()
        .enumerate()
        .map(|(position, id)| {
            let separator = if position == 0 { "ids=" } else { "&ids=" };
            separator.len() + query_escape(id).len()
        })
        .sum()
}

/// Go `net/url.QueryEscape`: every byte but `[A-Za-z0-9-_.~]` percent-encoded
/// in uppercase hex, a space as `+`.
#[must_use]
pub fn query_escape(text: &str) -> String {
    let mut escaped = String::with_capacity(text.len());
    for byte in text.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                escaped.push(char::from(byte));
            }
            b' ' => escaped.push('+'),
            _ => escaped.push_str(&format!("%{byte:02X}")),
        }
    }
    escaped
}

/// Go `shouldFallbackCreateAffinityGroups`.
fn should_fallback_create_affinity_groups(error: &PdHttpError) -> bool {
    is_pd_http_status_error(error, 400)
        || is_pd_http_status_error(error, 409)
        || is_pd_http_service_error_without_status(error)
}

/// Go `shouldFallbackGetAffinityGroups`.
fn should_fallback_get_affinity_groups(error: &PdHttpError) -> bool {
    is_pd_http_status_error(error, 400)
        || is_pd_http_status_error(error, 404)
        || is_pd_http_status_error(error, 414)
        || is_pd_http_service_error_without_status(error)
}

/// Go `isPDHTTPStatusError`.
fn is_pd_http_status_error(error: &PdHttpError, status_code: u16) -> bool {
    extract_pd_http_status_code(error) == Some(status_code)
}

/// Go `extractPDHTTPStatusCode`: the digits after `status: '` in the error's
/// text.
fn extract_pd_http_status_code(error: &PdHttpError) -> Option<u16> {
    const STATUS_PREFIX: &str = "status: '";
    let message = error.to_string();
    let start = message.find(STATUS_PREFIX)? + STATUS_PREFIX.len();
    let digits: String = message[start..]
        .chars()
        .take_while(char::is_ascii_digit)
        .collect();
    digits.parse().ok()
}

/// Go `isPDHTTPServiceErrorWithoutStatus`: TiDB's injected PD response
/// handler answers `ErrHTTPServiceError` without the HTTP status, so it is
/// treated as the compatibility case.
fn is_pd_http_service_error_without_status(error: &PdHttpError) -> bool {
    if extract_pd_http_status_code(error).is_some() {
        return false;
    }
    matches!(
        error,
        PdHttpError::Coded { code, .. }
            if *code == tidb_errno::errcode::ErrHTTPServiceError
    )
}

/// Go `filterAffinityGroups`: the requested ids PD answered, each once.
fn filter_affinity_groups(groups: &AffinityGroupStates, ids: &[String]) -> AffinityGroupStates {
    let mut result = AffinityGroupStates::with_capacity(ids.len());
    let mut seen = BTreeSet::new();
    for id in ids {
        if !seen.insert(id) {
            continue;
        }
        if let Some(group) = groups.get(id) {
            result.insert(id.clone(), group.clone());
        }
    }
    result
}

/// Go `mockManager`: groups held in memory, for a store without PD.
#[derive(Default)]
pub struct MockManager {
    groups: RwLock<AffinityGroupStates>,
}

impl MockManager {
    /// Go `NewMockManager`.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

impl Manager for MockManager {
    /// Go: idempotent, only the groups not already held are created.
    fn create_affinity_groups_if_not_exists(
        &self,
        groups: &AffinityGroups,
    ) -> Result<(), PdHttpError> {
        if groups.is_empty() {
            return Ok(());
        }
        let mut held = self.groups.write().unwrap_or_else(PoisonError::into_inner);
        for (id, ranges) in groups {
            held.entry(id.clone())
                .or_insert_with(|| AffinityGroupState {
                    group: AffinityGroup {
                        id: id.clone(),
                        ..AffinityGroup::default()
                    },
                    range_count: ranges.len() as i64,
                    ..AffinityGroupState::default()
                });
        }
        Ok(())
    }

    fn delete_affinity_groups(&self, ids: &[String]) -> Result<(), PdHttpError> {
        if ids.is_empty() {
            return Ok(());
        }
        let mut held = self.groups.write().unwrap_or_else(PoisonError::into_inner);
        for id in ids {
            held.remove(id);
        }
        Ok(())
    }

    fn get_affinity_groups(&self, ids: &[String]) -> Result<AffinityGroupStates, PdHttpError> {
        let held = self.groups.read().unwrap_or_else(PoisonError::into_inner);
        Ok(ids
            .iter()
            .filter_map(|id| held.get(id).map(|state| (id.clone(), state.clone())))
            .collect())
    }
}

/// Go's package-level `manager` and `pdClient`.
struct PackageState {
    manager: Arc<dyn Manager>,
    pd_client: Option<Arc<dyn PdAffinityClient>>,
}

static STATE: Mutex<Option<PackageState>> = Mutex::new(None);

fn manager() -> Arc<dyn Manager> {
    let mut state = STATE.lock().unwrap_or_else(PoisonError::into_inner);
    // Go reads a package variable that `infosync` always initializes; a
    // process that never called `init_manager` behaves as Go's nil client.
    Arc::clone(
        &state
            .get_or_insert_with(|| PackageState {
                manager: Arc::new(MockManager::new()),
                pd_client: None,
            })
            .manager,
    )
}

/// Go `InitManager`: the PD manager over `pd_client`, or the mock manager
/// when there is none. Called by the node's info syncer at startup.
pub fn init_manager(pd_client: Option<Arc<dyn PdAffinityClient>>) {
    let manager: Arc<dyn Manager> = match &pd_client {
        Some(client) => Arc::new(PdManager::new(Arc::clone(client))),
        None => Arc::new(MockManager::new()),
    };
    *STATE.lock().unwrap_or_else(PoisonError::into_inner) =
        Some(PackageState { manager, pd_client });
}

/// Go `CreateGroupsIfNotExists`: idempotent, which keeps a DDL job retry
/// (after an owner switch or a network failure) safe.
pub fn create_groups_if_not_exists(groups: &AffinityGroups) -> Result<(), PdHttpError> {
    if groups.is_empty() {
        return Ok(());
    }
    manager().create_affinity_groups_if_not_exists(groups)
}

/// Go `DeleteGroups`: removes the groups with `force=true`.
pub fn delete_groups(ids: &[String]) -> Result<(), PdHttpError> {
    if ids.is_empty() {
        return Ok(());
    }
    manager().delete_affinity_groups(ids)
}

/// Go `DeleteGroupsWithRetry`: best-effort cleanup, logged on the final
/// failure.
pub fn delete_groups_with_retry(ids: &[String]) -> Result<(), PdHttpError> {
    if ids.is_empty() {
        return Ok(());
    }
    let mut attempt = 0;
    loop {
        match delete_groups(ids) {
            Ok(()) => return Ok(()),
            Err(error) if attempt == MAX_RETRY_TIMES => {
                tracing::error!(
                    error = %error,
                    group_ids = ?ids,
                    "Failed to delete affinity groups after retries"
                );
                return Err(error);
            }
            Err(_) => {
                std::thread::sleep(RETRY_INTERVAL);
                attempt += 1;
            }
        }
    }
}

/// Go `GetGroups`.
pub fn get_groups(ids: &[String]) -> Result<AffinityGroupStates, PdHttpError> {
    if ids.is_empty() {
        return Ok(AffinityGroupStates::new());
    }
    manager().get_affinity_groups(ids)
}

/// Go `GetAllGroupStates`: every group PD holds, read from PD directly
/// (`SHOW AFFINITY`); none without a PD client.
pub fn get_all_group_states() -> Result<AffinityGroupStates, PdHttpError> {
    let client = STATE
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .as_ref()
        .and_then(|state| state.pd_client.clone());
    match client {
        Some(client) => client.get_all_affinity_groups(),
        None => Ok(AffinityGroupStates::new()),
    }
}

/// Go `SetPDClientForTest`: installs `client` for `GetAllGroupStates` and
/// returns the restore.
pub fn set_pd_client_for_test(client: Option<Arc<dyn PdAffinityClient>>) -> impl FnOnce() {
    let original = {
        let mut state = STATE.lock().unwrap_or_else(PoisonError::into_inner);
        let state = state.get_or_insert_with(|| PackageState {
            manager: Arc::new(MockManager::new()),
            pd_client: None,
        });
        std::mem::replace(&mut state.pd_client, client)
    };
    move || {
        if let Some(state) = STATE
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .as_mut()
        {
            state.pd_client = original;
        }
    }
}

#[cfg(test)]
mod tests {
    //! Go `manager_test.go`.

    use super::*;
    use std::collections::VecDeque;

    /// Go `mockPDClient`: each call pops the next scripted answer for its
    /// method and records its arguments.
    #[derive(Default)]
    struct MockPdClient {
        create: Mutex<VecDeque<(AffinityGroups, bool, Result<(), PdHttpError>)>>,
        get: Mutex<VecDeque<(Vec<String>, Result<AffinityGroupStates, PdHttpError>)>>,
        get_all: Mutex<VecDeque<Result<AffinityGroupStates, PdHttpError>>>,
    }

    impl MockPdClient {
        fn expect_create(
            &self,
            groups: AffinityGroups,
            skip_exist_check: bool,
            answer: Result<(), PdHttpError>,
        ) {
            self.create
                .lock()
                .unwrap()
                .push_back((groups, skip_exist_check, answer));
        }

        fn expect_get(&self, ids: &[&str], answer: Result<AffinityGroupStates, PdHttpError>) {
            self.get
                .lock()
                .unwrap()
                .push_back((ids.iter().map(|id| (*id).to_owned()).collect(), answer));
        }

        fn expect_get_all(&self, answer: Result<AffinityGroupStates, PdHttpError>) {
            self.get_all.lock().unwrap().push_back(answer);
        }

        /// Go `AssertExpectations`: every scripted call happened.
        fn assert_expectations(&self) {
            assert!(
                self.create.lock().unwrap().is_empty(),
                "CreateAffinityGroups"
            );
            assert!(self.get.lock().unwrap().is_empty(), "GetAffinityGroups");
            assert!(
                self.get_all.lock().unwrap().is_empty(),
                "GetAllAffinityGroups"
            );
        }
    }

    impl PdAffinityClient for MockPdClient {
        fn create_affinity_groups(
            &self,
            groups: &AffinityGroups,
            skip_exist_check: bool,
        ) -> Result<AffinityGroupStates, PdHttpError> {
            let (expected, expected_skip, answer) = self
                .create
                .lock()
                .unwrap()
                .pop_front()
                .expect("unexpected CreateAffinityGroups call");
            assert_eq!(groups, &expected);
            assert_eq!(skip_exist_check, expected_skip);
            answer.map(|()| AffinityGroupStates::new())
        }

        fn get_affinity_groups(&self, ids: &[String]) -> Result<AffinityGroupStates, PdHttpError> {
            let (expected, answer) = self
                .get
                .lock()
                .unwrap()
                .pop_front()
                .expect("unexpected GetAffinityGroups call");
            assert_eq!(ids, expected.as_slice());
            answer
        }

        fn get_all_affinity_groups(&self) -> Result<AffinityGroupStates, PdHttpError> {
            self.get_all
                .lock()
                .unwrap()
                .pop_front()
                .expect("unexpected GetAllAffinityGroups call")
        }

        fn batch_delete_affinity_groups(
            &self,
            _ids: &[String],
            _force: bool,
        ) -> Result<(), PdHttpError> {
            panic!("unexpected BatchDeleteAffinityGroups call")
        }
    }

    fn range(start: &str, end: &str) -> AffinityGroupKeyRange {
        AffinityGroupKeyRange {
            start_key: start.as_bytes().to_vec(),
            end_key: end.as_bytes().to_vec(),
        }
    }

    fn groups(entries: &[(&str, AffinityGroupKeyRange)]) -> AffinityGroups {
        entries
            .iter()
            .map(|(id, range)| ((*id).to_owned(), vec![range.clone()]))
            .collect()
    }

    /// Go `affinityGroupState`.
    fn affinity_group_state(id: &str) -> AffinityGroupState {
        AffinityGroupState {
            group: AffinityGroup {
                id: id.to_owned(),
                ..AffinityGroup::default()
            },
            ..AffinityGroupState::default()
        }
    }

    fn states(ids: &[&str]) -> AffinityGroupStates {
        ids.iter()
            .map(|id| ((*id).to_owned(), affinity_group_state(id)))
            .collect()
    }

    /// Go `pdStatusErr`.
    fn pd_status_err(status_code: u16, text: &str) -> PdHttpError {
        PdHttpError::Message(format!(
            "request pd http api failed with status: '{status_code} {text}', body: 'test'"
        ))
    }

    /// Go `pdHTTPServiceErr`.
    fn pd_http_service_err(message: &str) -> PdHttpError {
        PdHttpError::Coded {
            code: tidb_errno::errcode::ErrHTTPServiceError,
            message: format!("HTTP request failed with status {message}"),
        }
    }

    fn manager_over(client: &Arc<MockPdClient>) -> PdManager {
        PdManager::new(Arc::clone(client) as Arc<dyn PdAffinityClient>)
    }

    #[test]
    fn create_affinity_groups_if_not_exists_use_skip_exist_check() {
        let client = Arc::new(MockPdClient::default());
        let manager = manager_over(&client);
        let groups = groups(&[("g1", range("a", "b"))]);
        client.expect_create(groups.clone(), true, Ok(()));
        manager
            .create_affinity_groups_if_not_exists(&groups)
            .unwrap();
        client.assert_expectations();
    }

    #[test]
    fn create_affinity_groups_if_not_exists_fallback_when_skip_exist_rejected() {
        let client = Arc::new(MockPdClient::default());
        let manager = manager_over(&client);
        let groups = groups(&[("g1", range("a", "b")), ("g2", range("c", "d"))]);
        client.expect_create(groups.clone(), true, Err(pd_status_err(409, "Conflict")));
        client.expect_get(&["g1", "g2"], Ok(states(&["g1"])));
        client.expect_create(
            [("g2".to_owned(), groups["g2"].clone())]
                .into_iter()
                .collect(),
            false,
            Ok(()),
        );
        manager
            .create_affinity_groups_if_not_exists(&groups)
            .unwrap();
        client.assert_expectations();
    }

    #[test]
    fn create_affinity_groups_if_not_exists_fallback_for_http_service_error() {
        let client = Arc::new(MockPdClient::default());
        let manager = manager_over(&client);
        let groups = groups(&[("g1", range("a", "b")), ("g2", range("c", "d"))]);
        client.expect_create(
            groups.clone(),
            true,
            Err(pd_http_service_err("mock service error")),
        );
        client.expect_get(&["g1", "g2"], Ok(states(&["g1"])));
        client.expect_create(
            [("g2".to_owned(), groups["g2"].clone())]
                .into_iter()
                .collect(),
            false,
            Ok(()),
        );
        manager
            .create_affinity_groups_if_not_exists(&groups)
            .unwrap();
        client.assert_expectations();
    }

    #[test]
    fn create_affinity_groups_if_not_exists_do_not_fallback_for_non_compatibility_error() {
        let client = Arc::new(MockPdClient::default());
        let manager = manager_over(&client);
        let groups = groups(&[("g1", range("a", "b"))]);
        let test_err = pd_status_err(500, "Internal Server Error");
        client.expect_create(groups.clone(), true, Err(test_err.clone()));
        assert_eq!(
            manager.create_affinity_groups_if_not_exists(&groups),
            Err(test_err)
        );
        client.assert_expectations();
    }

    #[test]
    fn get_affinity_groups_filter_direct_response() {
        let client = Arc::new(MockPdClient::default());
        let manager = manager_over(&client);
        client.expect_get(&["g1", "g2"], Ok(states(&["g1", "g2", "g3"])));
        let result = manager
            .get_affinity_groups(&["g1".to_owned(), "g2".to_owned()])
            .unwrap();
        assert_eq!(result.len(), 2);
        assert!(result.contains_key("g1"));
        assert!(result.contains_key("g2"));
        assert!(!result.contains_key("g3"));
        client.assert_expectations();
    }

    #[test]
    fn get_affinity_groups_fallback_by_escaped_query_len() {
        let client = Arc::new(MockPdClient::default());
        let manager = manager_over(&client);
        let id = "/".repeat(1365);
        let mut all = states(&["other"]);
        all.insert(id.clone(), affinity_group_state(&id));
        client.expect_get_all(Ok(all));
        let result = manager.get_affinity_groups(&[id.clone()]).unwrap();
        assert_eq!(result.len(), 1);
        assert!(result.contains_key(&id));
        client.assert_expectations();
    }

    #[test]
    fn get_affinity_groups_fallback_when_ids_query_unsupported() {
        let client = Arc::new(MockPdClient::default());
        let manager = manager_over(&client);
        client.expect_get(&["g1"], Err(pd_status_err(400, "Bad Request")));
        client.expect_get_all(Ok(states(&["g1", "g2"])));
        let result = manager.get_affinity_groups(&["g1".to_owned()]).unwrap();
        assert_eq!(result.len(), 1);
        assert!(result.contains_key("g1"));
        assert!(!result.contains_key("g2"));
        client.assert_expectations();
    }

    #[test]
    fn get_affinity_groups_fallback_for_http_service_error() {
        let client = Arc::new(MockPdClient::default());
        let manager = manager_over(&client);
        client.expect_get(&["g1"], Err(pd_http_service_err("mock service error")));
        client.expect_get_all(Ok(states(&["g1", "g2"])));
        let result = manager.get_affinity_groups(&["g1".to_owned()]).unwrap();
        assert_eq!(result.len(), 1);
        assert!(result.contains_key("g1"));
        assert!(!result.contains_key("g2"));
        client.assert_expectations();
    }

    #[test]
    fn affinity_group_ids_escaped_query_len_boundary() {
        assert_eq!(
            affinity_group_ids_escaped_query_len(&["/".repeat(1364)]),
            MAX_AFFINITY_GROUP_IDS_QUERY_LEN
        );
        assert_eq!(
            affinity_group_ids_escaped_query_len(&["/".repeat(1365)]),
            MAX_AFFINITY_GROUP_IDS_QUERY_LEN + 3
        );
    }

    #[test]
    fn should_use_get_all_affinity_groups_by_id_count() {
        let ids: Vec<String> = (0..=MAX_AFFINITY_GROUP_IDS_COUNT)
            .map(|position| format!("g{position}"))
            .collect();
        assert!(should_use_get_all_affinity_groups(&ids));
    }

    #[test]
    fn is_pd_http_status_error_match_by_code_only() {
        let error = PdHttpError::Message(
            "request pd http api failed with status: '400 UnknownText', body: 'test'".to_owned(),
        );
        assert!(is_pd_http_status_error(&error, 400));
        assert!(!is_pd_http_status_error(&error, 409));
    }

    #[test]
    fn mock_manager_creates_idempotently_and_deletes() {
        let manager = MockManager::new();
        let first = groups(&[("g1", range("a", "b"))]);
        manager
            .create_affinity_groups_if_not_exists(&first)
            .unwrap();
        let mut again = first.clone();
        again.get_mut("g1").unwrap().push(range("c", "d"));
        manager
            .create_affinity_groups_if_not_exists(&again)
            .unwrap();
        let held = manager.get_affinity_groups(&["g1".to_owned()]).unwrap();
        assert_eq!(held["g1"].range_count, 1);
        manager.delete_affinity_groups(&["g1".to_owned()]).unwrap();
        assert!(manager
            .get_affinity_groups(&["g1".to_owned()])
            .unwrap()
            .is_empty());
    }

    #[test]
    fn query_escape_matches_go() {
        assert_eq!(query_escape("a b/c~d_e.f-g"), "a+b%2Fc~d_e.f-g");
    }
}
