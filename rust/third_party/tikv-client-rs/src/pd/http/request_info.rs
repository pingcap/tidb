// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! Go `request_info.go`: request names (the metrics and log label of each API)
//! and the per-request description the retry loop consumes.

use reqwest::Method;

use super::client::RespHandleFunc;
use crate::pd::backoff::Backoffer;

// The following constants are the names of the requests.
pub(super) const GET_MEMBERS_NAME: &str = "GetMembers";
pub(super) const GET_LEADER_NAME: &str = "GetLeader";
pub(super) const TRANSFER_LEADER_NAME: &str = "TransferLeader";
pub(super) const GET_REGION_BY_ID_NAME: &str = "GetRegionByID";
pub(super) const GET_REGION_BY_KEY_NAME: &str = "GetRegionByKey";
pub(super) const GET_REGIONS_NAME: &str = "GetRegions";
pub(super) const GET_REGIONS_BY_KEY_RANGE_NAME: &str = "GetRegionsByKeyRange";
pub(super) const GET_REGIONS_BY_STORE_ID_NAME: &str = "GetRegionsByStoreID";
pub(super) const GET_EMPTY_REGIONS_NAME: &str = "GetEmptyRegions";
pub(super) const GET_REGIONS_REPLICATED_STATE_BY_KEY_RANGE_NAME: &str =
    "GetRegionsReplicatedStateByKeyRange";
pub(super) const GET_REGION_SIBLINGS_BY_ID: &str = "GetRegionSiblingsByID";
pub(super) const GET_HOT_READ_REGIONS_NAME: &str = "GetHotReadRegions";
pub(super) const GET_HOT_WRITE_REGIONS_NAME: &str = "GetHotWriteRegions";
pub(super) const GET_HISTORY_HOT_REGIONS_NAME: &str = "GetHistoryHotRegions";
pub(super) const GET_REGION_STATUS_BY_KEY_RANGE_NAME: &str = "GetRegionStatusByKeyRange";
pub(super) const GET_REGION_DISTRIBUTIONS_BY_KEY_RANGE_NAME: &str =
    "GetRegionDistributionByKeyRange";
pub(super) const GET_STORES_NAME: &str = "GetStores";
pub(super) const GET_STORE_NAME: &str = "GetStore";
pub(super) const DELETE_STORE_NAME: &str = "DeleteStore";
pub(super) const SET_STORE_LABELS_NAME: &str = "SetStoreLabels";
pub(super) const DELETE_STORE_LABEL_NAME: &str = "DeleteStoreLabel";
pub(super) const GET_HEALTH_STATUS_NAME: &str = "GetHealthStatus";
pub(super) const GET_CONFIG_NAME: &str = "GetConfig";
pub(super) const SET_CONFIG_NAME: &str = "SetConfig";
pub(super) const GET_SCHEDULE_CONFIG_NAME: &str = "GetScheduleConfig";
pub(super) const SET_SCHEDULE_CONFIG_NAME: &str = "SetScheduleConfig";
pub(super) const GET_CLUSTER_VERSION_NAME: &str = "GetClusterVersion";
pub(super) const GET_CLUSTER_NAME: &str = "GetCluster";
pub(super) const GET_CLUSTER_STATUS_NAME: &str = "GetClusterStatus";
pub(super) const GET_STATUS_NAME: &str = "GetStatus";
pub(super) const GET_REPLICATE_CONFIG_NAME: &str = "GetReplicateConfig";
pub(super) const GET_SCHEDULERS_NAME: &str = "GetSchedulers";
pub(super) const GET_SCHEDULER_CONFIG: &str = "GetSchedulerConfig";
pub(super) const CREATE_SCHEDULER_NAME: &str = "CreateScheduler";
pub(super) const CREATE_SCHEDULER_NAME_WITH_INPUT: &str = "CreateSchedulerWithInput";
pub(super) const CANCEL_SCHEDULER_JOB_NAME: &str = "CancelSchedulerJob";
pub(super) const DELETE_SCHEDULER_NAME: &str = "DeleteScheduler";
pub(super) const SET_SCHEDULER_DELAY_NAME: &str = "SetSchedulerDelay";
pub(super) const GET_ALL_PLACEMENT_RULE_BUNDLES_NAME: &str = "GetAllPlacementRuleBundles";
pub(super) const GET_PLACEMENT_RULE_BUNDLE_BY_GROUP_NAME: &str = "GetPlacementRuleBundleByGroup";
pub(super) const GET_PLACEMENT_RULES_BY_GROUP_NAME: &str = "GetPlacementRulesByGroup";
pub(super) const GET_PLACEMENT_RULE_NAME: &str = "GetPlacementRule";
pub(super) const SET_PLACEMENT_RULE_NAME: &str = "SetPlacementRule";
pub(super) const SET_PLACEMENT_RULE_IN_BATCH_NAME: &str = "SetPlacementRuleInBatch";
pub(super) const SET_PLACEMENT_RULE_BUNDLES_NAME: &str = "SetPlacementRuleBundles";
pub(super) const DELETE_PLACEMENT_RULE_NAME: &str = "DeletePlacementRule";
pub(super) const DELETE_PLACEMENT_RULE_BUNDLE_BY_GROUP_NAME: &str =
    "DeletePlacementRuleBundleByGroup";
pub(super) const GET_ALL_PLACEMENT_RULE_GROUPS_NAME: &str = "GetAllPlacementRuleGroups";
pub(super) const GET_PLACEMENT_RULE_GROUP_BY_ID_NAME: &str = "GetPlacementRuleGroupByID";
pub(super) const SET_PLACEMENT_RULE_GROUP_NAME: &str = "SetPlacementRuleGroup";
pub(super) const DELETE_PLACEMENT_RULE_GROUP_BY_ID_NAME: &str = "DeletePlacementRuleGroupByID";
pub(super) const GET_ALL_REGION_LABEL_RULES_NAME: &str = "GetAllRegionLabelRules";
pub(super) const GET_REGION_LABEL_RULES_BY_IDS_NAME: &str = "GetRegionLabelRulesByIDs";
pub(super) const SET_REGION_LABEL_RULE_NAME: &str = "SetRegionLabelRule";
pub(super) const PATCH_REGION_LABEL_RULES_NAME: &str = "PatchRegionLabelRules";
pub(super) const ACCELERATE_SCHEDULE_NAME: &str = "AccelerateSchedule";
pub(super) const ACCELERATE_SCHEDULE_IN_BATCH_NAME: &str = "AccelerateScheduleInBatch";
pub(super) const GET_MIN_RESOLVED_TS_BY_STORES_IDS_NAME: &str = "GetMinResolvedTSByStoresIDs";
pub(super) const GET_MICROSERVICE_MEMBERS_NAME: &str = "GetMicroserviceMembers";
pub(super) const GET_MICROSERVICE_PRIMARY_NAME: &str = "GetMicroservicePrimary";
pub(super) const GET_PD_VERSION_NAME: &str = "GetPDVersion";
pub(super) const RESET_TS_NAME: &str = "ResetTS";
pub(super) const RESET_BASE_ALLOC_ID_NAME: &str = "ResetBaseAllocID";
pub(super) const SET_SNAPSHOT_RECOVERING_MARK_NAME: &str = "SetSnapshotRecoveringMark";
pub(super) const DELETE_SNAPSHOT_RECOVERING_MARK_NAME: &str = "DeleteSnapshotRecoveringMark";
pub(super) const SET_PITR_RESTORE_MODE_MARK_NAME: &str = "SetPitrRestoreModeMark";
pub(super) const DELETE_PITR_RESTORE_MODE_MARK_NAME: &str = "DeletePitrRestoreModeMark";
pub(super) const CREATE_OPERATORS: &str = "CreateOperators";
pub(super) const DELETE_OPERATORS: &str = "DeleteOperators";
pub const UPDATE_KEYSPACE_CONFIG_NAME: &str = "UpdateKeyspaceConfig";
pub const UPDATE_KEYSPACE_GC_MANAGEMENT_TYPE_NAME: &str = "UpdateKeyspaceGCManagementType";
pub const GET_KEYSPACE_META_BY_NAME_NAME: &str = "GetKeyspaceMetaByName";
pub const GET_KEYSPACE_META_BY_ID_NAME: &str = "GetKeyspaceMetaByID";
pub const GET_GC_SAFE_POINT_NAME: &str = "GetGCSafePoint";
pub const DELETE_GC_SAFE_POINT_NAME: &str = "DeleteGCSafePoint";

/// Go `requestInfo`. Go's `res any` decode target is `res`: whether the
/// response body is decoded into the caller's result type.
#[derive(Clone, Default)]
pub(super) struct RequestInfo {
    pub(super) caller_id: String,
    pub(super) name: String,
    pub(super) uri: String,
    pub(super) method: Method,
    pub(super) body: Vec<u8>,
    pub(super) res: bool,
    pub(super) resp_handler: Option<RespHandleFunc>,
    pub(super) bo: Option<Backoffer>,
    pub(super) target_url: String,
}

/// Go `newRequestInfo`.
pub(super) fn new_request_info() -> RequestInfo {
    RequestInfo::default()
}

impl RequestInfo {
    /// Go `WithCallerID`: sets the caller ID of the request.
    pub(super) fn with_caller_id(mut self, caller_id: impl Into<String>) -> Self {
        self.caller_id = caller_id.into();
        self
    }

    /// Go `WithName`: sets the name of the request.
    pub(super) fn with_name(mut self, name: impl Into<String>) -> Self {
        self.name = name.into();
        self
    }

    /// Go `WithURI`: sets the URI of the request.
    pub(super) fn with_uri(mut self, uri: impl Into<String>) -> Self {
        self.uri = uri.into();
        self
    }

    /// Go `WithMethod`: sets the method of the request.
    pub(super) fn with_method(mut self, method: Method) -> Self {
        self.method = method;
        self
    }

    /// Go `WithBody`: sets the body of the request.
    pub(super) fn with_body(mut self, body: Vec<u8>) -> Self {
        self.body = body;
        self
    }

    /// Go `WithResp`: the response body is decoded into the result.
    pub(super) fn with_resp(mut self) -> Self {
        self.res = true;
        self
    }

    /// Go `WithRespHandler`: sets the response handle function of the request.
    pub(super) fn with_resp_handler(mut self, resp_handler: Option<RespHandleFunc>) -> Self {
        self.resp_handler = resp_handler;
        self
    }

    /// Go `WithBackoffer`: sets the backoffer of the request.
    pub(super) fn with_backoffer(mut self, bo: Option<Backoffer>) -> Self {
        self.bo = bo;
        self
    }

    /// Go `WithTargetURL`: sets the target URL of the request.
    pub(super) fn with_target_url(mut self, target_url: impl Into<String>) -> Self {
        self.target_url = target_url.into();
        self
    }

    /// Go `getURL`.
    pub(super) fn get_url(&self, addr: &str) -> String {
        format!("{addr}{}", self.uri)
    }

    /// Go `logFields`, rendered as zap-style `[key=value]` pairs.
    pub(super) fn log_fields(&self) -> String {
        format!(
            "[caller-id={}] [name={}] [uri={}] [method={}] [target-url={}]",
            self.caller_id, self.name, self.uri, self.method, self.target_url
        )
    }
}
