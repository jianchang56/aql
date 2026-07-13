//! Engine-independent canonical data model for AQL.

use std::collections::BTreeMap;
use std::fmt;

use chrono::{DateTime, Utc};
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

#[must_use]
pub fn installation_scoped_hmac(domain: &str, value: &str, installation_salt: &[u8]) -> String {
    let mut mac =
        HmacSha256::new_from_slice(installation_salt).expect("HMAC accepts keys of every length");
    mac.update(b"aql-domain-v1\0");
    mac.update(domain.as_bytes());
    mac.update(b"\0");
    mac.update(value.as_bytes());
    mac.finalize()
        .into_bytes()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

macro_rules! string_id {
    ($name:ident) => {
        #[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(String);

        impl $name {
            #[must_use]
            pub fn new(value: impl Into<String>) -> Self {
                Self(value.into())
            }

            #[must_use]
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.0.fmt(formatter)
            }
        }
    };
}

string_id!(SourceId);
string_id!(NativeId);
string_id!(EntityId);
string_id!(SnapshotToken);

impl SourceId {
    /// Creates an installation-scoped identifier without exposing the data-root path.
    ///
    /// # Panics
    ///
    /// Panics only if the HMAC implementation rejects its key, which cannot happen for HMAC.
    #[must_use]
    pub fn for_data_root(agent_id: &str, normalized_root: &str, installation_salt: &[u8]) -> Self {
        let mut mac = HmacSha256::new_from_slice(installation_salt)
            .expect("HMAC accepts keys of every length");
        mac.update(normalized_root.as_bytes());
        let digest = mac.finalize().into_bytes();
        let fingerprint = digest[..12]
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        Self(format!("{agent_id}:{fingerprint}"))
    }
}

impl EntityId {
    #[must_use]
    pub fn from_parts(agent_id: &str, source_id: &SourceId, native_id: &NativeId) -> Self {
        let fingerprint = source_id
            .as_str()
            .split_once(':')
            .map_or(source_id.as_str(), |(_, value)| value);
        Self(format!("{agent_id}:{fingerprint}:{native_id}"))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AccessClass {
    Safe,
    Path,
    Content,
    ToolInput,
    ToolOutput,
    Secret,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FieldCost {
    Metadata,
    Content,
    Heavy,
    Derived,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IdentityConfidence {
    Exact,
    Unknown,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SnapshotState {
    Consistent,
    Weak,
    Stale,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Provenance {
    pub source_id: SourceId,
    pub source_kind: String,
    pub source_locator: String,
    pub source_version: Option<String>,
    pub observed_at: DateTime<Utc>,
    pub watermark: Option<String>,
    pub derived: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SourceManifest {
    pub source_id: SourceId,
    pub agent_id: String,
    pub display_name: String,
    pub data_root_token: String,
    pub format_fingerprint: String,
    pub capabilities: Vec<String>,
    pub snapshot: Option<SnapshotToken>,
    pub warnings: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SessionRecord {
    pub session_id: EntityId,
    pub native_id: NativeId,
    pub source_id: SourceId,
    pub agent_id: String,
    pub title: Option<String>,
    pub preview: Option<String>,
    pub cwd: Option<String>,
    pub project: Option<String>,
    pub model: Option<String>,
    pub provider: Option<String>,
    pub created_at: Option<DateTime<Utc>>,
    pub updated_at: Option<DateTime<Utc>>,
    pub status: Option<String>,
    pub archived: Option<bool>,
    pub message_count: Option<i64>,
    pub tool_call_count: Option<i64>,
    pub tokens_used: Option<i64>,
    pub identity_confidence: IdentityConfidence,
    pub snapshot_state: SnapshotState,
    pub provenance: BTreeMap<String, Vec<Provenance>>,
    pub extensions: BTreeMap<String, JsonValue>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct MessageRecord {
    pub message_id: EntityId,
    pub session_id: EntityId,
    pub source_id: SourceId,
    pub sequence: i64,
    pub role: String,
    pub kind: Option<String>,
    pub content: Option<String>,
    pub content_json: Option<JsonValue>,
    pub model: Option<String>,
    pub created_at: Option<DateTime<Utc>>,
    pub input_tokens: Option<i64>,
    pub output_tokens: Option<i64>,
    pub cached_tokens: Option<i64>,
    pub is_error: Option<bool>,
    pub provenance: BTreeMap<String, Vec<Provenance>>,
    pub extensions: BTreeMap<String, JsonValue>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ToolCallRecord {
    pub tool_call_id: EntityId,
    pub session_id: EntityId,
    pub message_id: Option<EntityId>,
    pub source_id: SourceId,
    pub sequence: i64,
    pub tool_name: String,
    pub namespace: Option<String>,
    pub arguments: Option<JsonValue>,
    pub output: Option<String>,
    pub status: Option<String>,
    pub started_at: Option<DateTime<Utc>>,
    pub ended_at: Option<DateTime<Utc>>,
    pub duration_ms: Option<i64>,
    pub exit_code: Option<i64>,
    pub provenance: BTreeMap<String, Vec<Provenance>>,
    pub extensions: BTreeMap<String, JsonValue>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct UsageRecord {
    pub usage_id: EntityId,
    pub source_id: SourceId,
    pub agent_id: String,
    pub session_id: Option<EntityId>,
    pub model: Option<String>,
    pub provider: Option<String>,
    pub bucket_start: Option<DateTime<Utc>>,
    pub input_tokens: Option<i64>,
    pub output_tokens: Option<i64>,
    pub cached_tokens: Option<i64>,
    pub total_tokens: Option<i64>,
    pub message_count: i64,
    pub tool_call_count: i64,
    pub error_count: i64,
    pub provenance: BTreeMap<String, Vec<Provenance>>,
    pub extensions: BTreeMap<String, JsonValue>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SessionEdgeRecord {
    pub edge_id: EntityId,
    pub source_id: SourceId,
    pub parent_session_id: EntityId,
    pub child_session_id: EntityId,
    pub edge_kind: String,
    pub created_at: Option<DateTime<Utc>>,
    pub native_edge_id: Option<NativeId>,
    pub provenance: BTreeMap<String, Vec<Provenance>>,
    pub extensions: BTreeMap<String, JsonValue>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ArtifactRecord {
    pub artifact_id: EntityId,
    pub source_id: SourceId,
    pub session_id: EntityId,
    pub tool_call_id: Option<EntityId>,
    pub kind: String,
    pub name: Option<String>,
    pub path: Option<String>,
    pub media_type: Option<String>,
    pub size_bytes: Option<i64>,
    pub created_at: Option<DateTime<Utc>>,
    pub content: Option<String>,
    pub content_json: Option<JsonValue>,
    pub provenance: BTreeMap<String, Vec<Provenance>>,
    pub extensions: BTreeMap<String, JsonValue>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "record_type", content = "record", rename_all = "snake_case")]
pub enum CanonicalRecord {
    Session(SessionRecord),
    Message(MessageRecord),
    ToolCall(ToolCallRecord),
    Usage(UsageRecord),
    SessionEdge(SessionEdgeRecord),
    Artifact(ArtifactRecord),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn source_ids_are_stable_for_the_same_installation() {
        let first = SourceId::for_data_root("codex", "/synthetic/root", b"fixture-salt");
        let second = SourceId::for_data_root("codex", "/synthetic/root", b"fixture-salt");
        assert_eq!(first, second);
        assert!(!first.as_str().contains("synthetic"));
    }

    #[test]
    fn source_ids_do_not_collide_across_profiles() {
        let first = SourceId::for_data_root("codex", "/synthetic/a", b"fixture-salt");
        let second = SourceId::for_data_root("codex", "/synthetic/b", b"fixture-salt");
        assert_ne!(first, second);
    }

    #[test]
    fn entity_id_includes_the_source_fingerprint() {
        let source = SourceId::for_data_root("codex", "/synthetic/a", b"fixture-salt");
        let native = NativeId::new("same-native-id");
        let entity = EntityId::from_parts("codex", &source, &native);
        assert!(entity.as_str().starts_with("codex:"));
        assert!(entity.as_str().ends_with(":same-native-id"));
    }

    #[test]
    fn installation_hmac_is_deterministic_and_domain_separated() {
        let first = installation_scoped_hmac("redact", "synthetic", b"fixture-salt");
        let second = installation_scoped_hmac("redact", "synthetic", b"fixture-salt");
        let other_domain = installation_scoped_hmac("project", "synthetic", b"fixture-salt");
        assert_eq!(first, second);
        assert_ne!(first, other_domain);
        assert!(!first.contains("synthetic"));
    }

    #[test]
    fn canonical_record_round_trips_unknown_extensions() {
        let source = SourceId::new("codex:fixture");
        let session = SessionRecord {
            session_id: EntityId::new("codex:fixture:session-1"),
            native_id: NativeId::new("session-1"),
            source_id: source,
            agent_id: "codex".to_string(),
            title: None,
            preview: None,
            cwd: None,
            project: None,
            model: Some("example-model".to_string()),
            provider: None,
            created_at: None,
            updated_at: None,
            status: None,
            archived: Some(false),
            message_count: None,
            tool_call_count: None,
            tokens_used: None,
            identity_confidence: IdentityConfidence::Exact,
            snapshot_state: SnapshotState::Consistent,
            provenance: BTreeMap::new(),
            extensions: BTreeMap::from([(
                "codex.future_field".to_string(),
                serde_json::json!({"synthetic": true}),
            )]),
        };
        let encoded = serde_json::to_string(&CanonicalRecord::Session(session.clone()))
            .expect("record must serialize");
        let decoded: CanonicalRecord =
            serde_json::from_str(&encoded).expect("record must deserialize");
        assert_eq!(decoded, CanonicalRecord::Session(session));
    }
}
