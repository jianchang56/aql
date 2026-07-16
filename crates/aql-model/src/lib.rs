//! Engine-independent canonical data model for AQL.
//!
//! The types in this crate form the stable boundary between source adapters and
//! the SQL engine. Identifiers are installation-scoped and records carry field
//! provenance so callers never need to depend on an Agent's private format.

#![deny(missing_docs)]

use std::collections::BTreeMap;
use std::fmt;

use chrono::{DateTime, Utc};
use hmac::{Hmac, KeyInit, Mac};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

/// Computes a deterministic, domain-separated HMAC for an installation-local value.
///
/// The returned digest does not contain `value` and differs between domains and
/// installation salts. It is suitable for stable redaction and local identity,
/// but is not a password hashing API.
///
/// # Examples
///
/// ```
/// use aql_model::installation_scoped_hmac;
///
/// let digest = installation_scoped_hmac("project", "/private/path", b"installation salt");
/// assert_eq!(digest.len(), 64);
/// assert!(!digest.contains("private"));
/// ```
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
    ($documentation:literal, $name:ident) => {
        #[doc = $documentation]
        #[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(String);

        impl $name {
            /// Wraps a canonical string representation without altering it.
            #[must_use]
            pub fn new(value: impl Into<String>) -> Self {
                Self(value.into())
            }

            /// Returns the canonical string representation.
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

string_id!(
    "An installation-scoped identifier for one physical Agent data source.",
    SourceId
);
string_id!(
    "An identifier as represented by the source Agent's native format.",
    NativeId
);
string_id!(
    "A canonical entity identifier namespaced by Agent and source.",
    EntityId
);
string_id!(
    "An opaque token that identifies a bounded source snapshot.",
    SnapshotToken
);

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
    /// Builds a canonical ID from an Agent ID, source fingerprint, and native ID.
    #[must_use]
    pub fn from_parts(agent_id: &str, source_id: &SourceId, native_id: &NativeId) -> Self {
        let fingerprint = source_id
            .as_str()
            .split_once(':')
            .map_or(source_id.as_str(), |(_, value)| value);
        Self(format!("{agent_id}:{fingerprint}:{native_id}"))
    }
}

/// Classifies the grant required before a field may be read from a source.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AccessClass {
    /// Non-sensitive metadata available without an explicit grant.
    Safe,
    /// A filesystem path or path-derived value.
    Path,
    /// User-authored or Agent-authored message content.
    Content,
    /// Arguments supplied to a tool invocation.
    ToolInput,
    /// Output returned by a tool invocation.
    ToolOutput,
    /// Secret material, which AQL never grants.
    Secret,
}

/// Describes the expected scan and computation cost of a canonical field.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FieldCost {
    /// Cheap metadata that does not require payload parsing.
    Metadata,
    /// Content that requires reading a sensitive payload.
    Content,
    /// A field requiring comparatively expensive source work.
    Heavy,
    /// A field computed from other canonical values.
    Derived,
}

/// Indicates whether canonical identity was proven or only best-effort.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IdentityConfidence {
    /// The adapter proved the native-to-canonical identity mapping.
    Exact,
    /// The source format did not provide enough evidence for exact identity.
    Unknown,
}

/// Describes the consistency of the source view used to produce a record.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SnapshotState {
    /// The record came from a consistent bounded snapshot.
    Consistent,
    /// The source offered only weaker consistency guarantees.
    Weak,
    /// The record came from a known stale but explicitly reported snapshot.
    Stale,
}

/// Records where and how a canonical field was observed or derived.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Provenance {
    /// Physical source that supplied the value.
    pub source_id: SourceId,
    /// Stable adapter-defined source category.
    pub source_kind: String,
    /// Masked, non-sensitive locator within the source.
    pub source_locator: String,
    /// Source format version when one is available.
    pub source_version: Option<String>,
    /// Time at which the value was observed.
    pub observed_at: DateTime<Utc>,
    /// Optional source watermark associated with the observation.
    pub watermark: Option<String>,
    /// Whether the value was derived instead of directly observed.
    pub derived: bool,
}

/// Describes one probed Agent data source without exposing its real data-root path.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SourceManifest {
    /// Installation-scoped source identifier.
    pub source_id: SourceId,
    /// Stable Agent identifier such as `codex` or `claude`.
    pub agent_id: String,
    /// Human-readable source name.
    pub display_name: String,
    /// Masked token representing the data root.
    pub data_root_token: String,
    /// Fingerprint of the detected source format.
    pub format_fingerprint: String,
    /// Adapter capability identifiers available for this source.
    pub capabilities: Vec<String>,
    /// Snapshot token established during probing, when supported.
    pub snapshot: Option<SnapshotToken>,
    /// Non-fatal probe warnings safe to expose to callers.
    pub warnings: Vec<String>,
}

/// Canonical row for the public `sessions` table.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SessionRecord {
    /// Canonical session identifier.
    pub session_id: EntityId,
    /// Native session identifier.
    pub native_id: NativeId,
    /// Source that owns the record.
    pub source_id: SourceId,
    /// Agent that produced the session.
    pub agent_id: String,
    /// Optional session title.
    pub title: Option<String>,
    /// Optional short content preview.
    pub preview: Option<String>,
    /// Optional working directory; requires path access.
    pub cwd: Option<String>,
    /// Optional project label or redacted project identity.
    pub project: Option<String>,
    /// Model associated with the session.
    pub model: Option<String>,
    /// Model provider associated with the session.
    pub provider: Option<String>,
    /// Session creation time.
    pub created_at: Option<DateTime<Utc>>,
    /// Most recent session update time.
    pub updated_at: Option<DateTime<Utc>>,
    /// Adapter-normalized session status.
    pub status: Option<String>,
    /// Whether the source marks the session as archived.
    pub archived: Option<bool>,
    /// Known or derived message count.
    pub message_count: Option<i64>,
    /// Known or derived tool-call count.
    pub tool_call_count: Option<i64>,
    /// Known or derived total token count.
    pub tokens_used: Option<i64>,
    /// Confidence in the canonical session identity.
    pub identity_confidence: IdentityConfidence,
    /// Consistency of the source snapshot.
    pub snapshot_state: SnapshotState,
    /// Per-field provenance keyed by canonical field name.
    pub provenance: BTreeMap<String, Vec<Provenance>>,
    /// Namespaced source-specific values not promoted to the canonical schema.
    pub extensions: BTreeMap<String, JsonValue>,
}

/// Canonical row for the public `messages` table.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct MessageRecord {
    /// Canonical message identifier.
    pub message_id: EntityId,
    /// Session containing the message.
    pub session_id: EntityId,
    /// Source that owns the message.
    pub source_id: SourceId,
    /// Stable order within the session.
    pub sequence: i64,
    /// Normalized author role.
    pub role: String,
    /// Optional source-specific message kind.
    pub kind: Option<String>,
    /// Textual message content; requires content access.
    pub content: Option<String>,
    /// Structured message content; requires content access.
    pub content_json: Option<JsonValue>,
    /// Model associated with this message.
    pub model: Option<String>,
    /// Message creation time.
    pub created_at: Option<DateTime<Utc>>,
    /// Input token count attributed to the message.
    pub input_tokens: Option<i64>,
    /// Output token count attributed to the message.
    pub output_tokens: Option<i64>,
    /// Cached token count attributed to the message.
    pub cached_tokens: Option<i64>,
    /// Whether the source marks the message as an error.
    pub is_error: Option<bool>,
    /// Per-field provenance keyed by canonical field name.
    pub provenance: BTreeMap<String, Vec<Provenance>>,
    /// Namespaced source-specific extension values.
    pub extensions: BTreeMap<String, JsonValue>,
}

/// Canonical row for the public `tool_calls` table.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ToolCallRecord {
    /// Canonical tool-call identifier.
    pub tool_call_id: EntityId,
    /// Session containing the invocation.
    pub session_id: EntityId,
    /// Message that initiated the invocation, when known.
    pub message_id: Option<EntityId>,
    /// Source that owns the invocation.
    pub source_id: SourceId,
    /// Stable order within the session.
    pub sequence: i64,
    /// Tool name reported by the source.
    pub tool_name: String,
    /// Optional tool namespace or server name.
    pub namespace: Option<String>,
    /// Structured tool input; requires tool-input access.
    pub arguments: Option<JsonValue>,
    /// Tool output; requires tool-output access.
    pub output: Option<String>,
    /// Normalized invocation status.
    pub status: Option<String>,
    /// Invocation start time.
    pub started_at: Option<DateTime<Utc>>,
    /// Invocation end time.
    pub ended_at: Option<DateTime<Utc>>,
    /// Duration in milliseconds when known.
    pub duration_ms: Option<i64>,
    /// Process exit code when the tool exposes one.
    pub exit_code: Option<i64>,
    /// Per-field provenance keyed by canonical field name.
    pub provenance: BTreeMap<String, Vec<Provenance>>,
    /// Namespaced source-specific extension values.
    pub extensions: BTreeMap<String, JsonValue>,
}

/// Canonical row for the public `usage` table.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct UsageRecord {
    /// Canonical usage-row identifier.
    pub usage_id: EntityId,
    /// Source that supplied the usage data.
    pub source_id: SourceId,
    /// Agent associated with the usage.
    pub agent_id: String,
    /// Session associated with the usage, when applicable.
    pub session_id: Option<EntityId>,
    /// Model associated with the usage.
    pub model: Option<String>,
    /// Model provider associated with the usage.
    pub provider: Option<String>,
    /// Start of the aggregation bucket.
    pub bucket_start: Option<DateTime<Utc>>,
    /// Input tokens in the bucket.
    pub input_tokens: Option<i64>,
    /// Output tokens in the bucket.
    pub output_tokens: Option<i64>,
    /// Cached tokens in the bucket.
    pub cached_tokens: Option<i64>,
    /// Total tokens in the bucket, when explicitly available or safely derived.
    pub total_tokens: Option<i64>,
    /// Messages counted in the bucket.
    pub message_count: i64,
    /// Tool calls counted in the bucket.
    pub tool_call_count: i64,
    /// Errors counted in the bucket.
    pub error_count: i64,
    /// Per-field provenance keyed by canonical field name.
    pub provenance: BTreeMap<String, Vec<Provenance>>,
    /// Namespaced source-specific extension values.
    pub extensions: BTreeMap<String, JsonValue>,
}

/// Canonical row for the public `session_edges` table.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SessionEdgeRecord {
    /// Canonical edge identifier.
    pub edge_id: EntityId,
    /// Source that supplied the relationship.
    pub source_id: SourceId,
    /// Parent session in the relationship.
    pub parent_session_id: EntityId,
    /// Child session in the relationship.
    pub child_session_id: EntityId,
    /// Adapter-normalized relationship kind.
    pub edge_kind: String,
    /// Relationship creation time, when known.
    pub created_at: Option<DateTime<Utc>>,
    /// Native relationship identifier, when present.
    pub native_edge_id: Option<NativeId>,
    /// Per-field provenance keyed by canonical field name.
    pub provenance: BTreeMap<String, Vec<Provenance>>,
    /// Namespaced source-specific extension values.
    pub extensions: BTreeMap<String, JsonValue>,
}

/// Canonical row for the public `artifacts` table.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ArtifactRecord {
    /// Canonical artifact identifier.
    pub artifact_id: EntityId,
    /// Source that owns the artifact.
    pub source_id: SourceId,
    /// Session associated with the artifact.
    pub session_id: EntityId,
    /// Tool call that produced the artifact, when known.
    pub tool_call_id: Option<EntityId>,
    /// Adapter-normalized artifact kind.
    pub kind: String,
    /// Display name of the artifact.
    pub name: Option<String>,
    /// Artifact path; requires path access.
    pub path: Option<String>,
    /// Media type reported or inferred by the adapter.
    pub media_type: Option<String>,
    /// Artifact size in bytes.
    pub size_bytes: Option<i64>,
    /// Artifact creation time.
    pub created_at: Option<DateTime<Utc>>,
    /// Textual artifact content; requires content access.
    pub content: Option<String>,
    /// Structured artifact content; requires content access.
    pub content_json: Option<JsonValue>,
    /// Per-field provenance keyed by canonical field name.
    pub provenance: BTreeMap<String, Vec<Provenance>>,
    /// Namespaced source-specific extension values.
    pub extensions: BTreeMap<String, JsonValue>,
}

/// A tagged union over every canonical record emitted by adapters.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "record_type", content = "record", rename_all = "snake_case")]
pub enum CanonicalRecord {
    /// A session row.
    Session(SessionRecord),
    /// A message row.
    Message(MessageRecord),
    /// A tool-call row.
    ToolCall(ToolCallRecord),
    /// A usage row.
    Usage(UsageRecord),
    /// A parent-child session edge.
    SessionEdge(SessionEdgeRecord),
    /// An artifact row.
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
    fn source_ids_do_not_collide_across_data_roots() {
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
