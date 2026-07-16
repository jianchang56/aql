//! Read-only adapter contract for agent data sources.
//!
//! Adapters probe a bounded source, declare its canonical schema, authorize a
//! projection, and return a lazy record stream under one shared budget,
//! deadline, cancellation token, and snapshot contract.

#![deny(missing_docs)]

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use aql_model::{AccessClass, CanonicalRecord, SnapshotToken, SourceManifest};
use thiserror::Error;

pub use aql_model as model;

/// Input to a bounded source probe.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProbeRequest {
    /// Absolute candidate root selected by the caller's fixed discovery policy.
    pub data_root: String,
}

/// Manifests produced by probing one candidate root.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProbeResult {
    /// Compatible sources found at the candidate root.
    pub manifests: Vec<SourceManifest>,
}

/// Canonical Agent-data tables that an adapter may scan.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TableName {
    /// Canonical sessions.
    Sessions,
    /// Canonical messages.
    Messages,
    /// Canonical tool invocations.
    ToolCalls,
    /// Canonical usage aggregates.
    Usage,
    /// Canonical parent-child session relationships.
    SessionEdges,
    /// Canonical artifacts.
    Artifacts,
}

/// Name of a canonical projected column.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ColumnName(String);

impl ColumnName {
    /// Creates a column name from its canonical spelling.
    #[must_use]
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// Returns the canonical column spelling.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Scalar literal accepted by conservative predicate pushdown.
#[derive(Clone, Debug, PartialEq)]
pub enum Literal {
    /// SQL `NULL`.
    Null,
    /// Boolean literal.
    Bool(bool),
    /// Signed integer literal.
    Integer(i64),
    /// UTF-8 text literal.
    Text(String),
}

/// Predicate shape offered to an adapter for optional conservative pushdown.
#[derive(Clone, Debug, PartialEq)]
pub enum Predicate {
    /// Column equality.
    Eq(ColumnName, Literal),
    /// Membership in a literal set.
    In(ColumnName, Vec<Literal>),
    /// Inclusive lower and upper bounds, when present.
    Range {
        /// Column being constrained.
        column: ColumnName,
        /// Optional inclusive lower bound.
        lower: Option<Literal>,
        /// Optional inclusive upper bound.
        upper: Option<Literal>,
    },
    /// SQL `IS NULL`.
    IsNull(ColumnName),
    /// Conjunction of predicates.
    And(Vec<Predicate>),
    /// A planner expression that the adapter must not attempt to interpret.
    Unsupported(String),
}

/// Requested ordering that an adapter may optionally preserve or push down.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OrderingHint {
    /// Canonical ordering column.
    pub column: ColumnName,
    /// Whether descending order was requested.
    pub descending: bool,
}

/// Session-scoped grants for sensitive canonical fields.
///
/// Secret access is intentionally absent and always denied.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct AccessGrant {
    /// Allows path-class fields.
    pub path: bool,
    /// Allows content-class fields.
    pub content: bool,
    /// Allows tool-input fields.
    pub tool_input: bool,
    /// Allows tool-output fields.
    pub tool_output: bool,
}

impl AccessGrant {
    /// Returns whether this grant permits the requested access class.
    #[must_use]
    pub fn allows(self, access: AccessClass) -> bool {
        match access {
            AccessClass::Safe => true,
            AccessClass::Path => self.path,
            AccessClass::Content => self.content,
            AccessClass::ToolInput => self.tool_input,
            AccessClass::ToolOutput => self.tool_output,
            AccessClass::Secret => false,
        }
    }
}

/// Shared per-query resource limits and atomic usage counters.
///
/// Clones share [`BudgetUsage`], so federated adapters consume one budget
/// rather than receiving a separate allowance per source.
#[derive(Clone, Debug)]
pub struct ResourceBudget {
    /// Maximum canonical source records consumed by the query.
    pub max_records: u64,
    /// Maximum source bytes read by the query.
    pub max_bytes_read: u64,
    /// Maximum bytes published by renderers.
    pub max_output_bytes: u64,
    /// Maximum size of one sensitive projected value.
    pub max_single_value_bytes: u64,
    /// Optional absolute deadline shared by all sources.
    pub deadline: Option<Instant>,
    /// Shared atomic usage counters.
    pub usage: BudgetUsage,
}

/// Atomic resource counters shared by cloned budgets.
#[derive(Clone, Debug, Default)]
pub struct BudgetUsage {
    records: Arc<AtomicU64>,
    bytes_read: Arc<AtomicU64>,
    output_bytes: Arc<AtomicU64>,
}

impl Default for ResourceBudget {
    fn default() -> Self {
        Self {
            max_records: 100_000,
            max_bytes_read: 256 * 1024 * 1024,
            max_output_bytes: 64 * 1024 * 1024,
            max_single_value_bytes: 16 * 1024 * 1024,
            deadline: None,
            usage: BudgetUsage::default(),
        }
    }
}

impl ResourceBudget {
    /// Atomically charges records and returns the new total.
    ///
    /// Returns [`AdapterError::BudgetExceeded`] after the shared total exceeds
    /// [`ResourceBudget::max_records`].
    pub fn charge_records(&self, delta: u64) -> Result<u64, AdapterError> {
        charge(&self.usage.records, delta, self.max_records, "records")
    }

    /// Atomically charges source bytes and returns the new total.
    pub fn charge_bytes_read(&self, delta: u64) -> Result<u64, AdapterError> {
        charge(
            &self.usage.bytes_read,
            delta,
            self.max_bytes_read,
            "bytes_read",
        )
    }

    /// Atomically charges output bytes and returns the new total.
    pub fn charge_output_bytes(&self, delta: u64) -> Result<u64, AdapterError> {
        charge(
            &self.usage.output_bytes,
            delta,
            self.max_output_bytes,
            "output_bytes",
        )
    }

    /// Returns the records charged across every clone.
    #[must_use]
    pub fn records_used(&self) -> u64 {
        self.usage.records.load(Ordering::Acquire)
    }

    /// Returns the source bytes charged across every clone.
    #[must_use]
    pub fn bytes_read_used(&self) -> u64 {
        self.usage.bytes_read.load(Ordering::Acquire)
    }

    /// Returns the output bytes charged across every clone.
    #[must_use]
    pub fn output_bytes_used(&self) -> u64 {
        self.usage.output_bytes.load(Ordering::Acquire)
    }
}

fn charge(
    counter: &AtomicU64,
    delta: u64,
    maximum: u64,
    resource: &str,
) -> Result<u64, AdapterError> {
    let previous = counter.fetch_add(delta, Ordering::AcqRel);
    let actual = previous.saturating_add(delta);
    if actual > maximum {
        Err(AdapterError::BudgetExceeded {
            resource: resource.to_string(),
            actual,
        })
    } else {
        Ok(actual)
    }
}

/// Thread-safe cooperative cancellation shared by all query participants.
#[derive(Clone, Debug, Default)]
pub struct CancellationToken(Arc<AtomicBool>);

impl CancellationToken {
    /// Permanently marks the query as cancelled.
    pub fn cancel(&self) {
        self.0.store(true, Ordering::Release);
    }

    /// Returns whether cancellation has been requested.
    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.0.load(Ordering::Acquire)
    }
}

/// Accuracy reported for one attempted pushdown.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PushdownState {
    /// The adapter applied the expression exactly.
    Exact,
    /// The adapter applied only a safe approximation; the engine must recheck it.
    Inexact,
    /// The adapter did not apply the expression.
    Unsupported,
}

/// Per-expression report describing which planner hints were applied.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PushdownReport {
    /// Result for each requested predicate in request order.
    pub predicates: Vec<PushdownState>,
    /// Result for the requested limit, when present.
    pub limit: Option<PushdownState>,
    /// Result for each requested ordering term in request order.
    pub ordering: Vec<PushdownState>,
}

/// Consistency guarantee provided by a source snapshot.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SnapshotStrength {
    /// No snapshot guarantee is available.
    None,
    /// The source provides a bounded but not fully transactional view.
    Weak,
    /// The source provides a stable transactional or identity-checked view.
    Strong,
}

/// Snapshot outcome associated with a completed scan.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SnapshotReport {
    /// Opaque snapshot token, when available.
    pub token: Option<SnapshotToken>,
    /// Consistency strength of the scan.
    pub strength: SnapshotStrength,
    /// Whether the adapter knows the snapshot is stale.
    pub stale: bool,
}

/// Stable category for a non-fatal adapter warning.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AdapterWarningKind {
    /// The source contained an unrecognized event type.
    UnknownEvent,
    /// The source contained an unrecognized field.
    UnknownField,
    /// A final partial record was safely ignored.
    TruncatedRecord,
    /// Multiple sources disagreed about one canonical field.
    FieldConflict,
    /// The adapter used a known stale snapshot.
    StaleSnapshot,
    /// The source could not provide all advertised data.
    IncompleteCapability,
}

/// Structured, path-safe warning emitted during a scan.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdapterWarning {
    /// Stable warning category.
    pub kind: AdapterWarningKind,
    /// Masked adapter-defined source category.
    pub source_kind: String,
    /// Stable processing stage without private payload details.
    pub stage: String,
}

/// Access metadata for one canonical column.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ColumnCapability {
    /// Canonical column name.
    pub name: ColumnName,
    /// Grant required before reading the source value.
    pub access: AccessClass,
}

/// Tables, columns, and snapshot strength supported by an adapter source.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Capabilities {
    /// Canonical tables available from the source.
    pub tables: Vec<TableName>,
    /// Canonical columns available from the source.
    pub columns: Vec<ColumnCapability>,
    /// Strongest snapshot guarantee the source can provide.
    pub snapshot_strength: SnapshotStrength,
}

/// Canonical column schema exposed by an adapter source.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdapterSchema {
    /// Available columns and their access classes.
    pub columns: Vec<ColumnCapability>,
}

/// Fully authorized request to scan one manifest and canonical table.
#[derive(Clone, Debug)]
pub struct ScanRequest {
    /// Manifest previously returned by this adapter's probe.
    pub source: SourceManifest,
    /// Canonical table to scan.
    pub table: TableName,
    /// Authorized canonical columns requested by the engine.
    pub projection: Vec<ColumnName>,
    /// Predicates offered for optional pushdown.
    pub predicates: Vec<Predicate>,
    /// Optional row limit offered for pushdown.
    pub limit: Option<u64>,
    /// Optional ordering offered for pushdown.
    pub order_hint: Vec<OrderingHint>,
    /// Session access grants already approved by the caller.
    pub access: AccessGrant,
    /// Shared query budget.
    pub budget: ResourceBudget,
    /// Shared cancellation token.
    pub cancellation: CancellationToken,
    /// Snapshot token established during probing, when applicable.
    pub snapshot: Option<SnapshotToken>,
}

/// Lazy, sendable stream of canonical records or a terminal adapter error.
pub type RecordStream = Box<dyn Iterator<Item = Result<CanonicalRecord, AdapterError>> + Send>;

/// Thread-safe collector for non-fatal warnings produced while streaming.
#[derive(Clone, Debug, Default)]
pub struct ScanDiagnostics(Arc<Mutex<Vec<AdapterWarning>>>);

impl ScanDiagnostics {
    /// Appends a path-safe warning to the scan diagnostics.
    pub fn push(&self, warning: AdapterWarning) -> Result<(), AdapterError> {
        self.0
            .lock()
            .map_err(|_| AdapterError::Internal {
                stage: "scan_diagnostics".to_string(),
            })?
            .push(warning);
        Ok(())
    }

    /// Returns a stable snapshot of warnings collected so far.
    pub fn snapshot(&self) -> Result<Vec<AdapterWarning>, AdapterError> {
        self.0
            .lock()
            .map(|warnings| warnings.clone())
            .map_err(|_| AdapterError::Internal {
                stage: "scan_diagnostics".to_string(),
            })
    }
}

/// Lazy scan result plus pushdown, diagnostics, and snapshot metadata.
pub struct ScanResult {
    /// Canonical records. Callers must consume to EOF before publishing results.
    pub records: RecordStream,
    /// Accuracy of every attempted pushdown.
    pub pushdown: PushdownReport,
    /// Warnings collected before and during stream consumption.
    pub diagnostics: ScanDiagnostics,
    /// Snapshot guarantee associated with the stream.
    pub snapshot: SnapshotReport,
}

/// Stable, sanitized failure categories returned by adapters.
#[derive(Debug, Error, Eq, PartialEq)]
pub enum AdapterError {
    /// The fixed candidate source does not exist.
    #[error("source not found at stage {stage}")]
    NotFound {
        /// Stable processing stage at which absence was detected.
        stage: String,
    },
    /// The source cannot be read under current filesystem permissions.
    #[error("source permission denied at stage {stage}")]
    PermissionDenied {
        /// Stable processing stage at which access was denied.
        stage: String,
    },
    /// The source format is unknown or incompatible.
    #[error("unsupported source format at stage {stage}")]
    UnsupportedFormat {
        /// Stable validation stage that rejected the format.
        stage: String,
    },
    /// The source violates its pinned structural contract.
    #[error("corrupt source at stage {stage}")]
    CorruptSource {
        /// Stable validation stage that detected corruption.
        stage: String,
    },
    /// A projected column lacks the required session grant.
    #[error("access denied for column {column}")]
    AccessDenied {
        /// Canonical column that requires an additional grant.
        column: String,
    },
    /// A shared resource limit or deadline was exceeded.
    #[error("resource budget exceeded: {resource}")]
    BudgetExceeded {
        /// Stable resource name such as `records`, `bytes_read`, or `deadline`.
        resource: String,
        /// Observed usage when the limit was detected.
        actual: u64,
    },
    /// Cooperative cancellation was observed.
    #[error("scan cancelled")]
    Cancelled,
    /// A required source snapshot could not be established or retained.
    #[error("snapshot unavailable")]
    SnapshotUnavailable,
    /// An invariant failed without exposing private internal details.
    #[error("internal adapter error at stage {stage}")]
    Internal {
        /// Stable internal stage without private source details.
        stage: String,
    },
}

/// Read-only contract implemented by every Agent source adapter.
pub trait AgentAdapter: Send + Sync {
    /// Returns the stable adapter identifier.
    fn id(&self) -> &'static str;
    /// Probes one fixed candidate root without recursive or process-based discovery.
    fn probe(&self, request: &ProbeRequest) -> Result<ProbeResult, AdapterError>;
    /// Returns capabilities for a manifest previously produced by this adapter.
    fn capabilities(&self, manifest: &SourceManifest) -> Capabilities;
    /// Returns the canonical schema for a manifest.
    fn schema(&self, manifest: &SourceManifest) -> AdapterSchema;
    /// Starts a lazy, bounded scan after projection authorization.
    fn scan(&self, request: ScanRequest) -> Result<ScanResult, AdapterError>;
}

/// Stable source-file category exposed to test and audit observers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SourceKind {
    /// Primary Agent state database.
    StateDatabase,
    /// Session metadata index.
    SessionIndex,
    /// Append-only rollout or transcript stream.
    Rollout,
}

/// Observer used to prove that authorization and laziness precede source reads.
pub trait FileAccessObserver: Send + Sync {
    /// Reports that a source category was opened.
    fn opened(&self, source_kind: SourceKind);
    /// Reports bounded bytes read from a source category.
    fn bytes_read(&self, source_kind: SourceKind, count: u64);
}

/// Rejects projected columns whose access class is not granted.
///
/// Adapters call this before opening sensitive source payloads.
pub fn validate_projection_access(
    projection: &[ColumnName],
    schema: &AdapterSchema,
    access: AccessGrant,
) -> Result<(), AdapterError> {
    for projected in projection {
        if let Some(column) = schema.columns.iter().find(|item| item.name == *projected)
            && !access.allows(column.access)
        {
            return Err(AdapterError::AccessDenied {
                column: projected.as_str().to_string(),
            });
        }
    }
    Ok(())
}

/// Checks cancellation, deadline, record count, and byte count in priority order.
///
/// Cancellation deliberately takes precedence over budget errors so all
/// federated participants converge on the same terminal state.
pub fn check_scan_state(
    cancellation: &CancellationToken,
    budget: &ResourceBudget,
    records: u64,
    bytes_read: u64,
) -> Result<(), AdapterError> {
    if cancellation.is_cancelled() {
        return Err(AdapterError::Cancelled);
    }
    if budget
        .deadline
        .is_some_and(|deadline| Instant::now() >= deadline)
    {
        return Err(AdapterError::BudgetExceeded {
            resource: "deadline".to_string(),
            actual: records,
        });
    }
    if records > budget.max_records {
        return Err(AdapterError::BudgetExceeded {
            resource: "records".to_string(),
            actual: records,
        });
    }
    if bytes_read > budget.max_bytes_read {
        return Err(AdapterError::BudgetExceeded {
            resource: "bytes_read".to_string(),
            actual: bytes_read,
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn schema() -> AdapterSchema {
        AdapterSchema {
            columns: vec![
                ColumnCapability {
                    name: ColumnName::new("session_id"),
                    access: AccessClass::Safe,
                },
                ColumnCapability {
                    name: ColumnName::new("cwd"),
                    access: AccessClass::Path,
                },
                ColumnCapability {
                    name: ColumnName::new("content"),
                    access: AccessClass::Content,
                },
            ],
        }
    }

    #[test]
    fn projection_access_is_checked_before_scanning() {
        let error = validate_projection_access(
            &[ColumnName::new("content")],
            &schema(),
            AccessGrant::default(),
        )
        .expect_err("content must require a grant");
        assert_eq!(
            error,
            AdapterError::AccessDenied {
                column: "content".to_string()
            }
        );
    }

    #[test]
    fn path_and_content_grants_are_independent() {
        validate_projection_access(
            &[ColumnName::new("cwd")],
            &schema(),
            AccessGrant {
                path: true,
                ..AccessGrant::default()
            },
        )
        .expect("path grant must allow cwd");
    }

    #[test]
    fn cancellation_precedes_budget_errors() {
        let token = CancellationToken::default();
        token.cancel();
        let error = check_scan_state(&token, &ResourceBudget::default(), u64::MAX, u64::MAX)
            .expect_err("cancelled scans must fail");
        assert_eq!(error, AdapterError::Cancelled);
    }

    #[test]
    fn record_budget_is_enforced() {
        let error = check_scan_state(
            &CancellationToken::default(),
            &ResourceBudget {
                max_records: 1,
                ..ResourceBudget::default()
            },
            2,
            0,
        )
        .expect_err("record budget must be enforced");
        assert_eq!(
            error,
            AdapterError::BudgetExceeded {
                resource: "records".to_string(),
                actual: 2
            }
        );
    }

    #[test]
    fn cloned_budgets_share_query_usage() {
        let budget = ResourceBudget {
            max_records: 2,
            ..ResourceBudget::default()
        };
        let second_scan = budget.clone();
        budget
            .charge_records(1)
            .expect("first scan may consume one record");
        second_scan
            .charge_records(1)
            .expect("second scan shares the remaining record");
        let error = budget
            .charge_records(1)
            .expect_err("third record must exceed the shared query budget");
        assert_eq!(
            error,
            AdapterError::BudgetExceeded {
                resource: "records".to_string(),
                actual: 3,
            }
        );
    }
}
