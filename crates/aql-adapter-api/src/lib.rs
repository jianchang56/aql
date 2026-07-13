//! Read-only adapter contract for agent data sources.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use aql_model::{AccessClass, CanonicalRecord, SnapshotToken, SourceManifest};
use thiserror::Error;

pub use aql_model as model;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProbeRequest {
    pub data_root: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProbeResult {
    pub manifests: Vec<SourceManifest>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TableName {
    Sessions,
    Messages,
    ToolCalls,
    Usage,
    SessionEdges,
    Artifacts,
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ColumnName(String);

impl ColumnName {
    #[must_use]
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum Literal {
    Null,
    Bool(bool),
    Integer(i64),
    Text(String),
}

#[derive(Clone, Debug, PartialEq)]
pub enum Predicate {
    Eq(ColumnName, Literal),
    In(ColumnName, Vec<Literal>),
    Range {
        column: ColumnName,
        lower: Option<Literal>,
        upper: Option<Literal>,
    },
    IsNull(ColumnName),
    And(Vec<Predicate>),
    Unsupported(String),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OrderingHint {
    pub column: ColumnName,
    pub descending: bool,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct AccessGrant {
    pub path: bool,
    pub content: bool,
    pub tool_input: bool,
    pub tool_output: bool,
}

impl AccessGrant {
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

#[derive(Clone, Debug)]
pub struct ResourceBudget {
    pub max_records: u64,
    pub max_bytes_read: u64,
    pub max_output_bytes: u64,
    pub max_single_value_bytes: u64,
    pub deadline: Option<Instant>,
    pub usage: BudgetUsage,
}

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
    pub fn charge_records(&self, delta: u64) -> Result<u64, AdapterError> {
        charge(&self.usage.records, delta, self.max_records, "records")
    }

    pub fn charge_bytes_read(&self, delta: u64) -> Result<u64, AdapterError> {
        charge(
            &self.usage.bytes_read,
            delta,
            self.max_bytes_read,
            "bytes_read",
        )
    }

    pub fn charge_output_bytes(&self, delta: u64) -> Result<u64, AdapterError> {
        charge(
            &self.usage.output_bytes,
            delta,
            self.max_output_bytes,
            "output_bytes",
        )
    }

    #[must_use]
    pub fn records_used(&self) -> u64 {
        self.usage.records.load(Ordering::Acquire)
    }

    #[must_use]
    pub fn bytes_read_used(&self) -> u64 {
        self.usage.bytes_read.load(Ordering::Acquire)
    }

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

#[derive(Clone, Debug, Default)]
pub struct CancellationToken(Arc<AtomicBool>);

impl CancellationToken {
    pub fn cancel(&self) {
        self.0.store(true, Ordering::Release);
    }

    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.0.load(Ordering::Acquire)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PushdownState {
    Exact,
    Inexact,
    Unsupported,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PushdownReport {
    pub predicates: Vec<PushdownState>,
    pub limit: Option<PushdownState>,
    pub ordering: Vec<PushdownState>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SnapshotStrength {
    None,
    Weak,
    Strong,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SnapshotReport {
    pub token: Option<SnapshotToken>,
    pub strength: SnapshotStrength,
    pub stale: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AdapterWarningKind {
    UnknownEvent,
    UnknownField,
    TruncatedRecord,
    FieldConflict,
    StaleSnapshot,
    IncompleteCapability,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdapterWarning {
    pub kind: AdapterWarningKind,
    pub source_kind: String,
    pub stage: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ColumnCapability {
    pub name: ColumnName,
    pub access: AccessClass,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Capabilities {
    pub tables: Vec<TableName>,
    pub columns: Vec<ColumnCapability>,
    pub snapshot_strength: SnapshotStrength,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdapterSchema {
    pub columns: Vec<ColumnCapability>,
}

#[derive(Clone, Debug)]
pub struct ScanRequest {
    pub source: SourceManifest,
    pub table: TableName,
    pub projection: Vec<ColumnName>,
    pub predicates: Vec<Predicate>,
    pub limit: Option<u64>,
    pub order_hint: Vec<OrderingHint>,
    pub access: AccessGrant,
    pub budget: ResourceBudget,
    pub cancellation: CancellationToken,
    pub snapshot: Option<SnapshotToken>,
}

pub type RecordStream = Box<dyn Iterator<Item = Result<CanonicalRecord, AdapterError>> + Send>;

#[derive(Clone, Debug, Default)]
pub struct ScanDiagnostics(Arc<Mutex<Vec<AdapterWarning>>>);

impl ScanDiagnostics {
    pub fn push(&self, warning: AdapterWarning) -> Result<(), AdapterError> {
        self.0
            .lock()
            .map_err(|_| AdapterError::Internal {
                stage: "scan_diagnostics".to_string(),
            })?
            .push(warning);
        Ok(())
    }

    pub fn snapshot(&self) -> Result<Vec<AdapterWarning>, AdapterError> {
        self.0
            .lock()
            .map(|warnings| warnings.clone())
            .map_err(|_| AdapterError::Internal {
                stage: "scan_diagnostics".to_string(),
            })
    }
}

pub struct ScanResult {
    pub records: RecordStream,
    pub pushdown: PushdownReport,
    pub diagnostics: ScanDiagnostics,
    pub snapshot: SnapshotReport,
}

#[derive(Debug, Error, Eq, PartialEq)]
pub enum AdapterError {
    #[error("source not found at stage {stage}")]
    NotFound { stage: String },
    #[error("source permission denied at stage {stage}")]
    PermissionDenied { stage: String },
    #[error("unsupported source format at stage {stage}")]
    UnsupportedFormat { stage: String },
    #[error("corrupt source at stage {stage}")]
    CorruptSource { stage: String },
    #[error("access denied for column {column}")]
    AccessDenied { column: String },
    #[error("resource budget exceeded: {resource}")]
    BudgetExceeded { resource: String, actual: u64 },
    #[error("scan cancelled")]
    Cancelled,
    #[error("snapshot unavailable")]
    SnapshotUnavailable,
    #[error("internal adapter error at stage {stage}")]
    Internal { stage: String },
}

pub trait AgentAdapter: Send + Sync {
    fn id(&self) -> &'static str;
    fn probe(&self, request: &ProbeRequest) -> Result<ProbeResult, AdapterError>;
    fn capabilities(&self, manifest: &SourceManifest) -> Capabilities;
    fn schema(&self, manifest: &SourceManifest) -> AdapterSchema;
    fn scan(&self, request: ScanRequest) -> Result<ScanResult, AdapterError>;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SourceKind {
    StateDatabase,
    SessionIndex,
    Rollout,
}

pub trait FileAccessObserver: Send + Sync {
    fn opened(&self, source_kind: SourceKind);
    fn bytes_read(&self, source_kind: SourceKind, count: u64);
}

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
