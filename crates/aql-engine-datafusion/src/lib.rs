use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    ops::ControlFlow,
};

use aql_adapter_api::{
    AccessGrant, AgentAdapter, CancellationToken, ColumnName, RecordStream, ResourceBudget,
    ScanDiagnostics, ScanRequest, TableName,
};
use aql_catalog::Catalog;
use aql_model::{
    AccessClass, CanonicalRecord, FieldCost, SourceManifest, installation_scoped_hmac,
};
use async_trait::async_trait;
use datafusion::arrow::array::{
    Array, ArrayRef, BooleanArray, Int64Array, StringArray, StringBuilder,
    TimestampMillisecondArray,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use datafusion::arrow::record_batch::{RecordBatch, RecordBatchOptions};
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::ScalarValue;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::datasource::ViewTable;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::disk_manager::{DiskManagerBuilder, DiskManagerMode};
use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryConsumer, MemoryReservation};
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::logical_expr::{
    ColumnarValue, Expr, LogicalPlan, Operator, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl,
    Signature, TableProviderFilterPushDown, TableType, TypeSignature, Volatility,
};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::streaming::{PartitionStream, StreamingTableExec};
use datafusion::physical_plan::{ExecutionPlan, SendableRecordBatchStream};
use datafusion::prelude::{SessionConfig, SessionContext};
use futures::{StreamExt, TryStreamExt, stream};
use sqlparser::ast::{
    Expr as SqlExpr, FunctionArg, FunctionArgExpr, FunctionArguments, Ident, ObjectName, Query,
    Select, SelectItem, SelectItemQualifiedWildcardKind, SetExpr, Statement, TableFactor, Value,
    Visit, Visitor,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

mod sql_firewall;

use sql_firewall::sql_rejected;
pub use sql_firewall::{QueryError, ValidatedSql, validate_read_only_sql};

const MAX_SQL_BYTES: usize = 64 * 1024;
const MAX_QUERY_DEPTH: usize = 64;
const MAX_EXPRESSIONS: usize = 256;
const MAX_CTES: usize = 32;
const MAX_JOINS: usize = 16;
const QUERY_MEMORY_BYTES: usize = 256 * 1024 * 1024;
const CANONICAL_TABLES: [&str; 7] = [
    "agents",
    "sessions",
    "messages",
    "tool_calls",
    "usage",
    "session_edges",
    "artifacts",
];

const USAGE_VIEW_SQL: &str = r#"
SELECT
  concat('message:', m.message_id) AS usage_id,
  m.source_id,
  s.agent_id,
  m.session_id,
  coalesce(m.model, s.model) AS model,
  s.provider,
  m.created_at AS bucket_start,
  m.input_tokens,
  m.output_tokens,
  m.cached_tokens,
  CASE
    WHEN m.input_tokens IS NULL AND m.output_tokens IS NULL AND m.cached_tokens IS NULL THEN CAST(NULL AS BIGINT)
    ELSE coalesce(m.input_tokens, 0) + coalesce(m.output_tokens, 0) + coalesce(m.cached_tokens, 0)
  END AS total_tokens,
  CAST(1 AS BIGINT) AS message_count,
  CAST(0 AS BIGINT) AS tool_call_count,
  CASE WHEN m.is_error = true THEN CAST(1 AS BIGINT) ELSE CAST(0 AS BIGINT) END AS error_count
FROM messages m
JOIN sessions s ON m.session_id = s.session_id
UNION ALL
SELECT
  concat('tool:', t.tool_call_id) AS usage_id,
  t.source_id,
  s.agent_id,
  t.session_id,
  s.model,
  s.provider,
  t.started_at AS bucket_start,
  CAST(NULL AS BIGINT) AS input_tokens,
  CAST(NULL AS BIGINT) AS output_tokens,
  CAST(NULL AS BIGINT) AS cached_tokens,
  CAST(NULL AS BIGINT) AS total_tokens,
  CAST(0 AS BIGINT) AS message_count,
  CAST(1 AS BIGINT) AS tool_call_count,
  CASE WHEN t.status = 'error' OR coalesce(t.exit_code, 0) <> 0 THEN CAST(1 AS BIGINT) ELSE CAST(0 AS BIGINT) END AS error_count
FROM tool_calls t
JOIN sessions s ON t.session_id = s.session_id
UNION ALL
SELECT
  usage_id,
  source_id,
  agent_id,
  session_id,
  model,
  provider,
  bucket_start,
  input_tokens,
  output_tokens,
  cached_tokens,
  total_tokens,
  message_count,
  tool_call_count,
  error_count
FROM _aql_usage_records
"#;
const ALLOWED_FUNCTIONS: [&str; 20] = [
    "abs",
    "avg",
    "char_length",
    "coalesce",
    "count",
    "date_trunc",
    "length",
    "lower",
    "max",
    "min",
    "nullif",
    "round",
    "substr",
    "substring",
    "sum",
    "trim",
    "upper",
    "concat",
    "redact",
    "mask_path",
];

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QueryDataType {
    Text,
    Int64,
    Bool,
    Timestamp,
    Json,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct QueryColumn {
    pub name: &'static str,
    pub data_type: QueryDataType,
    pub nullable: bool,
    pub access: AccessClass,
    pub cost: FieldCost,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct QueryTableSchema {
    pub name: &'static str,
    pub columns: &'static [QueryColumn],
}

const fn query_column(
    name: &'static str,
    data_type: QueryDataType,
    nullable: bool,
    access: AccessClass,
    cost: FieldCost,
) -> QueryColumn {
    QueryColumn {
        name,
        data_type,
        nullable,
        access,
        cost,
    }
}

const AGENT_QUERY_COLUMNS: &[QueryColumn] = &[
    query_column(
        "source_id",
        QueryDataType::Text,
        false,
        AccessClass::Safe,
        FieldCost::Metadata,
    ),
    query_column(
        "agent_id",
        QueryDataType::Text,
        false,
        AccessClass::Safe,
        FieldCost::Metadata,
    ),
    query_column(
        "display_name",
        QueryDataType::Text,
        false,
        AccessClass::Safe,
        FieldCost::Metadata,
    ),
    query_column(
        "format_fingerprint",
        QueryDataType::Text,
        false,
        AccessClass::Safe,
        FieldCost::Metadata,
    ),
    query_column(
        "snapshot_state",
        QueryDataType::Text,
        false,
        AccessClass::Safe,
        FieldCost::Metadata,
    ),
    query_column(
        "capabilities",
        QueryDataType::Json,
        false,
        AccessClass::Safe,
        FieldCost::Metadata,
    ),
];

const SESSION_QUERY_COLUMNS: &[QueryColumn] = &[
    query_column(
        "session_id",
        QueryDataType::Text,
        false,
        AccessClass::Safe,
        FieldCost::Metadata,
    ),
    query_column(
        "native_id",
        QueryDataType::Text,
        false,
        AccessClass::Safe,
        FieldCost::Metadata,
    ),
    query_column(
        "source_id",
        QueryDataType::Text,
        false,
        AccessClass::Safe,
        FieldCost::Metadata,
    ),
    query_column(
        "agent_id",
        QueryDataType::Text,
        false,
        AccessClass::Safe,
        FieldCost::Metadata,
    ),
    query_column(
        "title",
        QueryDataType::Text,
        true,
        AccessClass::Content,
        FieldCost::Metadata,
    ),
    query_column(
        "preview",
        QueryDataType::Text,
        true,
        AccessClass::Content,
        FieldCost::Content,
    ),
    query_column(
        "cwd",
        QueryDataType::Text,
        true,
        AccessClass::Path,
        FieldCost::Metadata,
    ),
    query_column(
        "project",
        QueryDataType::Text,
        true,
        AccessClass::Path,
        FieldCost::Derived,
    ),
    query_column(
        "model",
        QueryDataType::Text,
        true,
        AccessClass::Safe,
        FieldCost::Metadata,
    ),
    query_column(
        "provider",
        QueryDataType::Text,
        true,
        AccessClass::Safe,
        FieldCost::Metadata,
    ),
    query_column(
        "created_at",
        QueryDataType::Timestamp,
        true,
        AccessClass::Safe,
        FieldCost::Metadata,
    ),
    query_column(
        "updated_at",
        QueryDataType::Timestamp,
        true,
        AccessClass::Safe,
        FieldCost::Metadata,
    ),
    query_column(
        "status",
        QueryDataType::Text,
        true,
        AccessClass::Safe,
        FieldCost::Derived,
    ),
    query_column(
        "archived",
        QueryDataType::Bool,
        true,
        AccessClass::Safe,
        FieldCost::Metadata,
    ),
    query_column(
        "message_count",
        QueryDataType::Int64,
        true,
        AccessClass::Safe,
        FieldCost::Derived,
    ),
    query_column(
        "tool_call_count",
        QueryDataType::Int64,
        true,
        AccessClass::Safe,
        FieldCost::Derived,
    ),
    query_column(
        "tokens_used",
        QueryDataType::Int64,
        true,
        AccessClass::Safe,
        FieldCost::Metadata,
    ),
    query_column(
        "identity_confidence",
        QueryDataType::Text,
        false,
        AccessClass::Safe,
        FieldCost::Metadata,
    ),
    query_column(
        "snapshot_state",
        QueryDataType::Text,
        false,
        AccessClass::Safe,
        FieldCost::Metadata,
    ),
];

const MESSAGE_QUERY_COLUMNS: &[QueryColumn] = &[
    query_column(
        "message_id",
        QueryDataType::Text,
        false,
        AccessClass::Safe,
        FieldCost::Metadata,
    ),
    query_column(
        "session_id",
        QueryDataType::Text,
        false,
        AccessClass::Safe,
        FieldCost::Metadata,
    ),
    query_column(
        "source_id",
        QueryDataType::Text,
        false,
        AccessClass::Safe,
        FieldCost::Metadata,
    ),
    query_column(
        "sequence",
        QueryDataType::Int64,
        false,
        AccessClass::Safe,
        FieldCost::Metadata,
    ),
    query_column(
        "role",
        QueryDataType::Text,
        false,
        AccessClass::Safe,
        FieldCost::Metadata,
    ),
    query_column(
        "kind",
        QueryDataType::Text,
        true,
        AccessClass::Safe,
        FieldCost::Metadata,
    ),
    query_column(
        "content",
        QueryDataType::Text,
        true,
        AccessClass::Content,
        FieldCost::Content,
    ),
    query_column(
        "content_json",
        QueryDataType::Json,
        true,
        AccessClass::Content,
        FieldCost::Heavy,
    ),
    query_column(
        "model",
        QueryDataType::Text,
        true,
        AccessClass::Safe,
        FieldCost::Metadata,
    ),
    query_column(
        "created_at",
        QueryDataType::Timestamp,
        true,
        AccessClass::Safe,
        FieldCost::Metadata,
    ),
    query_column(
        "input_tokens",
        QueryDataType::Int64,
        true,
        AccessClass::Safe,
        FieldCost::Metadata,
    ),
    query_column(
        "output_tokens",
        QueryDataType::Int64,
        true,
        AccessClass::Safe,
        FieldCost::Metadata,
    ),
    query_column(
        "cached_tokens",
        QueryDataType::Int64,
        true,
        AccessClass::Safe,
        FieldCost::Metadata,
    ),
    query_column(
        "is_error",
        QueryDataType::Bool,
        true,
        AccessClass::Safe,
        FieldCost::Metadata,
    ),
];

const TOOL_CALL_QUERY_COLUMNS: &[QueryColumn] = &[
    query_column(
        "tool_call_id",
        QueryDataType::Text,
        false,
        AccessClass::Safe,
        FieldCost::Metadata,
    ),
    query_column(
        "session_id",
        QueryDataType::Text,
        false,
        AccessClass::Safe,
        FieldCost::Metadata,
    ),
    query_column(
        "message_id",
        QueryDataType::Text,
        true,
        AccessClass::Safe,
        FieldCost::Metadata,
    ),
    query_column(
        "source_id",
        QueryDataType::Text,
        false,
        AccessClass::Safe,
        FieldCost::Metadata,
    ),
    query_column(
        "sequence",
        QueryDataType::Int64,
        false,
        AccessClass::Safe,
        FieldCost::Metadata,
    ),
    query_column(
        "tool_name",
        QueryDataType::Text,
        false,
        AccessClass::Safe,
        FieldCost::Metadata,
    ),
    query_column(
        "namespace",
        QueryDataType::Text,
        true,
        AccessClass::Safe,
        FieldCost::Metadata,
    ),
    query_column(
        "arguments",
        QueryDataType::Json,
        true,
        AccessClass::ToolInput,
        FieldCost::Heavy,
    ),
    query_column(
        "output",
        QueryDataType::Text,
        true,
        AccessClass::ToolOutput,
        FieldCost::Heavy,
    ),
    query_column(
        "status",
        QueryDataType::Text,
        true,
        AccessClass::Safe,
        FieldCost::Metadata,
    ),
    query_column(
        "started_at",
        QueryDataType::Timestamp,
        true,
        AccessClass::Safe,
        FieldCost::Metadata,
    ),
    query_column(
        "ended_at",
        QueryDataType::Timestamp,
        true,
        AccessClass::Safe,
        FieldCost::Metadata,
    ),
    query_column(
        "duration_ms",
        QueryDataType::Int64,
        true,
        AccessClass::Safe,
        FieldCost::Derived,
    ),
    query_column(
        "exit_code",
        QueryDataType::Int64,
        true,
        AccessClass::Safe,
        FieldCost::Metadata,
    ),
];

const USAGE_QUERY_COLUMNS: &[QueryColumn] = &[
    query_column(
        "usage_id",
        QueryDataType::Text,
        false,
        AccessClass::Safe,
        FieldCost::Derived,
    ),
    query_column(
        "source_id",
        QueryDataType::Text,
        false,
        AccessClass::Safe,
        FieldCost::Metadata,
    ),
    query_column(
        "agent_id",
        QueryDataType::Text,
        false,
        AccessClass::Safe,
        FieldCost::Metadata,
    ),
    query_column(
        "session_id",
        QueryDataType::Text,
        true,
        AccessClass::Safe,
        FieldCost::Metadata,
    ),
    query_column(
        "model",
        QueryDataType::Text,
        true,
        AccessClass::Safe,
        FieldCost::Metadata,
    ),
    query_column(
        "provider",
        QueryDataType::Text,
        true,
        AccessClass::Safe,
        FieldCost::Metadata,
    ),
    query_column(
        "bucket_start",
        QueryDataType::Timestamp,
        true,
        AccessClass::Safe,
        FieldCost::Derived,
    ),
    query_column(
        "input_tokens",
        QueryDataType::Int64,
        true,
        AccessClass::Safe,
        FieldCost::Derived,
    ),
    query_column(
        "output_tokens",
        QueryDataType::Int64,
        true,
        AccessClass::Safe,
        FieldCost::Derived,
    ),
    query_column(
        "cached_tokens",
        QueryDataType::Int64,
        true,
        AccessClass::Safe,
        FieldCost::Derived,
    ),
    query_column(
        "total_tokens",
        QueryDataType::Int64,
        true,
        AccessClass::Safe,
        FieldCost::Derived,
    ),
    query_column(
        "message_count",
        QueryDataType::Int64,
        false,
        AccessClass::Safe,
        FieldCost::Derived,
    ),
    query_column(
        "tool_call_count",
        QueryDataType::Int64,
        false,
        AccessClass::Safe,
        FieldCost::Derived,
    ),
    query_column(
        "error_count",
        QueryDataType::Int64,
        false,
        AccessClass::Safe,
        FieldCost::Derived,
    ),
];

const SESSION_EDGE_QUERY_COLUMNS: &[QueryColumn] = &[
    query_column(
        "edge_id",
        QueryDataType::Text,
        false,
        AccessClass::Safe,
        FieldCost::Metadata,
    ),
    query_column(
        "source_id",
        QueryDataType::Text,
        false,
        AccessClass::Safe,
        FieldCost::Metadata,
    ),
    query_column(
        "parent_session_id",
        QueryDataType::Text,
        false,
        AccessClass::Safe,
        FieldCost::Metadata,
    ),
    query_column(
        "child_session_id",
        QueryDataType::Text,
        false,
        AccessClass::Safe,
        FieldCost::Metadata,
    ),
    query_column(
        "edge_kind",
        QueryDataType::Text,
        false,
        AccessClass::Safe,
        FieldCost::Metadata,
    ),
    query_column(
        "created_at",
        QueryDataType::Timestamp,
        true,
        AccessClass::Safe,
        FieldCost::Metadata,
    ),
    query_column(
        "native_edge_id",
        QueryDataType::Text,
        true,
        AccessClass::Safe,
        FieldCost::Metadata,
    ),
];

const ARTIFACT_QUERY_COLUMNS: &[QueryColumn] = &[
    query_column(
        "artifact_id",
        QueryDataType::Text,
        false,
        AccessClass::Safe,
        FieldCost::Metadata,
    ),
    query_column(
        "source_id",
        QueryDataType::Text,
        false,
        AccessClass::Safe,
        FieldCost::Metadata,
    ),
    query_column(
        "session_id",
        QueryDataType::Text,
        false,
        AccessClass::Safe,
        FieldCost::Metadata,
    ),
    query_column(
        "tool_call_id",
        QueryDataType::Text,
        true,
        AccessClass::Safe,
        FieldCost::Metadata,
    ),
    query_column(
        "kind",
        QueryDataType::Text,
        false,
        AccessClass::Safe,
        FieldCost::Metadata,
    ),
    query_column(
        "name",
        QueryDataType::Text,
        true,
        AccessClass::Content,
        FieldCost::Metadata,
    ),
    query_column(
        "path",
        QueryDataType::Text,
        true,
        AccessClass::Path,
        FieldCost::Metadata,
    ),
    query_column(
        "media_type",
        QueryDataType::Text,
        true,
        AccessClass::Safe,
        FieldCost::Metadata,
    ),
    query_column(
        "size_bytes",
        QueryDataType::Int64,
        true,
        AccessClass::Safe,
        FieldCost::Metadata,
    ),
    query_column(
        "created_at",
        QueryDataType::Timestamp,
        true,
        AccessClass::Safe,
        FieldCost::Metadata,
    ),
    query_column(
        "content",
        QueryDataType::Text,
        true,
        AccessClass::Content,
        FieldCost::Content,
    ),
    query_column(
        "content_json",
        QueryDataType::Json,
        true,
        AccessClass::Content,
        FieldCost::Heavy,
    ),
];

pub const QUERY_SCHEMAS: &[QueryTableSchema] = &[
    QueryTableSchema {
        name: "agents",
        columns: AGENT_QUERY_COLUMNS,
    },
    QueryTableSchema {
        name: "sessions",
        columns: SESSION_QUERY_COLUMNS,
    },
    QueryTableSchema {
        name: "messages",
        columns: MESSAGE_QUERY_COLUMNS,
    },
    QueryTableSchema {
        name: "tool_calls",
        columns: TOOL_CALL_QUERY_COLUMNS,
    },
    QueryTableSchema {
        name: "usage",
        columns: USAGE_QUERY_COLUMNS,
    },
    QueryTableSchema {
        name: "session_edges",
        columns: SESSION_EDGE_QUERY_COLUMNS,
    },
    QueryTableSchema {
        name: "artifacts",
        columns: ARTIFACT_QUERY_COLUMNS,
    },
];

#[derive(Debug, Eq, Hash, PartialEq)]
struct RedactUdf {
    signature: Signature,
    salt: Arc<Vec<u8>>,
}

impl RedactUdf {
    fn new(salt: Vec<u8>) -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Exact(vec![DataType::Utf8]),
                    TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8]),
                ],
                Volatility::Immutable,
            ),
            salt: Arc::new(salt),
        }
    }
}

impl ScalarUDFImpl for RedactUdf {
    fn name(&self) -> &str {
        "redact"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        apply_string_udf(&args.args, |value, policy, _| {
            match policy.unwrap_or("placeholder") {
                "placeholder" => Ok("[REDACTED]".to_string()),
                "last4" => {
                    let suffix = value
                        .chars()
                        .rev()
                        .take(4)
                        .collect::<Vec<_>>()
                        .into_iter()
                        .rev()
                        .collect::<String>();
                    Ok(format!("[REDACTED]…{suffix}"))
                }
                "hash" => {
                    if self.salt.is_empty() {
                        return Err(DataFusionError::Execution(
                            "redaction salt is unavailable".to_string(),
                        ));
                    }
                    let digest = installation_scoped_hmac("redact", value, &self.salt);
                    Ok(format!("hmac:{}", &digest[..24]))
                }
                _ => Err(DataFusionError::Plan(
                    "unsupported redaction policy".to_string(),
                )),
            }
        })
    }
}

#[derive(Debug, Eq, Hash, PartialEq)]
struct MaskPathUdf {
    signature: Signature,
}

impl MaskPathUdf {
    fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Exact(vec![DataType::Utf8]),
                    TypeSignature::Exact(vec![DataType::Utf8, DataType::Int64]),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for MaskPathUdf {
    fn name(&self) -> &str {
        "mask_path"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        apply_string_udf(&args.args, |value, _, depth| {
            let depth = depth.unwrap_or(1);
            if !(1..=16).contains(&depth) {
                return Err(DataFusionError::Plan(
                    "path mask depth must be between 1 and 16".to_string(),
                ));
            }
            let components = value
                .split(['/', '\\'])
                .filter(|component| !component.is_empty())
                .collect::<Vec<_>>();
            let start = components.len().saturating_sub(depth as usize);
            Ok(format!("…/{}", components[start..].join("/")))
        })
    }
}

fn apply_string_udf(
    args: &[ColumnarValue],
    transform: impl Fn(&str, Option<&str>, Option<i64>) -> Result<String>,
) -> Result<ColumnarValue> {
    let scalar = args
        .iter()
        .all(|value| matches!(value, ColumnarValue::Scalar(_)));
    let arrays = ColumnarValue::values_to_arrays(args)?;
    let values = arrays[0]
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| DataFusionError::Plan("function input must be text".to_string()))?;
    let policy = arrays
        .get(1)
        .and_then(|array| array.as_any().downcast_ref::<StringArray>());
    let depth = arrays
        .get(1)
        .and_then(|array| array.as_any().downcast_ref::<Int64Array>());
    let mut output = StringBuilder::new();
    for index in 0..values.len() {
        if values.is_null(index) {
            output.append_null();
            continue;
        }
        let policy = policy.and_then(|array| (!array.is_null(index)).then(|| array.value(index)));
        let depth = depth.and_then(|array| (!array.is_null(index)).then(|| array.value(index)));
        output.append_value(transform(values.value(index), policy, depth)?);
    }
    let output: ArrayRef = Arc::new(output.finish());
    if scalar {
        Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
            &output, 0,
        )?))
    } else {
        Ok(ColumnarValue::Array(output))
    }
}

#[derive(Clone)]
pub struct QueryOptions {
    pub access: AccessGrant,
    pub budget: ResourceBudget,
    pub cancellation: CancellationToken,
    pub max_memory_bytes: usize,
    pub redaction_salt: Vec<u8>,
}

impl Default for QueryOptions {
    fn default() -> Self {
        Self {
            access: AccessGrant::default(),
            budget: ResourceBudget::default(),
            cancellation: CancellationToken::default(),
            max_memory_bytes: QUERY_MEMORY_BYTES,
            redaction_salt: Vec::new(),
        }
    }
}

pub struct PreparedQuery {
    context: SessionContext,
    plan: LogicalPlan,
    providers: Vec<Arc<DeferredTable>>,
    options: QueryOptions,
    metadata: Arc<Mutex<QueryMetadata>>,
    plan_summary: PlanSummary,
}

#[derive(Clone, Debug, Default)]
pub struct QueryMetadata {
    pub source_ids: Vec<String>,
    pub warnings: Vec<String>,
    pub scans: Vec<ScanMetadata>,
    pub records_scanned: u64,
    pub bytes_read: u64,
    pub output_bytes: u64,
}

#[derive(Clone, Debug)]
pub struct ScanMetadata {
    pub table: String,
    pub source_id: String,
    pub predicate_pushdown: Vec<String>,
    pub limit_pushdown: Option<String>,
    pub ordering_pushdown: Vec<String>,
    pub snapshot_strength: String,
    pub stale: bool,
}

#[derive(Clone, Debug, Default)]
pub struct PlanSummary {
    pub tables: Vec<String>,
    pub columns: Vec<String>,
    pub required_access: Vec<String>,
    pub max_records: u64,
    pub max_bytes_read: u64,
    pub max_output_bytes: u64,
    pub max_memory_bytes: usize,
}

pub struct QueryResult {
    pub batches: Vec<RecordBatch>,
    pub metadata: QueryMetadata,
}

pub struct StreamingQueryResult {
    pub stream: SendableRecordBatchStream,
    pub metadata: QueryMetadataHandle,
}

pub struct QueryMetadataHandle {
    metadata: Arc<Mutex<QueryMetadata>>,
    options: QueryOptions,
    stream_complete: Arc<AtomicBool>,
}

impl QueryMetadataHandle {
    pub fn finish(self) -> std::result::Result<QueryMetadata, QueryError> {
        if !self.stream_complete.load(Ordering::Acquire) {
            return Err(sql_rejected(
                "metadata",
                "query stream must be consumed to completion before metadata is finalized",
            ));
        }
        let mut metadata = self
            .metadata
            .lock()
            .map_err(|_| QueryError::SqlRejected {
                stage: "metadata",
                reason: "query metadata is unavailable",
            })?
            .clone();
        metadata.records_scanned = self.options.budget.records_used();
        metadata.bytes_read = self.options.budget.bytes_read_used();
        metadata.output_bytes = self.options.budget.output_bytes_used();
        Ok(metadata)
    }
}

#[derive(Clone)]
pub struct FederatedSource {
    pub adapter: Arc<dyn AgentAdapter>,
    pub manifest: SourceManifest,
}

impl std::fmt::Debug for PreparedQuery {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PreparedQuery")
            .field("plan", &self.plan)
            .finish_non_exhaustive()
    }
}

impl PreparedQuery {
    #[must_use]
    pub fn plan_summary(&self) -> &PlanSummary {
        &self.plan_summary
    }

    pub async fn execute(
        self,
        sources: Vec<FederatedSource>,
    ) -> std::result::Result<QueryResult, QueryError> {
        let StreamingQueryResult { stream, metadata } = self.execute_stream(sources).await?;
        let batches = stream.try_collect::<Vec<_>>().await?;
        Ok(QueryResult {
            batches,
            metadata: metadata.finish()?,
        })
    }

    pub async fn execute_stream(
        self,
        sources: Vec<FederatedSource>,
    ) -> std::result::Result<StreamingQueryResult, QueryError> {
        if sources.is_empty() {
            return Err(sql_rejected("bind", "probe returned no compatible source"));
        }
        {
            let mut metadata = self.metadata.lock().map_err(|_| QueryError::SqlRejected {
                stage: "metadata",
                reason: "query metadata is unavailable",
            })?;
            metadata.source_ids = sources
                .iter()
                .map(|source| source.manifest.source_id.to_string())
                .collect();
            metadata.warnings.extend(
                sources
                    .iter()
                    .flat_map(|source| source.manifest.warnings.iter().cloned()),
            );
        }
        for provider in &self.providers {
            provider.bind(Binding {
                sources: sources.clone(),
                options: self.options.clone(),
                metadata: self.metadata.clone(),
            })?;
        }
        let stream = self
            .context
            .execute_logical_plan(self.plan)
            .await?
            .execute_stream()
            .await?;
        let schema = stream.schema();
        let stream_complete = Arc::new(AtomicBool::new(false));
        let completion = stream_complete.clone();
        let mut stream = stream;
        let stream = stream::poll_fn(move |context| match stream.poll_next_unpin(context) {
            std::task::Poll::Ready(None) => {
                completion.store(true, Ordering::Release);
                std::task::Poll::Ready(None)
            }
            result => result,
        });
        Ok(StreamingQueryResult {
            stream: Box::pin(RecordBatchStreamAdapter::new(schema, stream)),
            metadata: QueryMetadataHandle {
                metadata: self.metadata,
                options: self.options,
                stream_complete,
            },
        })
    }
}

pub async fn prepare_query(
    sql: &ValidatedSql,
    options: QueryOptions,
) -> std::result::Result<PreparedQuery, QueryError> {
    if options.max_memory_bytes == 0 {
        return Err(sql_rejected(
            "budget",
            "query memory budget must be greater than zero",
        ));
    }
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_pool(Arc::new(GreedyMemoryPool::new(options.max_memory_bytes)))
        .with_disk_manager_builder(
            DiskManagerBuilder::default().with_mode(DiskManagerMode::Disabled),
        )
        .build()?;
    let context = SessionContext::new_with_config_rt(SessionConfig::new(), Arc::new(runtime));
    context.register_udf(ScalarUDF::new_from_impl(RedactUdf::new(
        options.redaction_salt.clone(),
    )));
    context.register_udf(ScalarUDF::new_from_impl(MaskPathUdf::new()));
    let mut providers = Vec::new();
    for query_schema in QUERY_SCHEMAS.iter().filter(|schema| schema.name != "usage") {
        let provider = Arc::new(DeferredTable::new(query_schema));
        context.register_table(query_schema.name, provider.clone())?;
        providers.push(provider);
    }
    let usage_schema = QUERY_SCHEMAS
        .iter()
        .find(|schema| schema.name == "usage")
        .ok_or_else(|| DataFusionError::Plan("usage schema is unavailable".to_string()))?;
    let usage_provider = Arc::new(DeferredTable::new(usage_schema));
    context.register_table("_aql_usage_records", usage_provider.clone())?;
    providers.push(usage_provider);
    let usage_plan = context.state().create_logical_plan(USAGE_VIEW_SQL).await?;
    context.register_table("usage", Arc::new(ViewTable::new(usage_plan, None)))?;
    let plan = context
        .state()
        .create_logical_plan(&sql.normalized_sql())
        .await?;
    let plan = context.state().optimize(&plan)?;
    validate_plan_access(&plan, options.access)?;
    let plan_summary = summarize_plan(&plan, &options)?;
    let metadata = Arc::new(Mutex::new(QueryMetadata::default()));
    Ok(PreparedQuery {
        context,
        plan,
        providers,
        options,
        metadata,
        plan_summary,
    })
}

pub async fn query_sessions(
    adapter: Arc<dyn AgentAdapter>,
    source: SourceManifest,
    sql: &ValidatedSql,
    options: QueryOptions,
) -> std::result::Result<Vec<RecordBatch>, QueryError> {
    prepare_query(sql, options)
        .await?
        .execute(vec![FederatedSource {
            adapter,
            manifest: source,
        }])
        .await
        .map(|result| result.batches)
}

fn summarize_plan(plan: &LogicalPlan, options: &QueryOptions) -> Result<PlanSummary> {
    let mut tables = BTreeSet::new();
    let mut columns = BTreeSet::new();
    let mut required_access = BTreeSet::new();
    plan.apply(|node| {
        if let LogicalPlan::TableScan(scan) = node {
            let table_name = scan.table_name.to_string();
            let table_name = table_name
                .rsplit('.')
                .next()
                .unwrap_or(&table_name)
                .to_string();
            let table_name = if table_name == "_aql_usage_records" {
                "usage".to_string()
            } else {
                table_name
            };
            tables.insert(table_name.clone());
            if table_name == "artifacts" {
                required_access.insert("path".to_string());
            }
            if let Some(schema) = QUERY_SCHEMAS
                .iter()
                .find(|schema| schema.name == table_name)
            {
                for field in scan.projected_schema.fields() {
                    columns.insert(format!("{table_name}.{}", field.name()));
                    if let Some(column) = schema
                        .columns
                        .iter()
                        .find(|column| column.name == field.name())
                        && column.access != AccessClass::Safe
                    {
                        required_access.insert(required_grant(column.access).to_string());
                    }
                }
            }
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    Ok(PlanSummary {
        tables: tables.into_iter().collect(),
        columns: columns.into_iter().collect(),
        required_access: required_access.into_iter().collect(),
        max_records: options.budget.max_records,
        max_bytes_read: options.budget.max_bytes_read,
        max_output_bytes: options.budget.max_output_bytes,
        max_memory_bytes: options.max_memory_bytes,
    })
}

fn validate_plan_access(
    plan: &LogicalPlan,
    access: AccessGrant,
) -> std::result::Result<(), QueryError> {
    let mut denied = None;
    plan.apply(|node| {
        if let LogicalPlan::TableScan(scan) = node {
            let table_name = scan.table_name.to_string();
            let table_name = table_name.rsplit('.').next().unwrap_or(&table_name);
            let table_name = if table_name == "_aql_usage_records" {
                "usage"
            } else {
                table_name
            };
            let Some(query_schema) = QUERY_SCHEMAS
                .iter()
                .find(|schema| schema.name.eq_ignore_ascii_case(table_name))
            else {
                return Err(DataFusionError::Plan(
                    "logical plan contains a non-canonical table".to_string(),
                ));
            };
            if query_schema.name == "artifacts" && !access.path {
                denied = Some("path");
                return Ok(TreeNodeRecursion::Stop);
            }
            for field in scan.projected_schema.fields() {
                if let Some(column) = query_schema
                    .columns
                    .iter()
                    .find(|column| column.name.eq_ignore_ascii_case(field.name()))
                    && !access.allows(column.access)
                {
                    denied = Some(required_grant(column.access));
                    return Ok(TreeNodeRecursion::Stop);
                }
            }
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    if let Some(grant) = denied {
        Err(QueryError::AccessDenied(grant))
    } else {
        Ok(())
    }
}

fn required_grant(access: AccessClass) -> &'static str {
    match access {
        AccessClass::Safe => "safe",
        AccessClass::Path => "path",
        AccessClass::Content => "content",
        AccessClass::ToolInput => "tool-input",
        AccessClass::ToolOutput => "tool-output",
        AccessClass::Secret => "secret-denied",
    }
}

#[derive(Clone)]
struct Binding {
    sources: Vec<FederatedSource>,
    options: QueryOptions,
    metadata: Arc<Mutex<QueryMetadata>>,
}

struct DeferredTable {
    table: &'static QueryTableSchema,
    binding: Mutex<Option<Binding>>,
    schema: SchemaRef,
}

impl std::fmt::Debug for DeferredTable {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DeferredTable")
            .field("table", &self.table.name)
            .finish_non_exhaustive()
    }
}

impl DeferredTable {
    fn new(table: &'static QueryTableSchema) -> Self {
        let schema = Arc::new(Schema::new(
            table
                .columns
                .iter()
                .map(|column| {
                    Field::new(
                        column.name,
                        match column.data_type {
                            QueryDataType::Text | QueryDataType::Json => DataType::Utf8,
                            QueryDataType::Int64 => DataType::Int64,
                            QueryDataType::Bool => DataType::Boolean,
                            QueryDataType::Timestamp => {
                                DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into()))
                            }
                        },
                        column.nullable,
                    )
                })
                .collect::<Vec<_>>(),
        ));
        Self {
            table,
            binding: Mutex::new(None),
            schema,
        }
    }

    fn bind(&self, binding: Binding) -> std::result::Result<(), QueryError> {
        let mut slot = self.binding.lock().map_err(|_| QueryError::SqlRejected {
            stage: "bind",
            reason: "query provider state is unavailable",
        })?;
        *slot = Some(binding);
        Ok(())
    }
}

#[async_trait]
impl TableProvider for DeferredTable {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        Ok(filters
            .iter()
            .map(|filter| {
                if expr_to_predicate(filter).is_some() {
                    TableProviderFilterPushDown::Inexact
                } else {
                    TableProviderFilterPushDown::Unsupported
                }
            })
            .collect())
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let binding = self
            .binding
            .lock()
            .map_err(|_| DataFusionError::Execution("query provider state is unavailable".into()))?
            .clone()
            .ok_or_else(|| DataFusionError::Execution("query provider is not bound".into()))?;
        let projection = projection
            .cloned()
            .unwrap_or_else(|| (0..self.table.columns.len()).collect());
        let projected_schema = Arc::new(self.schema.project(&projection)?);
        let partition = Arc::new(AdapterPartition {
            table: self.table,
            binding,
            projection,
            schema: projected_schema.clone(),
            limit,
            predicates: filters.iter().filter_map(expr_to_predicate).collect(),
        });
        Ok(Arc::new(StreamingTableExec::try_new(
            projected_schema,
            vec![partition],
            None,
            [],
            false,
            limit,
        )?))
    }
}

struct AdapterPartition {
    table: &'static QueryTableSchema,
    binding: Binding,
    projection: Vec<usize>,
    schema: SchemaRef,
    limit: Option<usize>,
    predicates: Vec<aql_adapter_api::Predicate>,
}

impl std::fmt::Debug for AdapterPartition {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AdapterPartition")
            .field("table", &self.table.name)
            .field("projection", &self.projection)
            .field("predicates", &self.predicates.len())
            .finish_non_exhaustive()
    }
}

impl PartitionStream for AdapterPartition {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(
        &self,
        context: Arc<datafusion::execution::TaskContext>,
    ) -> SendableRecordBatchStream {
        let reservation =
            MemoryConsumer::new("AQL session reconciliation").register(context.memory_pool());
        let state = AdapterBatchState::new(self, reservation);
        Box::pin(RecordBatchStreamAdapter::new(
            self.schema.clone(),
            stream::unfold(state, |mut state| async move {
                state.next_batch().map(|batch| (batch, state))
            }),
        ))
    }
}

struct AdapterBatchState {
    table: &'static QueryTableSchema,
    table_name: Option<TableName>,
    binding: Binding,
    projection: Vec<usize>,
    requested: Vec<ColumnName>,
    schema: SchemaRef,
    limit: Option<usize>,
    predicates: Vec<aql_adapter_api::Predicate>,
    source_index: usize,
    current: Option<RecordStream>,
    current_diagnostics: Option<ScanDiagnostics>,
    reconciliation_memory: MemoryReservation,
    session_records: Vec<CanonicalRecord>,
    pending_sessions: VecDeque<CanonicalRecord>,
    sessions_reconciled: bool,
    seen: BTreeSet<String>,
    emitted: usize,
    agents_emitted: bool,
    finished: bool,
}

impl AdapterBatchState {
    fn new(partition: &AdapterPartition, reconciliation_memory: MemoryReservation) -> Self {
        let table_name = match partition.table.name {
            "agents" => None,
            "sessions" => Some(TableName::Sessions),
            "messages" => Some(TableName::Messages),
            "tool_calls" => Some(TableName::ToolCalls),
            "usage" => Some(TableName::Usage),
            "session_edges" => Some(TableName::SessionEdges),
            "artifacts" => Some(TableName::Artifacts),
            _ => None,
        };
        Self {
            table: partition.table,
            table_name,
            binding: partition.binding.clone(),
            projection: partition.projection.clone(),
            requested: partition
                .projection
                .iter()
                .map(|index| ColumnName::new(partition.table.columns[*index].name))
                .collect(),
            schema: partition.schema.clone(),
            limit: partition.limit,
            predicates: partition.predicates.clone(),
            source_index: 0,
            current: None,
            current_diagnostics: None,
            reconciliation_memory,
            session_records: Vec::new(),
            pending_sessions: VecDeque::new(),
            sessions_reconciled: false,
            seen: BTreeSet::new(),
            emitted: 0,
            agents_emitted: false,
            finished: false,
        }
    }

    fn next_batch(&mut self) -> Option<Result<RecordBatch>> {
        if self.finished {
            return None;
        }
        if self.table.name == "agents" {
            return self.next_agents_batch();
        }

        let Some(table_name) = self.table_name else {
            self.finished = true;
            return Some(Err(DataFusionError::Plan(
                "logical plan contains an unknown table".to_string(),
            )));
        };
        if table_name == TableName::Sessions {
            return self.next_session_batch();
        }
        self.next_record_batch(table_name)
    }

    fn next_agents_batch(&mut self) -> Option<Result<RecordBatch>> {
        self.finished = true;
        if self.agents_emitted {
            return None;
        }
        self.agents_emitted = true;
        if let Err(error) = self
            .binding
            .options
            .budget
            .charge_records(self.binding.sources.len() as u64)
        {
            return Some(Err(DataFusionError::External(Box::new(error))));
        }
        let manifests = self
            .binding
            .sources
            .iter()
            .map(|source| source.manifest.clone())
            .collect::<Vec<_>>();
        let arrays = self
            .projection
            .iter()
            .map(|index| agent_array(self.table.columns[*index].name, &manifests))
            .collect::<Result<Vec<_>>>();
        Some(arrays.and_then(|arrays| {
            RecordBatch::try_new_with_options(
                self.schema.clone(),
                arrays,
                &RecordBatchOptions::new().with_row_count(Some(self.binding.sources.len())),
            )
            .map_err(Into::into)
        }))
    }

    fn next_record_batch(&mut self, table_name: TableName) -> Option<Result<RecordBatch>> {
        let mut records = Vec::with_capacity(1024);
        while records.len() < 1024 && self.limit.is_none_or(|limit| self.emitted < limit) {
            if self.current.is_none() {
                let remaining = self
                    .limit
                    .map(|limit| limit.saturating_sub(self.emitted) as u64);
                match self.open_next_source(table_name, remaining) {
                    Ok(true) => {}
                    Ok(false) => {
                        self.finished = true;
                        break;
                    }
                    Err(error) => {
                        self.finished = true;
                        return Some(Err(error));
                    }
                }
            }

            match self.current.as_mut().and_then(Iterator::next) {
                Some(Ok(record)) => {
                    if let Err(error) = validate_record_metrics(&record) {
                        self.finished = true;
                        return Some(Err(error));
                    }
                    if !self.seen.insert(record_identity(&record)) {
                        self.finished = true;
                        return Some(Err(DataFusionError::Execution(
                            "adapter produced a duplicate canonical entity".to_string(),
                        )));
                    }
                    records.push(record);
                    self.emitted += 1;
                }
                Some(Err(error)) => {
                    self.finished = true;
                    return Some(Err(DataFusionError::External(Box::new(error))));
                }
                None => {
                    if let Err(error) = self.flush_current_diagnostics() {
                        self.finished = true;
                        return Some(Err(error));
                    }
                    self.current = None;
                    self.source_index += 1;
                }
            }
        }
        if records.is_empty() {
            self.finished = true;
            return None;
        }
        if self.limit.is_some_and(|limit| self.emitted >= limit) {
            if let Err(error) = self.flush_current_diagnostics() {
                self.finished = true;
                return Some(Err(error));
            }
            self.current = None;
            self.finished = true;
        }
        let arrays = self
            .projection
            .iter()
            .map(|index| record_array(self.table, self.table.columns[*index].name, &records))
            .collect::<Result<Vec<_>>>();
        Some(arrays.and_then(|arrays| {
            RecordBatch::try_new_with_options(
                self.schema.clone(),
                arrays,
                &RecordBatchOptions::new().with_row_count(Some(records.len())),
            )
            .map_err(Into::into)
        }))
    }

    fn open_next_source(&mut self, table_name: TableName, limit: Option<u64>) -> Result<bool> {
        loop {
            let Some(source) = self.binding.sources.get(self.source_index) else {
                return Ok(false);
            };
            let capability = table_capability(table_name);
            if !source
                .manifest
                .capabilities
                .iter()
                .any(|candidate| candidate == capability)
            {
                self.source_index += 1;
                continue;
            }
            let predicates = if table_name == TableName::Sessions {
                self.predicates
                    .iter()
                    .filter(|predicate| session_identity_predicate(predicate))
                    .cloned()
                    .collect()
            } else {
                self.predicates.clone()
            };
            let result = source
                .adapter
                .scan(ScanRequest {
                    source: source.manifest.clone(),
                    table: table_name,
                    projection: self.requested.clone(),
                    predicates,
                    limit,
                    order_hint: Vec::new(),
                    access: self.binding.options.access,
                    budget: self.binding.options.budget.clone(),
                    cancellation: self.binding.options.cancellation.clone(),
                    snapshot: source.manifest.snapshot.clone(),
                })
                .map_err(|error| DataFusionError::External(Box::new(error)))?;
            let mut metadata = self.binding.metadata.lock().map_err(|_| {
                DataFusionError::External(Box::new(aql_adapter_api::AdapterError::Internal {
                    stage: "query_metadata".to_string(),
                }))
            })?;
            metadata.scans.push(ScanMetadata {
                table: self.table.name.to_string(),
                source_id: source.manifest.source_id.to_string(),
                predicate_pushdown: result
                    .pushdown
                    .predicates
                    .iter()
                    .map(|state| format!("{state:?}").to_ascii_lowercase())
                    .collect(),
                limit_pushdown: result
                    .pushdown
                    .limit
                    .map(|state| format!("{state:?}").to_ascii_lowercase()),
                ordering_pushdown: result
                    .pushdown
                    .ordering
                    .iter()
                    .map(|state| format!("{state:?}").to_ascii_lowercase())
                    .collect(),
                snapshot_strength: format!("{:?}", result.snapshot.strength).to_ascii_lowercase(),
                stale: result.snapshot.stale,
            });
            drop(metadata);
            self.current_diagnostics = Some(result.diagnostics);
            self.current = Some(result.records);
            return Ok(true);
        }
    }

    fn next_session_batch(&mut self) -> Option<Result<RecordBatch>> {
        if !self.sessions_reconciled
            && let Err(error) = self.reconcile_sessions()
        {
            self.finished = true;
            return Some(Err(error));
        }

        let records = self
            .pending_sessions
            .drain(..self.pending_sessions.len().min(1024))
            .collect::<Vec<_>>();
        if records.is_empty() {
            self.finished = true;
            return None;
        }
        self.emitted += records.len();
        if self.pending_sessions.is_empty() {
            self.finished = true;
        }
        let arrays = self
            .projection
            .iter()
            .map(|index| record_array(self.table, self.table.columns[*index].name, &records))
            .collect::<Result<Vec<_>>>();
        Some(arrays.and_then(|arrays| {
            RecordBatch::try_new_with_options(
                self.schema.clone(),
                arrays,
                &RecordBatchOptions::new().with_row_count(Some(records.len())),
            )
            .map_err(Into::into)
        }))
    }

    fn reconcile_sessions(&mut self) -> Result<()> {
        self.collect_session_records()?;
        let reconciled = Catalog.reconcile_sessions(std::mem::take(&mut self.session_records));
        let mut metadata = self.binding.metadata.lock().map_err(|_| {
            DataFusionError::External(Box::new(aql_adapter_api::AdapterError::Internal {
                stage: "query_metadata".to_string(),
            }))
        })?;
        metadata
            .warnings
            .extend(reconciled.warnings.iter().map(|warning| {
                format!(
                    "catalog:{:?}:entity_id={}:field={}",
                    warning.kind,
                    warning.entity_id,
                    warning.field.as_deref().unwrap_or("none")
                )
            }));
        drop(metadata);

        let mut records = reconciled
            .records
            .into_iter()
            .map(CanonicalRecord::Session)
            .collect::<Vec<_>>();
        if let Some(limit) = self.limit {
            records.truncate(limit);
        }
        self.pending_sessions = records.into();
        self.sessions_reconciled = true;
        Ok(())
    }

    fn collect_session_records(&mut self) -> Result<()> {
        loop {
            if self.current.is_none() && !self.open_next_source(TableName::Sessions, None)? {
                return Ok(());
            }
            match self.current.as_mut().and_then(Iterator::next) {
                Some(Ok(record)) => {
                    validate_record_metrics(&record)?;
                    self.reconciliation_memory
                        .try_grow(retained_session_bytes(&record)?)?;
                    self.session_records.push(record);
                }
                Some(Err(error)) => return Err(DataFusionError::External(Box::new(error))),
                None => {
                    self.flush_current_diagnostics()?;
                    self.current = None;
                    self.source_index += 1;
                }
            }
        }
    }

    fn flush_current_diagnostics(&mut self) -> Result<()> {
        let Some(diagnostics) = self.current_diagnostics.take() else {
            return Ok(());
        };
        let warnings = diagnostics
            .snapshot()
            .map_err(|error| DataFusionError::External(Box::new(error)))?;
        let mut metadata = self.binding.metadata.lock().map_err(|_| {
            DataFusionError::External(Box::new(aql_adapter_api::AdapterError::Internal {
                stage: "query_metadata".to_string(),
            }))
        })?;
        metadata.warnings.extend(warnings.iter().map(|warning| {
            format!(
                "adapter:{:?}:source_kind={}:stage={}",
                warning.kind, warning.source_kind, warning.stage
            )
        }));
        Ok(())
    }
}

fn session_identity_predicate(predicate: &aql_adapter_api::Predicate) -> bool {
    use aql_adapter_api::Predicate;

    match predicate {
        Predicate::Eq(column, _)
        | Predicate::In(column, _)
        | Predicate::Range { column, .. }
        | Predicate::IsNull(column) => column.as_str() == "session_id",
        Predicate::And(predicates) => predicates.iter().all(session_identity_predicate),
        Predicate::Unsupported(_) => false,
    }
}

fn retained_session_bytes(record: &CanonicalRecord) -> Result<usize> {
    let CanonicalRecord::Session(session) = record else {
        return Err(DataFusionError::Execution(
            "sessions adapter produced a non-session record".to_string(),
        ));
    };
    let mut bytes = std::mem::size_of_val(session);
    for value in [
        session.session_id.as_str(),
        session.native_id.as_str(),
        session.source_id.as_str(),
        &session.agent_id,
    ] {
        bytes = bytes.saturating_add(value.len());
    }
    for value in [
        &session.title,
        &session.preview,
        &session.cwd,
        &session.project,
        &session.model,
        &session.provider,
        &session.status,
    ]
    .into_iter()
    .flatten()
    {
        bytes = bytes.saturating_add(value.capacity());
    }
    for (field, provenance) in &session.provenance {
        bytes = bytes
            .saturating_add(std::mem::size_of::<(String, Vec<aql_model::Provenance>)>())
            .saturating_add(3 * std::mem::size_of::<usize>())
            .saturating_add(field.capacity())
            .saturating_add(
                provenance
                    .capacity()
                    .saturating_mul(std::mem::size_of::<aql_model::Provenance>()),
            );
        for item in provenance {
            bytes = bytes
                .saturating_add(item.source_id.as_str().len())
                .saturating_add(item.source_kind.capacity())
                .saturating_add(item.source_locator.capacity())
                .saturating_add(item.source_version.as_ref().map_or(0, String::capacity))
                .saturating_add(item.watermark.as_ref().map_or(0, String::capacity));
        }
    }
    for (key, value) in &session.extensions {
        bytes = bytes
            .saturating_add(std::mem::size_of::<(String, serde_json::Value)>())
            .saturating_add(3 * std::mem::size_of::<usize>())
            .saturating_add(key.capacity())
            .saturating_add(json_retained_bytes(value));
    }
    Ok(bytes)
}

fn json_retained_bytes(value: &serde_json::Value) -> usize {
    match value {
        serde_json::Value::Null | serde_json::Value::Bool(_) | serde_json::Value::Number(_) => 0,
        serde_json::Value::String(value) => value.capacity(),
        serde_json::Value::Array(values) => values
            .capacity()
            .saturating_mul(std::mem::size_of::<serde_json::Value>())
            .saturating_add(
                values
                    .iter()
                    .map(json_retained_bytes)
                    .fold(0, usize::saturating_add),
            ),
        serde_json::Value::Object(values) => values.iter().fold(0, |bytes, (key, value)| {
            bytes
                .saturating_add(std::mem::size_of::<(String, serde_json::Value)>())
                .saturating_add(3 * std::mem::size_of::<usize>())
                .saturating_add(key.capacity())
                .saturating_add(json_retained_bytes(value))
        }),
    }
}

fn table_capability(table: TableName) -> &'static str {
    match table {
        TableName::Sessions => "sessions",
        TableName::Messages => "messages",
        TableName::ToolCalls => "tool_calls",
        TableName::Usage => "usage",
        TableName::SessionEdges => "session_edges",
        TableName::Artifacts => "artifacts",
    }
}

fn record_identity(record: &CanonicalRecord) -> String {
    match record {
        CanonicalRecord::Session(value) => value.session_id.to_string(),
        CanonicalRecord::Message(value) => value.message_id.to_string(),
        CanonicalRecord::ToolCall(value) => value.tool_call_id.to_string(),
        CanonicalRecord::Usage(value) => value.usage_id.to_string(),
        CanonicalRecord::SessionEdge(value) => value.edge_id.to_string(),
        CanonicalRecord::Artifact(value) => value.artifact_id.to_string(),
    }
}

fn validate_record_metrics(record: &CanonicalRecord) -> Result<()> {
    let invalid = match record {
        CanonicalRecord::Session(value) => value.tokens_used.is_some_and(|tokens| tokens < 0),
        CanonicalRecord::Message(value) => {
            let tokens = [value.input_tokens, value.output_tokens, value.cached_tokens];
            tokens.into_iter().flatten().any(|tokens| tokens < 0)
                || tokens
                    .into_iter()
                    .flatten()
                    .try_fold(0_i64, i64::checked_add)
                    .is_none()
        }
        CanonicalRecord::Usage(value) => [
            value.input_tokens,
            value.output_tokens,
            value.cached_tokens,
            value.total_tokens,
        ]
        .into_iter()
        .flatten()
        .chain([
            value.message_count,
            value.tool_call_count,
            value.error_count,
        ])
        .any(|tokens| tokens < 0),
        CanonicalRecord::ToolCall(_)
        | CanonicalRecord::SessionEdge(_)
        | CanonicalRecord::Artifact(_) => false,
    };
    if invalid {
        Err(DataFusionError::Execution(
            "canonical metrics are invalid".to_string(),
        ))
    } else {
        Ok(())
    }
}

fn expr_to_predicate(expr: &Expr) -> Option<aql_adapter_api::Predicate> {
    match expr {
        Expr::BinaryExpr(binary) if binary.op == Operator::And => {
            Some(aql_adapter_api::Predicate::And(vec![
                expr_to_predicate(&binary.left)?,
                expr_to_predicate(&binary.right)?,
            ]))
        }
        Expr::BinaryExpr(binary) if binary.op == Operator::Eq => {
            column_and_literal(&binary.left, &binary.right)
                .or_else(|| column_and_literal(&binary.right, &binary.left))
                .map(|(column, literal)| aql_adapter_api::Predicate::Eq(column, literal))
        }
        Expr::BinaryExpr(binary) if binary.op == Operator::GtEq || binary.op == Operator::LtEq => {
            let (column, literal) = column_and_literal(&binary.left, &binary.right)?;
            Some(aql_adapter_api::Predicate::Range {
                column,
                lower: (binary.op == Operator::GtEq).then_some(literal.clone()),
                upper: (binary.op == Operator::LtEq).then_some(literal),
            })
        }
        Expr::IsNull(inner) => match inner.as_ref() {
            Expr::Column(column) => Some(aql_adapter_api::Predicate::IsNull(ColumnName::new(
                column.name.clone(),
            ))),
            _ => None,
        },
        Expr::InList(list) if !list.negated => {
            let Expr::Column(column) = list.expr.as_ref() else {
                return None;
            };
            let literals = list
                .list
                .iter()
                .map(expr_literal)
                .collect::<Option<Vec<_>>>()?;
            Some(aql_adapter_api::Predicate::In(
                ColumnName::new(column.name.clone()),
                literals,
            ))
        }
        _ => None,
    }
}

fn column_and_literal(
    column: &Expr,
    literal: &Expr,
) -> Option<(ColumnName, aql_adapter_api::Literal)> {
    let Expr::Column(column) = column else {
        return None;
    };
    Some((ColumnName::new(column.name.clone()), expr_literal(literal)?))
}

fn expr_literal(expr: &Expr) -> Option<aql_adapter_api::Literal> {
    let Expr::Literal(value, _) = expr else {
        return None;
    };
    match value {
        ScalarValue::Null => Some(aql_adapter_api::Literal::Null),
        ScalarValue::Boolean(value) => value.map(aql_adapter_api::Literal::Bool),
        ScalarValue::Int64(value) => value.map(aql_adapter_api::Literal::Integer),
        ScalarValue::Utf8(value) | ScalarValue::LargeUtf8(value) => {
            value.clone().map(aql_adapter_api::Literal::Text)
        }
        ScalarValue::TimestampMillisecond(value, _) => value.map(aql_adapter_api::Literal::Integer),
        _ => None,
    }
}

fn agent_array(column: &str, sources: &[SourceManifest]) -> Result<ArrayRef> {
    let values = sources
        .iter()
        .map(|source| match column {
            "source_id" => Ok(source.source_id.to_string()),
            "agent_id" => Ok(source.agent_id.clone()),
            "display_name" => Ok(source.display_name.clone()),
            "format_fingerprint" => Ok(source.format_fingerprint.clone()),
            "snapshot_state" => Ok(if source.snapshot.is_some() {
                "weak"
            } else {
                "unavailable"
            }
            .to_string()),
            "capabilities" => serde_json::to_string(&source.capabilities)
                .map_err(|error| DataFusionError::External(Box::new(error))),
            _ => Err(DataFusionError::Plan("unknown agents column".to_string())),
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(Arc::new(StringArray::from(
        values.into_iter().map(Some).collect::<Vec<_>>(),
    )))
}

fn record_array(
    table: &QueryTableSchema,
    column: &str,
    records: &[CanonicalRecord],
) -> Result<ArrayRef> {
    let data_type = table
        .columns
        .iter()
        .find(|candidate| candidate.name == column)
        .map(|candidate| candidate.data_type)
        .ok_or_else(|| DataFusionError::Plan("unknown canonical column".to_string()))?;
    match data_type {
        QueryDataType::Text | QueryDataType::Json => Ok(Arc::new(StringArray::from(
            records
                .iter()
                .map(|record| record_text(record, column))
                .collect::<Vec<_>>(),
        ))),
        QueryDataType::Int64 => Ok(Arc::new(Int64Array::from(
            records
                .iter()
                .map(|record| record_int(record, column))
                .collect::<Vec<_>>(),
        ))),
        QueryDataType::Bool => Ok(Arc::new(BooleanArray::from(
            records
                .iter()
                .map(|record| record_bool(record, column))
                .collect::<Vec<_>>(),
        ))),
        QueryDataType::Timestamp => Ok(Arc::new(
            TimestampMillisecondArray::from(
                records
                    .iter()
                    .map(|record| record_timestamp(record, column))
                    .collect::<Vec<_>>(),
            )
            .with_timezone("UTC"),
        )),
    }
}

fn record_text(record: &CanonicalRecord, column: &str) -> Option<String> {
    match record {
        CanonicalRecord::Session(value) => match column {
            "session_id" => Some(value.session_id.to_string()),
            "native_id" => Some(value.native_id.to_string()),
            "source_id" => Some(value.source_id.to_string()),
            "agent_id" => Some(value.agent_id.clone()),
            "title" => value.title.clone(),
            "preview" => value.preview.clone(),
            "cwd" => value.cwd.clone(),
            "project" => value.project.clone(),
            "model" => value.model.clone(),
            "provider" => value.provider.clone(),
            "status" => value.status.clone(),
            "identity_confidence" => {
                Some(format!("{:?}", value.identity_confidence).to_ascii_lowercase())
            }
            "snapshot_state" => Some(format!("{:?}", value.snapshot_state).to_ascii_lowercase()),
            _ => None,
        },
        CanonicalRecord::Message(value) => match column {
            "message_id" => Some(value.message_id.to_string()),
            "session_id" => Some(value.session_id.to_string()),
            "source_id" => Some(value.source_id.to_string()),
            "role" => Some(value.role.clone()),
            "kind" => value.kind.clone(),
            "content" => value.content.clone(),
            "content_json" => value.content_json.as_ref().map(ToString::to_string),
            "model" => value.model.clone(),
            _ => None,
        },
        CanonicalRecord::ToolCall(value) => match column {
            "tool_call_id" => Some(value.tool_call_id.to_string()),
            "session_id" => Some(value.session_id.to_string()),
            "message_id" => value.message_id.as_ref().map(ToString::to_string),
            "source_id" => Some(value.source_id.to_string()),
            "tool_name" => Some(value.tool_name.clone()),
            "namespace" => value.namespace.clone(),
            "arguments" => value.arguments.as_ref().map(ToString::to_string),
            "output" => value.output.clone(),
            "status" => value.status.clone(),
            _ => None,
        },
        CanonicalRecord::Usage(value) => match column {
            "usage_id" => Some(value.usage_id.to_string()),
            "source_id" => Some(value.source_id.to_string()),
            "agent_id" => Some(value.agent_id.clone()),
            "session_id" => value.session_id.as_ref().map(ToString::to_string),
            "model" => value.model.clone(),
            "provider" => value.provider.clone(),
            _ => None,
        },
        CanonicalRecord::SessionEdge(value) => match column {
            "edge_id" => Some(value.edge_id.to_string()),
            "source_id" => Some(value.source_id.to_string()),
            "parent_session_id" => Some(value.parent_session_id.to_string()),
            "child_session_id" => Some(value.child_session_id.to_string()),
            "edge_kind" => Some(value.edge_kind.clone()),
            "native_edge_id" => value.native_edge_id.as_ref().map(ToString::to_string),
            _ => None,
        },
        CanonicalRecord::Artifact(value) => match column {
            "artifact_id" => Some(value.artifact_id.to_string()),
            "source_id" => Some(value.source_id.to_string()),
            "session_id" => Some(value.session_id.to_string()),
            "tool_call_id" => value.tool_call_id.as_ref().map(ToString::to_string),
            "kind" => Some(value.kind.clone()),
            "name" => value.name.clone(),
            "path" => value.path.clone(),
            "media_type" => value.media_type.clone(),
            "content" => value.content.clone(),
            "content_json" => value.content_json.as_ref().map(ToString::to_string),
            _ => None,
        },
    }
}

fn record_int(record: &CanonicalRecord, column: &str) -> Option<i64> {
    match record {
        CanonicalRecord::Session(value) => match column {
            "message_count" => value.message_count,
            "tool_call_count" => value.tool_call_count,
            "tokens_used" => value.tokens_used,
            _ => None,
        },
        CanonicalRecord::Message(value) => match column {
            "sequence" => Some(value.sequence),
            "input_tokens" => value.input_tokens,
            "output_tokens" => value.output_tokens,
            "cached_tokens" => value.cached_tokens,
            _ => None,
        },
        CanonicalRecord::ToolCall(value) => match column {
            "sequence" => Some(value.sequence),
            "duration_ms" => value.duration_ms,
            "exit_code" => value.exit_code,
            _ => None,
        },
        CanonicalRecord::Usage(value) => match column {
            "input_tokens" => value.input_tokens,
            "output_tokens" => value.output_tokens,
            "cached_tokens" => value.cached_tokens,
            "total_tokens" => value.total_tokens,
            "message_count" => Some(value.message_count),
            "tool_call_count" => Some(value.tool_call_count),
            "error_count" => Some(value.error_count),
            _ => None,
        },
        CanonicalRecord::Artifact(value) if column == "size_bytes" => value.size_bytes,
        CanonicalRecord::SessionEdge(_) | CanonicalRecord::Artifact(_) => None,
    }
}

fn record_bool(record: &CanonicalRecord, column: &str) -> Option<bool> {
    match record {
        CanonicalRecord::Session(value) if column == "archived" => value.archived,
        CanonicalRecord::Message(value) if column == "is_error" => value.is_error,
        _ => None,
    }
}

fn record_timestamp(record: &CanonicalRecord, column: &str) -> Option<i64> {
    match record {
        CanonicalRecord::Session(value) => match column {
            "created_at" => value.created_at.map(|time| time.timestamp_millis()),
            "updated_at" => value.updated_at.map(|time| time.timestamp_millis()),
            _ => None,
        },
        CanonicalRecord::Message(value) if column == "created_at" => {
            value.created_at.map(|time| time.timestamp_millis())
        }
        CanonicalRecord::ToolCall(value) => match column {
            "started_at" => value.started_at.map(|time| time.timestamp_millis()),
            "ended_at" => value.ended_at.map(|time| time.timestamp_millis()),
            _ => None,
        },
        CanonicalRecord::Usage(value) if column == "bucket_start" => {
            value.bucket_start.map(|time| time.timestamp_millis())
        }
        CanonicalRecord::SessionEdge(value) if column == "created_at" => {
            value.created_at.map(|time| time.timestamp_millis())
        }
        CanonicalRecord::Artifact(value) if column == "created_at" => {
            value.created_at.map(|time| time.timestamp_millis())
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aql_adapter_api::{
        AdapterError, AdapterSchema, Capabilities, ColumnCapability, ProbeRequest, ProbeResult,
        PushdownReport, PushdownState, ScanResult, SnapshotReport, SnapshotStrength,
    };
    use aql_adapter_codex::CodexAdapter;
    use aql_model::{
        EntityId, IdentityConfidence, MessageRecord, NativeId, SessionRecord, SnapshotState,
        SnapshotToken, SourceId, ToolCallRecord,
    };
    use datafusion::arrow::array::{Int64Array, StringArray};
    use datafusion::prelude::{col, lit};

    struct SyntheticSessionAdapter {
        session: SessionRecord,
    }

    struct SyntheticUsageAdapter {
        sessions: Vec<SessionRecord>,
        messages: Vec<MessageRecord>,
        tool_calls: Vec<ToolCallRecord>,
    }

    impl AgentAdapter for SyntheticUsageAdapter {
        fn id(&self) -> &'static str {
            "synthetic-usage"
        }

        fn probe(&self, _request: &ProbeRequest) -> std::result::Result<ProbeResult, AdapterError> {
            unreachable!("engine test binds its manifest directly")
        }

        fn capabilities(&self, _manifest: &SourceManifest) -> Capabilities {
            Capabilities {
                tables: vec![
                    TableName::Sessions,
                    TableName::Messages,
                    TableName::ToolCalls,
                ],
                columns: Vec::new(),
                snapshot_strength: SnapshotStrength::Strong,
            }
        }

        fn schema(&self, _manifest: &SourceManifest) -> AdapterSchema {
            AdapterSchema {
                columns: Vec::new(),
            }
        }

        fn scan(&self, request: ScanRequest) -> std::result::Result<ScanResult, AdapterError> {
            let records: Vec<CanonicalRecord> = match request.table {
                TableName::Sessions => self
                    .sessions
                    .iter()
                    .cloned()
                    .map(CanonicalRecord::Session)
                    .collect(),
                TableName::Messages => self
                    .messages
                    .iter()
                    .cloned()
                    .map(CanonicalRecord::Message)
                    .collect(),
                TableName::ToolCalls => self
                    .tool_calls
                    .iter()
                    .cloned()
                    .map(CanonicalRecord::ToolCall)
                    .collect(),
                TableName::Usage | TableName::SessionEdges | TableName::Artifacts => {
                    return Err(AdapterError::UnsupportedFormat {
                        stage: "synthetic_usage_table".to_string(),
                    });
                }
            };
            Ok(ScanResult {
                records: Box::new(records.into_iter().map(Ok)),
                pushdown: PushdownReport {
                    predicates: request
                        .predicates
                        .iter()
                        .map(|_| PushdownState::Unsupported)
                        .collect(),
                    limit: request.limit.map(|_| PushdownState::Unsupported),
                    ordering: request
                        .order_hint
                        .iter()
                        .map(|_| PushdownState::Unsupported)
                        .collect(),
                },
                diagnostics: ScanDiagnostics::default(),
                snapshot: SnapshotReport {
                    token: request.snapshot,
                    strength: SnapshotStrength::Strong,
                    stale: false,
                },
            })
        }
    }

    impl AgentAdapter for SyntheticSessionAdapter {
        fn id(&self) -> &'static str {
            "synthetic"
        }

        fn probe(&self, _request: &ProbeRequest) -> std::result::Result<ProbeResult, AdapterError> {
            unreachable!("federated engine tests bind manifests directly")
        }

        fn capabilities(&self, _manifest: &SourceManifest) -> Capabilities {
            Capabilities {
                tables: vec![TableName::Sessions],
                columns: self.schema(_manifest).columns,
                snapshot_strength: SnapshotStrength::Strong,
            }
        }

        fn schema(&self, _manifest: &SourceManifest) -> AdapterSchema {
            AdapterSchema {
                columns: vec![
                    ColumnCapability {
                        name: ColumnName::new("session_id"),
                        access: AccessClass::Safe,
                    },
                    ColumnCapability {
                        name: ColumnName::new("title"),
                        access: AccessClass::Content,
                    },
                ],
            }
        }

        fn scan(&self, request: ScanRequest) -> std::result::Result<ScanResult, AdapterError> {
            assert_eq!(request.table, TableName::Sessions);
            let diagnostics = ScanDiagnostics::default();
            let records = request
                .predicates
                .iter()
                .all(|predicate| synthetic_session_matches(&self.session, predicate))
                .then(|| CanonicalRecord::Session(self.session.clone()))
                .into_iter()
                .map(Ok);
            Ok(ScanResult {
                records: Box::new(records),
                pushdown: PushdownReport {
                    predicates: request
                        .predicates
                        .iter()
                        .map(|_| PushdownState::Unsupported)
                        .collect(),
                    limit: request.limit.map(|_| PushdownState::Exact),
                    ordering: request
                        .order_hint
                        .iter()
                        .map(|_| PushdownState::Unsupported)
                        .collect(),
                },
                diagnostics,
                snapshot: SnapshotReport {
                    token: request.snapshot,
                    strength: SnapshotStrength::Strong,
                    stale: false,
                },
            })
        }
    }

    fn synthetic_session_matches(
        session: &SessionRecord,
        predicate: &aql_adapter_api::Predicate,
    ) -> bool {
        use aql_adapter_api::{Literal, Predicate};

        match predicate {
            Predicate::Eq(column, Literal::Text(value)) if column.as_str() == "session_id" => {
                session.session_id.as_str() == value
            }
            Predicate::Eq(column, Literal::Text(value)) if column.as_str() == "title" => {
                session.title.as_deref() == Some(value)
            }
            Predicate::And(predicates) => predicates
                .iter()
                .all(|predicate| synthetic_session_matches(session, predicate)),
            _ => true,
        }
    }

    fn synthetic_session(source_id: &str, native_id: &str) -> SessionRecord {
        let source_id = SourceId::new(source_id);
        let native_id = NativeId::new(native_id);
        SessionRecord {
            session_id: EntityId::from_parts("synthetic", &source_id, &native_id),
            native_id,
            source_id,
            agent_id: "synthetic".to_string(),
            title: None,
            preview: None,
            cwd: None,
            project: None,
            model: None,
            provider: None,
            created_at: None,
            updated_at: None,
            status: None,
            archived: None,
            message_count: None,
            tool_call_count: None,
            tokens_used: None,
            identity_confidence: IdentityConfidence::Exact,
            snapshot_state: SnapshotState::Consistent,
            provenance: BTreeMap::new(),
            extensions: BTreeMap::new(),
        }
    }

    fn synthetic_message(
        source_id: &str,
        session_id: &EntityId,
        suffix: &str,
        input_tokens: Option<i64>,
        output_tokens: Option<i64>,
        cached_tokens: Option<i64>,
        is_error: Option<bool>,
    ) -> MessageRecord {
        MessageRecord {
            message_id: EntityId::new(format!("{session_id}:message-{suffix}")),
            session_id: session_id.clone(),
            source_id: SourceId::new(source_id),
            sequence: 1,
            role: "assistant".to_string(),
            kind: Some("message".to_string()),
            content: None,
            content_json: None,
            model: Some("synthetic-model".to_string()),
            created_at: None,
            input_tokens,
            output_tokens,
            cached_tokens,
            is_error,
            provenance: BTreeMap::new(),
            extensions: BTreeMap::new(),
        }
    }

    fn synthetic_tool_call(source_id: &str, session_id: &EntityId) -> ToolCallRecord {
        ToolCallRecord {
            tool_call_id: EntityId::new(format!("{session_id}:tool-1")),
            session_id: session_id.clone(),
            message_id: None,
            source_id: SourceId::new(source_id),
            sequence: 1,
            tool_name: "synthetic_tool".to_string(),
            namespace: None,
            arguments: None,
            output: None,
            status: Some("error".to_string()),
            started_at: None,
            ended_at: None,
            duration_ms: None,
            exit_code: Some(1),
            provenance: BTreeMap::new(),
            extensions: BTreeMap::new(),
        }
    }

    #[tokio::test]
    async fn sensitive_identifiers_are_rejected_before_engine_execution() {
        let sql = validate_read_only_sql("SELECT title FROM sessions").expect("valid SQL");
        let error = prepare_query(&sql, QueryOptions::default())
            .await
            .expect_err("title must require content access");
        assert!(matches!(error, QueryError::AccessDenied("content")));
    }

    #[tokio::test]
    async fn agents_query_combines_manifests_bound_to_different_adapters() {
        let source = |source_id: &str, agent_id: &str| SourceManifest {
            source_id: SourceId::new(source_id),
            agent_id: agent_id.to_string(),
            display_name: agent_id.to_string(),
            data_root_token: format!("root:{source_id}"),
            format_fingerprint: format!("{agent_id}-fixture-v1"),
            capabilities: vec!["sessions".to_string()],
            snapshot: Some(SnapshotToken::new("synthetic-snapshot")),
            warnings: Vec::new(),
        };
        let sql = validate_read_only_sql("SELECT agent_id FROM agents ORDER BY agent_id")
            .expect("valid agents query");
        let result = prepare_query(&sql, QueryOptions::default())
            .await
            .expect("query prepares")
            .execute(vec![
                FederatedSource {
                    adapter: Arc::new(CodexAdapter::new(b"adapter-a".to_vec())),
                    manifest: source("source-a", "codex"),
                },
                FederatedSource {
                    adapter: Arc::new(CodexAdapter::new(b"adapter-b".to_vec())),
                    manifest: source("source-b", "claude-code"),
                },
            ])
            .await
            .expect("federated agents query succeeds");

        let values = result
            .batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("agent_id is a string")
                    .iter()
                    .flatten()
            })
            .collect::<Vec<_>>();
        assert_eq!(values, vec!["claude-code", "codex"]);
    }

    #[tokio::test]
    async fn sessions_query_consumes_each_sources_bound_adapter() {
        let manifest = |source_id: &str| SourceManifest {
            source_id: SourceId::new(source_id),
            agent_id: "synthetic".to_string(),
            display_name: "Synthetic".to_string(),
            data_root_token: format!("root:{source_id}"),
            format_fingerprint: "synthetic-v1".to_string(),
            capabilities: vec!["sessions".to_string()],
            snapshot: Some(SnapshotToken::new("synthetic-snapshot")),
            warnings: Vec::new(),
        };
        let sql = validate_read_only_sql("SELECT session_id FROM sessions ORDER BY session_id")
            .expect("valid sessions query");
        let result = prepare_query(&sql, QueryOptions::default())
            .await
            .expect("query prepares")
            .execute(vec![
                FederatedSource {
                    adapter: Arc::new(SyntheticSessionAdapter {
                        session: synthetic_session("source-a", "session-a"),
                    }),
                    manifest: manifest("source-a"),
                },
                FederatedSource {
                    adapter: Arc::new(SyntheticSessionAdapter {
                        session: synthetic_session("source-b", "session-b"),
                    }),
                    manifest: manifest("source-b"),
                },
            ])
            .await
            .expect("federated sessions query succeeds");

        assert_eq!(result.metadata.scans.len(), 2);
        assert_eq!(
            result
                .batches
                .iter()
                .map(RecordBatch::num_rows)
                .sum::<usize>(),
            2
        );
    }

    #[tokio::test]
    async fn federated_sessions_are_reconciled_before_publication() {
        let manifest = |source_id: &str| SourceManifest {
            source_id: SourceId::new(source_id),
            agent_id: "synthetic".to_string(),
            display_name: "Synthetic".to_string(),
            data_root_token: format!("root:{source_id}"),
            format_fingerprint: "synthetic-v1".to_string(),
            capabilities: vec!["sessions".to_string()],
            snapshot: Some(SnapshotToken::new("synthetic-snapshot")),
            warnings: Vec::new(),
        };
        let mut first = synthetic_session("shared-source", "shared-session");
        first.title = Some("Synthetic first title".to_string());
        let mut second = first.clone();
        second.title = Some("Synthetic conflicting title".to_string());
        let sql = validate_read_only_sql("SELECT title FROM sessions").expect("valid query");
        let result = prepare_query(
            &sql,
            QueryOptions {
                access: AccessGrant {
                    content: true,
                    ..AccessGrant::default()
                },
                ..QueryOptions::default()
            },
        )
        .await
        .expect("query prepares")
        .execute(vec![
            FederatedSource {
                adapter: Arc::new(SyntheticSessionAdapter { session: first }),
                manifest: manifest("source-a"),
            },
            FederatedSource {
                adapter: Arc::new(SyntheticSessionAdapter { session: second }),
                manifest: manifest("source-b"),
            },
        ])
        .await
        .expect("matching sessions reconcile");

        assert_eq!(
            result
                .batches
                .iter()
                .map(RecordBatch::num_rows)
                .sum::<usize>(),
            1
        );
        assert_eq!(
            result.batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("title is a string")
                .value(0),
            "Synthetic first title"
        );
        assert!(
            result
                .metadata
                .warnings
                .iter()
                .any(|warning| warning.contains("catalog:FieldConflict"))
        );
    }

    #[tokio::test]
    async fn federated_session_predicates_run_after_reconciliation() {
        let manifest = |source_id: &str| SourceManifest {
            source_id: SourceId::new(source_id),
            agent_id: "synthetic".to_string(),
            display_name: "Synthetic".to_string(),
            data_root_token: format!("root:{source_id}"),
            format_fingerprint: "synthetic-v1".to_string(),
            capabilities: vec!["sessions".to_string()],
            snapshot: Some(SnapshotToken::new("synthetic-snapshot")),
            warnings: Vec::new(),
        };
        let mut authoritative = synthetic_session("shared-source", "shared-session");
        authoritative.title = Some("Synthetic authoritative title".to_string());
        let mut conflicting = authoritative.clone();
        conflicting.title = Some("Synthetic matching title".to_string());
        let sql = validate_read_only_sql(
            "SELECT session_id FROM sessions WHERE title = 'Synthetic matching title'",
        )
        .expect("valid query");
        let result = prepare_query(
            &sql,
            QueryOptions {
                access: AccessGrant {
                    content: true,
                    ..AccessGrant::default()
                },
                ..QueryOptions::default()
            },
        )
        .await
        .expect("query prepares")
        .execute(vec![
            FederatedSource {
                adapter: Arc::new(SyntheticSessionAdapter {
                    session: authoritative,
                }),
                manifest: manifest("source-a"),
            },
            FederatedSource {
                adapter: Arc::new(SyntheticSessionAdapter {
                    session: conflicting,
                }),
                manifest: manifest("source-b"),
            },
        ])
        .await
        .expect("query executes");

        assert_eq!(
            result
                .batches
                .iter()
                .map(RecordBatch::num_rows)
                .sum::<usize>(),
            0
        );
        assert!(
            result
                .metadata
                .scans
                .iter()
                .all(|scan| scan.predicate_pushdown.is_empty())
        );
    }

    #[tokio::test]
    async fn session_reconciliation_obeys_query_memory_budget() {
        let mut session = synthetic_session("memory-source", "memory-session");
        session.title = Some("x".repeat(64 * 1024));
        let manifest = SourceManifest {
            source_id: SourceId::new("memory-source"),
            agent_id: "synthetic".to_string(),
            display_name: "Synthetic".to_string(),
            data_root_token: "root:memory-source".to_string(),
            format_fingerprint: "synthetic-v1".to_string(),
            capabilities: vec!["sessions".to_string()],
            snapshot: Some(SnapshotToken::new("synthetic-snapshot")),
            warnings: Vec::new(),
        };
        let sql = validate_read_only_sql("SELECT title FROM sessions").expect("valid query");
        let result = prepare_query(
            &sql,
            QueryOptions {
                access: AccessGrant {
                    content: true,
                    ..AccessGrant::default()
                },
                max_memory_bytes: 32 * 1024,
                ..QueryOptions::default()
            },
        )
        .await
        .expect("query prepares")
        .execute(vec![FederatedSource {
            adapter: Arc::new(SyntheticSessionAdapter { session }),
            manifest,
        }])
        .await;
        let Err(error) = result else {
            panic!("reconciliation must respect the query memory pool");
        };

        assert!(error.to_string().contains("Resources exhausted"));
    }

    #[tokio::test]
    async fn count_star_preserves_rows_for_zero_column_adapter_projection() {
        let manifest = SourceManifest {
            source_id: SourceId::new("source-count"),
            agent_id: "synthetic".to_string(),
            display_name: "Synthetic".to_string(),
            data_root_token: "root:count".to_string(),
            format_fingerprint: "synthetic-v1".to_string(),
            capabilities: vec!["sessions".to_string()],
            snapshot: Some(SnapshotToken::new("synthetic-snapshot")),
            warnings: Vec::new(),
        };
        let sql = validate_read_only_sql("SELECT COUNT(*) AS sessions FROM sessions")
            .expect("valid count query");
        let result = prepare_query(&sql, QueryOptions::default())
            .await
            .expect("query prepares")
            .execute(vec![FederatedSource {
                adapter: Arc::new(SyntheticSessionAdapter {
                    session: synthetic_session("source-count", "session-count"),
                }),
                manifest,
            }])
            .await
            .expect("count star succeeds");
        let counts = result.batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("count is int64");
        assert_eq!(counts.value(0), 1);
    }

    #[tokio::test]
    async fn streaming_metadata_requires_consuming_the_stream_to_eof() {
        let source = FederatedSource {
            adapter: Arc::new(SyntheticSessionAdapter {
                session: synthetic_session("source-stream", "session-stream"),
            }),
            manifest: SourceManifest {
                source_id: SourceId::new("source-stream"),
                agent_id: "synthetic".to_string(),
                display_name: "Synthetic".to_string(),
                data_root_token: "root:stream".to_string(),
                format_fingerprint: "synthetic-v1".to_string(),
                capabilities: vec!["sessions".to_string()],
                snapshot: Some(SnapshotToken::new("synthetic-snapshot")),
                warnings: Vec::new(),
            },
        };
        let sql = validate_read_only_sql("SELECT session_id FROM sessions")
            .expect("valid streaming query");

        let unfinished = prepare_query(&sql, QueryOptions::default())
            .await
            .expect("query prepares")
            .execute_stream(vec![source.clone()])
            .await
            .expect("stream starts");
        assert!(matches!(
            unfinished.metadata.finish(),
            Err(QueryError::SqlRejected {
                stage: "metadata",
                ..
            })
        ));

        let StreamingQueryResult {
            mut stream,
            metadata,
        } = prepare_query(&sql, QueryOptions::default())
            .await
            .expect("query prepares")
            .execute_stream(vec![source])
            .await
            .expect("stream starts");
        let first = stream
            .next()
            .await
            .expect("stream yields a batch")
            .expect("batch succeeds");
        assert_eq!(first.num_rows(), 1);
        assert!(stream.next().await.is_none());
        let metadata = metadata.finish().expect("EOF finalizes metadata");
        assert_eq!(metadata.source_ids, vec!["source-stream"]);
    }

    #[tokio::test]
    async fn privacy_functions_are_deterministic_and_do_not_return_original_values() {
        let mut session = synthetic_session("source-private", "session-private");
        session.title = Some("Synthetic private title".to_string());
        session.cwd = Some("/workspace/example/project".to_string());
        let adapter = Arc::new(SyntheticSessionAdapter { session });
        let manifest = SourceManifest {
            source_id: SourceId::new("source-private"),
            agent_id: "synthetic".to_string(),
            display_name: "Synthetic".to_string(),
            data_root_token: "root:private".to_string(),
            format_fingerprint: "synthetic-v1".to_string(),
            capabilities: vec!["sessions".to_string()],
            snapshot: None,
            warnings: Vec::new(),
        };
        let sql = validate_read_only_sql(
            "SELECT REDACT(title, 'hash') AS hash_one, REDACT(title, 'hash') AS hash_two, REDACT(title, 'last4') AS tail, MASK_PATH(cwd, 2) AS masked_cwd, REDACT(CAST(NULL AS VARCHAR)) AS null_value FROM sessions",
        )
        .expect("valid privacy function query");
        let options = QueryOptions {
            access: AccessGrant {
                path: true,
                content: true,
                ..AccessGrant::default()
            },
            redaction_salt: b"synthetic-redaction-salt".to_vec(),
            ..QueryOptions::default()
        };
        let result = prepare_query(&sql, options)
            .await
            .expect("query prepares")
            .execute(vec![FederatedSource { adapter, manifest }])
            .await
            .expect("privacy functions execute");
        let batch = &result.batches[0];
        let value = |column: usize| {
            batch
                .column(column)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("privacy output is text")
                .value(0)
        };
        assert!(value(0).starts_with("hmac:"));
        assert!(!value(0).contains("Synthetic"));
        assert_eq!(value(0), value(1));
        assert_eq!(value(2), "[REDACTED]…itle");
        assert_eq!(value(3), "…/example/project");
        assert!(batch.column(4).is_null(0));
    }

    #[tokio::test]
    async fn usage_view_preserves_null_tokens_and_aggregates_explicit_metrics() {
        let session = synthetic_session("source-usage", "session-usage");
        let session_id = session.session_id.clone();
        let adapter = Arc::new(SyntheticUsageAdapter {
            sessions: vec![session],
            messages: vec![
                synthetic_message(
                    "source-usage",
                    &session_id,
                    "known",
                    Some(10),
                    Some(20),
                    Some(5),
                    Some(false),
                ),
                synthetic_message(
                    "source-usage",
                    &session_id,
                    "unknown",
                    None,
                    None,
                    None,
                    Some(true),
                ),
            ],
            tool_calls: vec![synthetic_tool_call("source-usage", &session_id)],
        });
        let manifest = SourceManifest {
            source_id: SourceId::new("source-usage"),
            agent_id: "synthetic".to_string(),
            display_name: "Synthetic".to_string(),
            data_root_token: "root:usage".to_string(),
            format_fingerprint: "synthetic-v1".to_string(),
            capabilities: vec![
                "sessions".to_string(),
                "messages".to_string(),
                "tool_calls".to_string(),
            ],
            snapshot: Some(SnapshotToken::new("synthetic-snapshot")),
            warnings: Vec::new(),
        };
        let sql = validate_read_only_sql(
            "SELECT SUM(input_tokens), SUM(total_tokens), SUM(message_count), SUM(tool_call_count), SUM(error_count) FROM usage",
        )
        .expect("valid usage aggregate");
        let result = prepare_query(&sql, QueryOptions::default())
            .await
            .expect("usage query prepares")
            .execute(vec![FederatedSource { adapter, manifest }])
            .await
            .expect("usage aggregate succeeds");
        let batch = &result.batches[0];
        let value = |column: usize| {
            batch
                .column(column)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("aggregate is int64")
                .value(0)
        };
        assert_eq!(value(0), 10);
        assert_eq!(value(1), 35);
        assert_eq!(value(2), 2);
        assert_eq!(value(3), 1);
        assert_eq!(value(4), 2);
    }

    #[tokio::test]
    async fn negative_or_overflowing_message_tokens_fail_closed() {
        for tokens in [(Some(-1), None, None), (Some(i64::MAX), Some(1), None)] {
            let session = synthetic_session("source-invalid", "session-invalid");
            let session_id = session.session_id.clone();
            let adapter = Arc::new(SyntheticUsageAdapter {
                sessions: vec![session],
                messages: vec![synthetic_message(
                    "source-invalid",
                    &session_id,
                    "invalid",
                    tokens.0,
                    tokens.1,
                    tokens.2,
                    None,
                )],
                tool_calls: Vec::new(),
            });
            let manifest = SourceManifest {
                source_id: SourceId::new("source-invalid"),
                agent_id: "synthetic".to_string(),
                display_name: "Synthetic".to_string(),
                data_root_token: "root:invalid".to_string(),
                format_fingerprint: "synthetic-v1".to_string(),
                capabilities: vec!["sessions".to_string(), "messages".to_string()],
                snapshot: None,
                warnings: Vec::new(),
            };
            let sql = validate_read_only_sql("SELECT total_tokens FROM usage")
                .expect("valid usage query");
            let result = prepare_query(&sql, QueryOptions::default())
                .await
                .expect("query prepares")
                .execute(vec![FederatedSource { adapter, manifest }])
                .await;
            let Err(error) = result else {
                panic!("invalid token metrics must fail closed");
            };
            assert!(matches!(error, QueryError::Engine(_)));
        }
    }

    #[tokio::test]
    async fn plan_access_does_not_match_aliases_or_substrings() {
        let safe_alias =
            validate_read_only_sql("SELECT session_id AS content, model AS output FROM sessions")
                .expect("valid SQL");
        prepare_query(&safe_alias, QueryOptions::default())
            .await
            .expect("safe aliases are not sensitive columns");

        let output_tokens =
            validate_read_only_sql("SELECT output_tokens FROM messages").expect("valid SQL");
        prepare_query(&output_tokens, QueryOptions::default())
            .await
            .expect("output_tokens is not tool output");
    }

    #[tokio::test]
    async fn plan_access_covers_hidden_and_derived_references() {
        for (sql, grant) in [
            (
                "SELECT session_id FROM sessions WHERE title LIKE '%synthetic%'",
                "content",
            ),
            ("SELECT session_id FROM sessions ORDER BY cwd", "path"),
            (
                "WITH private AS (SELECT content AS x FROM messages) SELECT x FROM private",
                "content",
            ),
            ("SELECT COUNT(arguments) FROM tool_calls", "tool-input"),
            (
                "SELECT session_id FROM tool_calls WHERE output IS NOT NULL",
                "tool-output",
            ),
            (
                "SELECT sessions.session_id FROM sessions JOIN messages ON sessions.session_id = messages.session_id AND messages.content IS NOT NULL",
                "content",
            ),
        ] {
            let validated = validate_read_only_sql(sql).expect("valid SQL");
            let error = prepare_query(&validated, QueryOptions::default())
                .await
                .expect_err("sensitive source lineage must require a grant");
            assert!(matches!(error, QueryError::AccessDenied(actual) if actual == grant));
        }

        for (sql, grant) in [
            ("SELECT REDACT(content) FROM messages", "content"),
            ("SELECT MASK_PATH(cwd, 2) FROM sessions", "path"),
            (
                "SELECT session_id FROM messages WHERE REDACT(content) = '[REDACTED]'",
                "content",
            ),
        ] {
            let validated = validate_read_only_sql(sql).expect("valid privacy function query");
            let error = prepare_query(&validated, QueryOptions::default())
                .await
                .expect_err("privacy functions must preserve input lineage access");
            assert!(matches!(error, QueryError::AccessDenied(actual) if actual == grant));
        }
    }

    #[tokio::test]
    async fn safe_wildcards_exclude_sensitive_columns() {
        let sessions = validate_read_only_sql("SELECT * FROM sessions").expect("valid wildcard");
        let normalized = sessions.normalized_sql();
        assert!(normalized.contains("sessions.session_id"));
        assert!(!normalized.contains("sessions.title"));
        assert!(!normalized.contains("sessions.cwd"));
        prepare_query(&sessions, QueryOptions::default())
            .await
            .expect("safe wildcard must not require grants");

        let qualified =
            validate_read_only_sql("SELECT s.* FROM sessions AS s").expect("valid wildcard");
        assert!(qualified.normalized_sql().contains("s.session_id"));
        assert!(!qualified.normalized_sql().contains("s.title"));

        let cte = validate_read_only_sql(
            "WITH mixed AS (SELECT session_id, title AS private_title FROM sessions) SELECT * FROM mixed",
        )
        .expect("valid CTE wildcard");
        let normalized = cte.normalized_sql();
        assert!(normalized.contains("mixed.session_id"));
        assert!(!normalized.contains("mixed.private_title"));
        prepare_query(&cte, QueryOptions::default())
            .await
            .expect("CTE wildcard must omit sensitive lineage");

        let count = validate_read_only_sql("SELECT COUNT(*) FROM sessions").expect("valid count");
        assert!(count.normalized_sql().contains("COUNT(*)"));
        prepare_query(&count, QueryOptions::default())
            .await
            .expect("COUNT(*) must not read sensitive fields");

        let artifacts =
            validate_read_only_sql("SELECT * FROM artifacts").expect("valid artifacts wildcard");
        let normalized = artifacts.normalized_sql();
        assert!(normalized.contains("artifacts.artifact_id"));
        assert!(!normalized.contains("artifacts.name"));
        assert!(!normalized.contains("artifacts.path"));
        assert!(!normalized.contains("artifacts.content"));
        let error = prepare_query(&artifacts, QueryOptions::default())
            .await
            .expect_err("artifact enumeration requires path access");
        assert!(matches!(error, QueryError::AccessDenied("path")));
        let prepared = prepare_query(
            &artifacts,
            QueryOptions {
                access: AccessGrant {
                    path: true,
                    ..AccessGrant::default()
                },
                ..QueryOptions::default()
            },
        )
        .await
        .expect("path grant allows artifact metadata wildcard");
        assert!(
            prepared
                .plan_summary()
                .required_access
                .contains(&"path".to_string())
        );

        for (sql, grant, path_granted) in [
            ("SELECT path FROM artifacts", "path", false),
            ("SELECT name FROM artifacts", "content", true),
            ("SELECT content_json FROM artifacts", "content", true),
        ] {
            let sql = validate_read_only_sql(sql).expect("valid artifact query");
            let error = prepare_query(
                &sql,
                QueryOptions {
                    access: AccessGrant {
                        path: path_granted,
                        ..AccessGrant::default()
                    },
                    ..QueryOptions::default()
                },
            )
            .await
            .expect_err("sensitive artifact field requires a grant");
            assert!(matches!(error, QueryError::AccessDenied(actual) if actual == grant));
        }
    }

    #[test]
    fn wildcard_modifiers_are_rejected() {
        assert!(matches!(
            validate_read_only_sql("SELECT * EXCLUDE (title) FROM sessions"),
            Err(QueryError::SqlRejected {
                stage: "wildcard",
                ..
            })
        ));
    }

    #[test]
    fn filter_translation_is_conservative() {
        assert!(matches!(
            expr_to_predicate(&col("native_id").eq(lit("session-minimal"))),
            Some(aql_adapter_api::Predicate::Eq(column, aql_adapter_api::Literal::Text(value)))
                if column.as_str() == "native_id" && value == "session-minimal"
        ));
        assert!(matches!(
            expr_to_predicate(&col("updated_at").gt_eq(lit(100_i64))),
            Some(aql_adapter_api::Predicate::Range {
                lower: Some(aql_adapter_api::Literal::Integer(100)),
                ..
            })
        ));
        assert!(expr_to_predicate(&col("model").like(lit("example%"))).is_none());

        let provider = DeferredTable::new(&QUERY_SCHEMAS[1]);
        let supported = col("native_id").eq(lit("session-minimal"));
        let unsupported = col("model").like(lit("example%"));
        assert_eq!(
            provider
                .supports_filters_pushdown(&[&supported, &unsupported])
                .expect("pushdown declaration must succeed"),
            vec![
                TableProviderFilterPushDown::Inexact,
                TableProviderFilterPushDown::Unsupported,
            ]
        );
    }

    #[test]
    fn read_only_firewall_accepts_select_and_cte() {
        validate_read_only_sql("SELECT session_id FROM sessions")
            .expect("canonical SELECT must be accepted");
        validate_read_only_sql(
            "WITH recent AS (SELECT session_id FROM sessions) SELECT session_id FROM recent",
        )
        .expect("CTE over a canonical table must be accepted");
    }

    #[test]
    fn read_only_firewall_rejects_writes_multiple_statements_and_external_tables() {
        for sql in [
            "DELETE FROM sessions",
            "SELECT session_id FROM sessions; DELETE FROM sessions",
            "SELECT * FROM information_schema.tables",
            "SELECT * FROM read_csv('fixture.csv')",
            "SELECT * FROM unknown_table",
        ] {
            assert!(
                matches!(
                    validate_read_only_sql(sql),
                    Err(QueryError::SqlRejected { .. })
                ),
                "query should be rejected"
            );
        }
    }

    #[test]
    fn read_only_firewall_rejects_disallowed_functions_and_complexity() {
        assert!(matches!(
            validate_read_only_sql("SELECT dangerous(session_id) FROM sessions"),
            Err(QueryError::SqlRejected { .. })
        ));
        let oversized = "x".repeat(MAX_SQL_BYTES + 1);
        assert!(matches!(
            validate_read_only_sql(&oversized),
            Err(QueryError::SqlRejected { .. })
        ));
        for sql in [
            "SELECT REDACT(title, 'unknown') FROM sessions",
            "SELECT REDACT(title, model) FROM sessions",
            "SELECT MASK_PATH(cwd, 0) FROM sessions",
            "SELECT MASK_PATH(cwd, 17) FROM sessions",
            "SELECT MASK_PATH(cwd, tokens_used) FROM sessions",
        ] {
            assert!(matches!(
                validate_read_only_sql(sql),
                Err(QueryError::SqlRejected { .. })
            ));
        }
    }

    #[test]
    fn static_query_schemas_cover_phase_one_and_phase_three_tables() {
        assert_eq!(
            QUERY_SCHEMAS
                .iter()
                .map(|schema| schema.name)
                .collect::<Vec<_>>(),
            vec![
                "agents",
                "sessions",
                "messages",
                "tool_calls",
                "usage",
                "session_edges",
                "artifacts",
            ]
        );
        let secret_columns = QUERY_SCHEMAS
            .iter()
            .flat_map(|schema| schema.columns)
            .filter(|column| column.access == AccessClass::Secret)
            .count();
        assert_eq!(secret_columns, 0);
        let artifacts = QUERY_SCHEMAS
            .iter()
            .find(|schema| schema.name == "artifacts")
            .expect("artifacts schema exists");
        assert_eq!(
            artifacts
                .columns
                .iter()
                .find(|column| column.name == "path")
                .expect("path column exists")
                .access,
            AccessClass::Path
        );
        for column in ["name", "content", "content_json"] {
            assert_eq!(
                artifacts
                    .columns
                    .iter()
                    .find(|candidate| candidate.name == column)
                    .expect("content column exists")
                    .access,
                AccessClass::Content
            );
        }
    }
}
