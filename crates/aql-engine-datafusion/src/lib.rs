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

mod arrays;
mod execution;
mod sql_firewall;

use arrays::{agent_array, record_array};
#[cfg(test)]
use execution::expr_to_predicate;
use execution::{Binding, DeferredTable};
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

#[cfg(test)]
mod tests;
