//! Read-only canonical SQL planning and execution for AQL.
//!
//! The engine validates one SELECT/CTE query, rewrites safe wildcards,
//! authorizes canonical columns before adapters read sensitive data, binds
//! explicitly probed sources, and executes under shared resource limits with
//! transactional result publication delegated to the caller.

#![deny(missing_docs)]

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque},
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
use datafusion::execution::{SessionState, SessionStateBuilder, SessionStateDefaults};
use datafusion::logical_expr::{
    AggregateUDF, ColumnarValue, Expr, LogicalPlan, Operator, ScalarFunctionArgs, ScalarUDF,
    ScalarUDFImpl, Signature, TableProviderFilterPushDown, TableType, TypeSignature, Volatility,
    logical_plan,
};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::streaming::{PartitionStream, StreamingTableExec};
use datafusion::physical_plan::{ExecutionPlan, SendableRecordBatchStream};
use datafusion::prelude::{SessionConfig, SessionContext};
use futures::{StreamExt, TryStreamExt, stream};
use sqlparser::ast::{
    Expr as SqlExpr, FunctionArg, FunctionArgExpr, FunctionArguments, Ident, LimitClause,
    ObjectName, OrderByKind, Query, Select, SelectItem, SelectItemQualifiedWildcardKind, SetExpr,
    Statement, TableFactor, Value, Visit, Visitor,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

mod arrays;
mod execution;
mod metadata;
mod sql_firewall;

use arrays::{agents_arrays, record_arrays};
#[cfg(test)]
use execution::expr_to_predicate;
use execution::{Binding, DeferredTable};
pub use sql_firewall::{
    QueryError, SqlParameter, SqlStage, ValidatedSql, bind_sql_parameters, validate_read_only_sql,
};
use sql_firewall::{required_grant, sql_rejected};

/// Maximum accepted SQL input size shared by the engine and the CLI.
pub const MAX_SQL_BYTES: usize = 64 * 1024;
/// Arrow field metadata key marking a canonical JSON column.
pub const JSON_TYPE_METADATA_KEY: &str = "aql.type";
/// Arrow field metadata value marking a canonical JSON column.
pub const JSON_TYPE_METADATA_VALUE: &str = "json";
const MAX_QUERY_DEPTH: usize = 64;
const MAX_EXPRESSIONS: usize = 256;
const MAX_CTES: usize = 32;
const MAX_JOINS: usize = 16;
const QUERY_MEMORY_BYTES: usize = 256 * 1024 * 1024;

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
const ALLOWED_FUNCTIONS: [&str; 22] = [
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
    "replace",
    "date_part",
    "redact",
    "mask_path",
];

/// Logical data types exposed by AQL's canonical query schema.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QueryDataType {
    /// UTF-8 text.
    Text,
    /// Signed 64-bit integer.
    Int64,
    /// Boolean value.
    Bool,
    /// UTC timestamp with millisecond precision.
    Timestamp,
    /// JSON value serialized through Arrow's text representation.
    Json,
}

/// Public metadata for one canonical query column.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct QueryColumn {
    /// Stable SQL column name.
    pub name: &'static str,
    /// Logical column type.
    pub data_type: QueryDataType,
    /// Whether SQL `NULL` is permitted.
    pub nullable: bool,
    /// Grant required before the source value may be read.
    pub access: AccessClass,
    /// Expected scan or computation cost.
    pub cost: FieldCost,
}

/// Public schema for one canonical or AQL metadata table.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct QueryTableSchema {
    /// Stable SQL table name.
    pub name: &'static str,
    /// Ordered public columns.
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

const AGENTS_QUERY_COLUMNS: &[QueryColumn] = &[
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

const AQL_TABLES_COLUMNS: &[QueryColumn] = &[
    query_column(
        "table_name",
        QueryDataType::Text,
        false,
        AccessClass::Safe,
        FieldCost::Metadata,
    ),
    query_column(
        "table_kind",
        QueryDataType::Text,
        false,
        AccessClass::Safe,
        FieldCost::Metadata,
    ),
];
const AQL_COLUMNS_COLUMNS: &[QueryColumn] = &[
    query_column(
        "table_name",
        QueryDataType::Text,
        false,
        AccessClass::Safe,
        FieldCost::Metadata,
    ),
    query_column(
        "column_name",
        QueryDataType::Text,
        false,
        AccessClass::Safe,
        FieldCost::Metadata,
    ),
    query_column(
        "ordinal_position",
        QueryDataType::Int64,
        false,
        AccessClass::Safe,
        FieldCost::Metadata,
    ),
    query_column(
        "data_type",
        QueryDataType::Text,
        false,
        AccessClass::Safe,
        FieldCost::Metadata,
    ),
    query_column(
        "nullable",
        QueryDataType::Bool,
        false,
        AccessClass::Safe,
        FieldCost::Metadata,
    ),
    query_column(
        "access_class",
        QueryDataType::Text,
        false,
        AccessClass::Safe,
        FieldCost::Metadata,
    ),
];
const AQL_SOURCES_COLUMNS: &[QueryColumn] = &[
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
];
const AQL_CAPABILITIES_COLUMNS: &[QueryColumn] = &[
    query_column(
        "source_id",
        QueryDataType::Text,
        false,
        AccessClass::Safe,
        FieldCost::Metadata,
    ),
    query_column(
        "table_name",
        QueryDataType::Text,
        false,
        AccessClass::Safe,
        FieldCost::Metadata,
    ),
    query_column(
        "supported",
        QueryDataType::Bool,
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

/// Canonical schemas accepted by the SQL firewall and registered in DataFusion.
pub const QUERY_SCHEMAS: &[QueryTableSchema] = &[
    QueryTableSchema {
        name: "aql_tables",
        columns: AQL_TABLES_COLUMNS,
    },
    QueryTableSchema {
        name: "aql_columns",
        columns: AQL_COLUMNS_COLUMNS,
    },
    QueryTableSchema {
        name: "aql_sources",
        columns: AQL_SOURCES_COLUMNS,
    },
    QueryTableSchema {
        name: "aql_capabilities",
        columns: AQL_CAPABILITIES_COLUMNS,
    },
    QueryTableSchema {
        name: "agents",
        columns: AGENTS_QUERY_COLUMNS,
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

/// Per-query authorization, budget, cancellation, memory, and redaction settings.
#[derive(Clone)]
pub struct QueryOptions {
    /// Session grants available to projection authorization.
    pub access: AccessGrant,
    /// Shared source and output resource budget.
    pub budget: ResourceBudget,
    /// Shared cooperative cancellation token.
    pub cancellation: CancellationToken,
    /// Maximum in-memory bytes available to DataFusion.
    pub max_memory_bytes: usize,
    /// Installation-local salt used by privacy functions.
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

/// Authorized logical query ready to bind to explicitly probed sources.
pub struct PreparedQuery {
    context: SessionContext,
    plan: LogicalPlan,
    providers: Vec<Arc<DeferredTable>>,
    options: QueryOptions,
    metadata: Arc<Mutex<QueryMetadata>>,
    plan_summary: PlanSummary,
}

/// Query-wide diagnostics and final resource usage.
#[derive(Clone, Debug, Default)]
pub struct QueryMetadata {
    /// Bound installation-scoped source IDs.
    pub source_ids: Vec<String>,
    /// Sanitized warnings emitted by probes, scans, and reconciliation.
    pub warnings: Vec<String>,
    /// Per-table, per-source scan diagnostics.
    pub scans: Vec<ScanMetadata>,
    /// Canonical records consumed after stream completion.
    pub records_scanned: u64,
    /// Source bytes read after stream completion.
    pub bytes_read: u64,
    /// Output bytes charged by the renderer.
    pub output_bytes: u64,
}

/// Diagnostics for one adapter scan participating in a query.
#[derive(Clone, Debug)]
pub struct ScanMetadata {
    /// Canonical table scanned.
    pub table: String,
    /// Installation-scoped source ID.
    pub source_id: String,
    /// Accuracy reported for each predicate pushdown.
    pub predicate_pushdown: Vec<String>,
    /// Accuracy reported for limit pushdown.
    pub limit_pushdown: Option<String>,
    /// Accuracy reported for each ordering pushdown.
    pub ordering_pushdown: Vec<String>,
    /// Snapshot consistency reported by the adapter.
    pub snapshot_strength: String,
    /// Whether the adapter reported a stale snapshot.
    pub stale: bool,
}

/// Explain-style summary produced before any source is bound or opened.
#[derive(Clone, Debug, Default)]
pub struct PlanSummary {
    /// Canonical tables referenced by the plan.
    pub tables: Vec<String>,
    /// Canonical columns referenced directly or through expressions.
    pub columns: Vec<String>,
    /// Access classes required by the plan.
    pub required_access: Vec<String>,
    /// Human-readable reasons each access class is required.
    pub access_reasons: Vec<String>,
    /// Planner hints that may be offered to adapters.
    pub pushdown: Vec<String>,
    /// Shared record limit.
    pub max_records: u64,
    /// Shared source-byte limit.
    pub max_bytes_read: u64,
    /// Shared output-byte limit.
    pub max_output_bytes: u64,
    /// DataFusion memory limit.
    pub max_memory_bytes: usize,
}

/// Fully materialized query batches and finalized metadata.
pub struct QueryResult {
    /// Arrow record batches in query order.
    pub batches: Vec<RecordBatch>,
    /// Metadata finalized after consuming the complete stream.
    pub metadata: QueryMetadata,
}

/// Streaming query output and a handle that finalizes metadata at EOF.
pub struct StreamingQueryResult {
    /// Arrow record-batch stream.
    pub stream: SendableRecordBatchStream,
    /// Metadata handle that succeeds only after `stream` reaches EOF.
    pub metadata: QueryMetadataHandle,
}

/// Finalizes resource usage and diagnostics after a stream reaches EOF.
pub struct QueryMetadataHandle {
    metadata: Arc<Mutex<QueryMetadata>>,
    options: QueryOptions,
    stream_complete: Arc<AtomicBool>,
}

impl QueryMetadataHandle {
    /// Returns finalized metadata after the associated stream was fully consumed.
    ///
    /// Returns [`QueryError::StreamLifecycle`] if a caller attempts to publish
    /// metadata for a partial stream.
    pub fn finish(self) -> std::result::Result<QueryMetadata, QueryError> {
        if !self.stream_complete.load(Ordering::Acquire) {
            return Err(QueryError::StreamLifecycle {
                reason: "query stream must be consumed to completion before metadata is finalized",
            });
        }
        let mut metadata = self
            .metadata
            .lock()
            .map_err(|_| QueryError::SqlRejected {
                stage: SqlStage::Metadata,
                reason: "query metadata is unavailable",
            })?
            .clone();
        metadata.records_scanned = self.options.budget.records_used();
        metadata.bytes_read = self.options.budget.bytes_read_used();
        metadata.output_bytes = self.options.budget.output_bytes_used();
        Ok(metadata)
    }
}

/// One adapter permanently bound to the manifest it produced during probing.
#[derive(Clone)]
pub struct FederatedSource {
    /// Adapter that produced and owns the manifest.
    pub adapter: Arc<dyn AgentAdapter>,
    /// Manifest to scan through `adapter`.
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
    /// Returns the authorized, source-free plan summary.
    #[must_use]
    pub fn plan_summary(&self) -> &PlanSummary {
        &self.plan_summary
    }

    /// Executes the query and materializes all Arrow batches.
    ///
    /// No partial result is returned if binding, scanning, execution, or stream
    /// finalization fails.
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

    /// Executes the query as a lazy Arrow stream.
    ///
    /// Every supplied manifest must remain paired with the adapter that probed
    /// it. Metadata can be finalized only after the returned stream reaches EOF.
    pub async fn execute_stream(
        self,
        sources: Vec<FederatedSource>,
    ) -> std::result::Result<StreamingQueryResult, QueryError> {
        if sources.is_empty() {
            return Err(sql_rejected(
                SqlStage::Bind,
                "probe returned no compatible source",
            ));
        }
        {
            let mut metadata = self.metadata.lock().map_err(|_| QueryError::SqlRejected {
                stage: SqlStage::Metadata,
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

fn aql_scalar_functions() -> Vec<Arc<ScalarUDF>> {
    vec![
        datafusion::functions::core::coalesce(),
        datafusion::functions::core::nullif(),
        datafusion::functions::datetime::date_part(),
        datafusion::functions::datetime::date_trunc(),
        datafusion::functions::math::abs(),
        datafusion::functions::math::round(),
        datafusion::functions::string::btrim(),
        datafusion::functions::string::concat(),
        datafusion::functions::string::lower(),
        datafusion::functions::string::replace(),
        datafusion::functions::string::upper(),
        datafusion::functions::unicode::character_length(),
        datafusion::functions::unicode::substr(),
    ]
}

fn aql_aggregate_functions() -> Vec<Arc<AggregateUDF>> {
    vec![
        datafusion::functions_aggregate::average::avg_udaf(),
        datafusion::functions_aggregate::count::count_udaf(),
        datafusion::functions_aggregate::min_max::max_udaf(),
        datafusion::functions_aggregate::min_max::min_udaf(),
        datafusion::functions_aggregate::sum::sum_udaf(),
    ]
}

fn aql_session_state(options: &QueryOptions) -> Result<SessionState> {
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_pool(Arc::new(GreedyMemoryPool::new(options.max_memory_bytes)))
        .with_disk_manager_builder(
            DiskManagerBuilder::default().with_mode(DiskManagerMode::Disabled),
        )
        .build()?;
    let mut scalar_functions = aql_scalar_functions();
    scalar_functions.push(Arc::new(ScalarUDF::new_from_impl(RedactUdf::new(
        options.redaction_salt.clone(),
    ))));
    scalar_functions.push(Arc::new(ScalarUDF::new_from_impl(MaskPathUdf::new())));
    Ok(SessionStateBuilder::new()
        .with_config(SessionConfig::new())
        .with_runtime_env(Arc::new(runtime))
        .with_expr_planners(SessionStateDefaults::default_expr_planners())
        .with_scalar_functions(scalar_functions)
        .with_aggregate_functions(aql_aggregate_functions())
        .build())
}

fn aql_session_context(options: &QueryOptions) -> Result<SessionContext> {
    Ok(SessionContext::new_with_state(aql_session_state(options)?))
}

/// Reusable engine session for interactive query loops.
///
/// The expensive DataFusion session state (function registry, optimizer
/// configuration, runtime environment) is built once and shared by every
/// query prepared through this handle. Each [`QuerySession::prepare_query`]
/// call still creates a fresh `SessionContext` and fresh deferred table
/// providers, so explicitly probed sources are rebound per query exactly as
/// in the one-shot [`prepare_query`] path.
///
/// The memory pool is session-scoped: interactive shells execute statements
/// serially and DataFusion releases every reservation when the query stream
/// is dropped, so each query still starts with the full `max_memory_bytes`
/// budget. The pool size and the redaction salt are baked into the shared
/// state at construction; per-query options must repeat the same values.
pub struct QuerySession {
    state: SessionState,
    max_memory_bytes: usize,
    redaction_salt: Vec<u8>,
}

impl QuerySession {
    /// Builds the shared session state reused by one interactive query loop.
    pub fn new(options: &QueryOptions) -> std::result::Result<Self, QueryError> {
        if options.max_memory_bytes == 0 {
            return Err(sql_rejected(
                SqlStage::Budget,
                "query memory budget must be greater than zero",
            ));
        }
        Ok(Self {
            state: aql_session_state(options)?,
            max_memory_bytes: options.max_memory_bytes,
            redaction_salt: options.redaction_salt.clone(),
        })
    }

    /// Validates, authorizes, and plans one query on the shared session
    /// state, with the same per-query source binding contract as
    /// [`prepare_query`].
    ///
    /// `max_memory_bytes` and `redaction_salt` are baked into the shared
    /// state, so per-query options must repeat the values the session was
    /// built with.
    pub async fn prepare_query(
        &self,
        sql: &ValidatedSql,
        options: QueryOptions,
    ) -> std::result::Result<PreparedQuery, QueryError> {
        if options.max_memory_bytes != self.max_memory_bytes
            || options.redaction_salt != self.redaction_salt
        {
            return Err(sql_rejected(
                SqlStage::Budget,
                "memory budget and redaction salt are fixed for the session lifetime",
            ));
        }
        let context = SessionContext::new_with_state(self.state.clone());
        prepare_query_with_context(context, sql, options).await
    }
}

/// Validates, authorizes, and plans one query without opening Agent sources.
///
/// The returned [`PreparedQuery`] can be inspected through
/// [`PreparedQuery::plan_summary`] before explicitly probed sources are bound.
pub async fn prepare_query(
    sql: &ValidatedSql,
    options: QueryOptions,
) -> std::result::Result<PreparedQuery, QueryError> {
    if options.max_memory_bytes == 0 {
        return Err(sql_rejected(
            SqlStage::Budget,
            "query memory budget must be greater than zero",
        ));
    }
    let context = aql_session_context(&options)?;
    prepare_query_with_context(context, sql, options).await
}

async fn prepare_query_with_context(
    context: SessionContext,
    sql: &ValidatedSql,
    options: QueryOptions,
) -> std::result::Result<PreparedQuery, QueryError> {
    let mut providers = Vec::new();
    for query_schema in QUERY_SCHEMAS.iter().filter(|schema| schema.name != "usage") {
        let provider = Arc::new(DeferredTable::new(query_schema));
        register_query_table(&context, query_schema.name, provider.clone())?;
        providers.push(provider);
    }
    let usage_schema = QUERY_SCHEMAS
        .iter()
        .find(|schema| schema.name == "usage")
        .ok_or_else(|| DataFusionError::Plan("usage schema is unavailable".to_string()))?;
    let usage_provider = Arc::new(DeferredTable::new(usage_schema));
    register_query_table(&context, "_aql_usage_records", usage_provider.clone())?;
    providers.push(usage_provider);
    // The usage view plan resolves its table providers at creation time, so
    // it captures this query's providers and must be rebuilt per query; it
    // cannot be cached on a shared session state.
    let usage_plan = context.state().create_logical_plan(USAGE_VIEW_SQL).await?;
    register_query_table(
        &context,
        "usage",
        Arc::new(ViewTable::new(usage_plan, None)),
    )?;
    let plan = context
        .state()
        .create_logical_plan(&sql.normalized_sql())
        .await?;
    let plan = context.state().optimize(&plan)?;
    validate_plan_access(&plan, options.access)?;
    let plan_summary = summarize_plan(&plan, &options)?;
    let metadata = Arc::new(Mutex::new(QueryMetadata::default()));
    if plan_contains_pagination(&plan)? && !plan_contains_ordering(&plan)? {
        metadata
            .lock()
            .map_err(|_| DataFusionError::Execution("query metadata is unavailable".into()))?
            .warnings
            .push("result ordering is unspecified; add ORDER BY for stable pagination".to_string());
    }
    Ok(PreparedQuery {
        context,
        plan,
        providers,
        options,
        metadata,
        plan_summary,
    })
}

/// Registers one per-query provider, replacing any provider a reused session
/// catalog still holds under the same name from a previous query.
fn register_query_table(
    context: &SessionContext,
    name: &str,
    provider: Arc<dyn TableProvider>,
) -> Result<()> {
    context.deregister_table(name)?;
    context.register_table(name, provider)?;
    Ok(())
}

fn plan_contains_pagination(plan: &LogicalPlan) -> Result<bool> {
    let mut found = false;
    plan.apply(|node| {
        if let LogicalPlan::Limit(limit) = node
            && limit_requires_stable_ordering(limit)
        {
            found = true;
            return Ok(TreeNodeRecursion::Stop);
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    Ok(found)
}

/// A single-row `LIMIT 1` sample is not pagination and needs no stable
/// ordering; pagination starts at a positive `OFFSET` or a larger fetch. The
/// optimizer normalizes a missing offset to the literal `0`, which is not an
/// offset.
fn limit_requires_stable_ordering(limit: &logical_plan::Limit) -> bool {
    let has_offset = match limit.skip.as_deref() {
        Some(Expr::Literal(ScalarValue::Int64(Some(skip)), _)) => *skip > 0,
        Some(Expr::Literal(ScalarValue::UInt64(Some(skip)), _)) => *skip > 0,
        // Any other skip cannot be proven to be no offset.
        Some(_) => true,
        None => false,
    };
    if has_offset {
        return true;
    }
    match limit.fetch.as_deref() {
        Some(Expr::Literal(ScalarValue::Int64(Some(fetch)), _)) => *fetch > 1,
        Some(Expr::Literal(ScalarValue::UInt64(Some(fetch)), _)) => *fetch > 1,
        // Any other fetch cannot be proven to be a single-row sample.
        Some(_) => true,
        None => false,
    }
}

fn plan_contains_ordering(plan: &LogicalPlan) -> Result<bool> {
    let mut found = false;
    plan.apply(|node| {
        if matches!(node, LogicalPlan::Sort(_)) {
            found = true;
            Ok(TreeNodeRecursion::Stop)
        } else {
            Ok(TreeNodeRecursion::Continue)
        }
    })?;
    Ok(found)
}

fn summarize_plan(plan: &LogicalPlan, options: &QueryOptions) -> Result<PlanSummary> {
    let mut tables = BTreeSet::new();
    let mut columns = BTreeSet::new();
    let mut required_access = BTreeSet::new();
    let mut access_reasons = BTreeSet::new();
    let mut pushdown = BTreeSet::new();
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
            pushdown.insert(format!(
                "{table_name}:predicates={},limit={}",
                if scan.filters.is_empty() {
                    "none"
                } else {
                    "conservative"
                },
                if scan.fetch.is_some() {
                    "candidate"
                } else {
                    "none"
                },
            ));
            if table_name == "artifacts" {
                required_access.insert("path".to_string());
                access_reasons.insert("artifacts:<table>:path".to_string());
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
                        required_access.insert(required_grant(&column.access).to_string());
                        access_reasons.insert(format!(
                            "{table_name}.{}:{}",
                            field.name(),
                            required_grant(&column.access)
                        ));
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
        access_reasons: access_reasons.into_iter().collect(),
        pushdown: pushdown.into_iter().collect(),
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
                denied = Some(AccessClass::Path);
                return Ok(TreeNodeRecursion::Stop);
            }
            for field in scan.projected_schema.fields() {
                if let Some(column) = query_schema
                    .columns
                    .iter()
                    .find(|column| column.name.eq_ignore_ascii_case(field.name()))
                    && !access.allows(column.access)
                {
                    denied = Some(column.access);
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

/// Maps the adapter-declared snapshot strength to the public `snapshot_state`
/// column value. The mapping stays conservative: a source that declares no
/// snapshot guarantee is reported as `unavailable`, never as `weak`/`strong`.
fn declared_snapshot_state(strength: aql_adapter_api::SnapshotStrength) -> &'static str {
    match strength {
        aql_adapter_api::SnapshotStrength::None => "unavailable",
        aql_adapter_api::SnapshotStrength::Weak => "weak",
        aql_adapter_api::SnapshotStrength::Strong => "strong",
    }
}

#[cfg(test)]
mod tests;
