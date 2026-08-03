use super::*;

pub(super) fn error_hint(error: &(dyn std::error::Error + 'static)) -> Option<String> {
    if let Some(cli) = error.downcast_ref::<CliError>() {
        if cli.kind == CliErrorKind::SourceUnavailable {
            return Some(
                "run `aql database discover`, then `aql doctor -d <database>`".to_string(),
            );
        }
        return match cli.hint {
            CliErrorHint::None => None,
            CliErrorHint::DatabaseSelection => {
                Some("run `aql database list`, then select one with `-d <database>`".to_string())
            }
            CliErrorHint::SqlInput => {
                Some("pass SQL directly, with `--file query.aql`, or with `--stdin`".to_string())
            }
            CliErrorHint::SchemaList => {
                Some("run `aql schema --list` to list canonical tables".to_string())
            }
            CliErrorHint::ExamplesList => {
                Some("run `aql examples --list` to list curated examples".to_string())
            }
        };
    }
    if let Some(config) = error.downcast_ref::<ConfigError>()
        && matches!(config, ConfigError::InvalidOwnershipMarker)
    {
        return Some(
            "the AQL config root is an AQL-owned directory; after confirming it is empty or holds no AQL data, delete it and AQL will recreate it"
                .to_string(),
        );
    }
    if let Some(aql_engine_datafusion::QueryError::SqlRejected { stage, .. }) =
        error.downcast_ref::<aql_engine_datafusion::QueryError>()
    {
        return match stage {
            aql_engine_datafusion::SqlStage::Parse => {
                Some("check the SQL syntax and pass exactly one query".to_string())
            }
            aql_engine_datafusion::SqlStage::Parameters => Some(
                "bind every :name once with --param NAME=VALUE and remove unused parameters"
                    .to_string(),
            ),
            aql_engine_datafusion::SqlStage::Allowlist => Some(
                "use one read-only canonical SELECT, SHOW TABLES, DESCRIBE or EXPLAIN query"
                    .to_string(),
            ),
            aql_engine_datafusion::SqlStage::Wildcard => Some(
                "select explicit canonical columns when wildcard scope is ambiguous".to_string(),
            ),
            aql_engine_datafusion::SqlStage::Complexity => {
                Some("reduce the number of CTEs in the query".to_string())
            }
            aql_engine_datafusion::SqlStage::Bind => {
                Some("run `aql doctor -d <database>` to check the selected sources".to_string())
            }
            aql_engine_datafusion::SqlStage::Budget => {
                Some("set AQL_MAX_MEMORY_BYTES to a positive byte size".to_string())
            }
            // Internal query metadata state is not user-actionable.
            aql_engine_datafusion::SqlStage::Metadata => None,
        };
    }
    if let Some(aql_engine_datafusion::QueryError::AccessDenied(access)) =
        error.downcast_ref::<aql_engine_datafusion::QueryError>()
    {
        return match access {
            AccessClass::Content => Some(access_retry_hint("content", "Content")),
            AccessClass::Path => Some(access_retry_hint("path", "Path")),
            AccessClass::ToolInput => Some(access_retry_hint("tool-input", "tool input")),
            AccessClass::ToolOutput => Some(access_retry_hint("tool-output", "tool output")),
            AccessClass::Safe | AccessClass::Secret => Some(
                "run `aql schema <table>` and add only the required temporary access grant"
                    .to_string(),
            ),
        };
    }
    if let Some(aql_engine_datafusion::QueryError::Engine(engine)) =
        error.downcast_ref::<aql_engine_datafusion::QueryError>()
        && matches!(
            datafusion_adapter_error(engine),
            Some(aql_adapter_api::AdapterError::AccessDenied { .. })
        )
    {
        return Some(
            "run `aql schema <table>` and add only the required temporary access grant".to_string(),
        );
    }
    if matches!(
        error.downcast_ref::<aql_adapter_api::AdapterError>(),
        Some(aql_adapter_api::AdapterError::AccessDenied { .. })
    ) {
        return Some(
            "run `aql schema <table>` and add only the required temporary access grant".to_string(),
        );
    }
    None
}

fn access_retry_hint(access: &str, label: &str) -> String {
    if io::stderr().is_terminal()
        && let Some(command) = retry_query_command(access)
    {
        return format!("retry only if {label} is genuinely needed: `{command}`");
    }
    format!("retry with `--access {access}` only when the query genuinely needs {label}")
}

fn retry_query_command(access: &str) -> Option<String> {
    let mut arguments = std::env::args().skip(1).collect::<Vec<_>>();
    let query_index = arguments.iter().position(|argument| argument == "query")?;
    if arguments
        .windows(2)
        .any(|pair| pair == ["--access", access])
    {
        return None;
    }
    arguments.splice(
        query_index + 1..query_index + 1,
        ["--access".to_string(), access.to_string()],
    );
    Some(
        std::iter::once("aql".to_string())
            .chain(arguments)
            .map(|argument| shell_quote(&argument))
            .collect::<Vec<_>>()
            .join(" "),
    )
}

pub(super) fn shell_quote(value: &str) -> String {
    if !value.is_empty()
        && value.bytes().all(|byte| {
            byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.' | b'/' | b'=' | b':')
        })
    {
        value.to_string()
    } else {
        format!("'{}'", value.replace('\'', "'\"'\"'"))
    }
}

pub(super) fn render_error(error: &(dyn std::error::Error + 'static), format: ErrorFormat) {
    let category = error_category(error);
    let exit_code = error_exit_code(error);
    let hint = error_hint(error);
    let stage = error_stage(error);
    let location = error_location(error);
    match format {
        ErrorFormat::Text => {
            eprintln!("error_category={category}");
            eprintln!("error_stage={stage}");
            if let Some((line, column)) = location {
                eprintln!("error_location=line:{line},column:{column}");
            }
            eprintln!("error={error}");
            if let Some(hint) = hint {
                eprintln!("hint={hint}");
            }
        }
        ErrorFormat::Json => {
            eprintln!(
                "{}",
                serde_json::json!({
                    "category": category,
                    "stage": stage,
                    "message": error.to_string(),
                    "hint": hint,
                    "location": location.map(|(line, column)| serde_json::json!({
                        "line": line,
                        "column": column,
                    })),
                    "exit_code": exit_code,
                })
            );
        }
    }
}

pub(super) fn error_stage(error: &(dyn std::error::Error + 'static)) -> &'static str {
    if let Some(cli) = error.downcast_ref::<CliError>() {
        return cli.stage;
    }
    if let Some(query) = error.downcast_ref::<aql_engine_datafusion::QueryError>() {
        return match query {
            aql_engine_datafusion::QueryError::SqlRejected { stage, .. } => stage.as_str(),
            aql_engine_datafusion::QueryError::StreamLifecycle { .. } => "metadata",
            aql_engine_datafusion::QueryError::AccessDenied(_) => "authorize",
            aql_engine_datafusion::QueryError::Engine(_) => "execute",
        };
    }
    if error
        .downcast_ref::<aql_adapter_api::AdapterError>()
        .is_some()
    {
        return "source";
    }
    if error.downcast_ref::<ConfigError>().is_some() {
        return "config";
    }
    "internal"
}

pub(super) fn error_location(error: &(dyn std::error::Error + 'static)) -> Option<(u64, u64)> {
    error
        .downcast_ref::<CliError>()
        .and_then(|error| error.location)
}

pub(super) fn error_exit_code(error: &(dyn std::error::Error + 'static)) -> i32 {
    if let Some(cli) = error.downcast_ref::<CliError>() {
        return match cli.kind {
            CliErrorKind::InvalidRequest => 2,
            CliErrorKind::NotFound
            | CliErrorKind::SourceUnavailable
            | CliErrorKind::Unsupported
            | CliErrorKind::AlreadyExists
            | CliErrorKind::StateIntegrity
            | CliErrorKind::StateUnavailable => 4,
            CliErrorKind::DeadlineExceeded => 5,
            CliErrorKind::Cancelled => 130,
        };
    }
    if let Some(query) = error.downcast_ref::<aql_engine_datafusion::QueryError>() {
        return match query {
            aql_engine_datafusion::QueryError::SqlRejected { .. }
            | aql_engine_datafusion::QueryError::StreamLifecycle { .. } => 2,
            aql_engine_datafusion::QueryError::AccessDenied(_) => 3,
            aql_engine_datafusion::QueryError::Engine(engine)
                if datafusion_resource_limited(engine) =>
            {
                5
            }
            aql_engine_datafusion::QueryError::Engine(engine) => {
                datafusion_adapter_error(engine).map_or(1, adapter_error_exit_code)
            }
        };
    }
    if let Some(adapter) = error.downcast_ref::<aql_adapter_api::AdapterError>() {
        return adapter_error_exit_code(adapter);
    }
    if let Some(config) = error.downcast_ref::<ConfigError>() {
        return match config {
            ConfigError::InvalidDatabaseName | ConfigError::InvalidMember => 2,
            ConfigError::Missing
            | ConfigError::UnsafeRoot
            | ConfigError::RootOverlap
            | ConfigError::InvalidOwnershipMarker
            | ConfigError::UnknownFile
            | ConfigError::InvalidConfig
            | ConfigError::UnsupportedSchema
            | ConfigError::DatabaseExists
            | ConfigError::DatabaseMissing
            | ConfigError::LockHeld
            | ConfigError::StateChanged
            | ConfigError::Io(_) => 4,
        };
    }
    1
}

pub(super) fn error_category(error: &(dyn std::error::Error + 'static)) -> &'static str {
    if let Some(cli) = error.downcast_ref::<CliError>() {
        return match cli.kind {
            CliErrorKind::InvalidRequest => "invalid_request",
            CliErrorKind::NotFound => "not_found",
            CliErrorKind::SourceUnavailable => "source_unavailable",
            CliErrorKind::DeadlineExceeded => "deadline_exceeded",
            CliErrorKind::Cancelled => "cancelled",
            CliErrorKind::Unsupported => "unsupported",
            CliErrorKind::AlreadyExists => "already_exists",
            CliErrorKind::StateIntegrity => "state_integrity",
            CliErrorKind::StateUnavailable => "state_unavailable",
        };
    }
    if let Some(query) = error.downcast_ref::<aql_engine_datafusion::QueryError>() {
        return match query {
            aql_engine_datafusion::QueryError::SqlRejected { .. }
            | aql_engine_datafusion::QueryError::StreamLifecycle { .. } => "invalid_request",
            aql_engine_datafusion::QueryError::AccessDenied(_) => "access_denied",
            aql_engine_datafusion::QueryError::Engine(engine)
                if datafusion_resource_limited(engine) =>
            {
                "resource_limit"
            }
            aql_engine_datafusion::QueryError::Engine(engine) => {
                datafusion_adapter_error(engine).map_or("execution_failed", adapter_error_category)
            }
        };
    }
    if let Some(adapter) = error.downcast_ref::<aql_adapter_api::AdapterError>() {
        return adapter_error_category(adapter);
    }
    if let Some(config) = error.downcast_ref::<ConfigError>() {
        return match config {
            ConfigError::InvalidDatabaseName | ConfigError::InvalidMember => "invalid_request",
            ConfigError::DatabaseExists => "already_exists",
            ConfigError::DatabaseMissing | ConfigError::Missing => "not_found",
            ConfigError::LockHeld => "concurrent_writer",
            ConfigError::UnsafeRoot
            | ConfigError::RootOverlap
            | ConfigError::InvalidOwnershipMarker
            | ConfigError::UnknownFile
            | ConfigError::InvalidConfig
            | ConfigError::UnsupportedSchema
            | ConfigError::StateChanged => "state_integrity",
            ConfigError::Io(_) => "state_unavailable",
        };
    }
    "internal"
}

fn datafusion_resource_limited(error: &datafusion::error::DataFusionError) -> bool {
    use datafusion::error::DataFusionError;

    match error {
        DataFusionError::ResourcesExhausted(_) => true,
        DataFusionError::External(error) => error
            .downcast_ref::<aql_adapter_api::AdapterError>()
            .is_some_and(|error| {
                matches!(error, aql_adapter_api::AdapterError::BudgetExceeded { .. })
            }),
        DataFusionError::Context(_, error) | DataFusionError::Diagnostic(_, error) => {
            datafusion_resource_limited(error)
        }
        DataFusionError::Collection(errors) => errors.iter().any(datafusion_resource_limited),
        DataFusionError::Shared(error) => datafusion_resource_limited(error),
        _ => false,
    }
}

fn datafusion_adapter_error(
    error: &datafusion::error::DataFusionError,
) -> Option<&aql_adapter_api::AdapterError> {
    use datafusion::error::DataFusionError;

    match error {
        DataFusionError::External(error) => error.downcast_ref::<aql_adapter_api::AdapterError>(),
        DataFusionError::Context(_, error) | DataFusionError::Diagnostic(_, error) => {
            datafusion_adapter_error(error)
        }
        DataFusionError::Collection(errors) => errors.iter().find_map(datafusion_adapter_error),
        DataFusionError::Shared(error) => datafusion_adapter_error(error),
        _ => None,
    }
}

fn adapter_error_exit_code(error: &aql_adapter_api::AdapterError) -> i32 {
    match error {
        aql_adapter_api::AdapterError::AccessDenied { .. } => 3,
        aql_adapter_api::AdapterError::BudgetExceeded { .. }
        | aql_adapter_api::AdapterError::Cancelled => 5,
        aql_adapter_api::AdapterError::NotFound { .. }
        | aql_adapter_api::AdapterError::PermissionDenied { .. }
        | aql_adapter_api::AdapterError::UnsupportedFormat { .. }
        | aql_adapter_api::AdapterError::CorruptSource { .. }
        | aql_adapter_api::AdapterError::SnapshotUnavailable => 4,
        aql_adapter_api::AdapterError::Internal { .. } => 1,
    }
}

fn adapter_error_category(error: &aql_adapter_api::AdapterError) -> &'static str {
    match error {
        aql_adapter_api::AdapterError::AccessDenied { .. } => "access_denied",
        aql_adapter_api::AdapterError::BudgetExceeded { .. } => "resource_limit",
        aql_adapter_api::AdapterError::Cancelled => "cancelled",
        aql_adapter_api::AdapterError::NotFound { .. }
        | aql_adapter_api::AdapterError::PermissionDenied { .. }
        | aql_adapter_api::AdapterError::UnsupportedFormat { .. }
        | aql_adapter_api::AdapterError::CorruptSource { .. }
        | aql_adapter_api::AdapterError::SnapshotUnavailable => "source_unavailable",
        aql_adapter_api::AdapterError::Internal { .. } => "internal",
    }
}
