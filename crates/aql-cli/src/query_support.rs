use super::*;

pub(super) fn access_grant(values: &[Access]) -> AccessGrant {
    let mut grant = AccessGrant::default();
    for value in values {
        match value {
            Access::Path => grant.path = true,
            Access::Content => grant.content = true,
            Access::ToolInput => grant.tool_input = true,
            Access::ToolOutput => grant.tool_output = true,
        }
    }
    grant
}

pub(super) fn execution_budget(
    max_records: u64,
    max_bytes_read: u64,
    max_output_bytes: u64,
    max_single_value_bytes: u64,
    timeout: Duration,
) -> Result<(ResourceBudget, Instant), Box<dyn std::error::Error>> {
    let deadline = Instant::now()
        .checked_add(timeout)
        .ok_or_else(|| invalid_argument("timeout exceeds the supported range"))?;
    Ok((
        ResourceBudget {
            max_records,
            max_bytes_read,
            max_output_bytes,
            max_single_value_bytes,
            deadline: Some(deadline),
            ..ResourceBudget::default()
        },
        deadline,
    ))
}

pub(super) fn remaining_timeout(deadline: Instant) -> Result<Duration, Box<dyn std::error::Error>> {
    deadline
        .checked_duration_since(Instant::now())
        .ok_or_else(|| deadline_exceeded("query timed out").into())
}

pub(super) fn ensure_before_deadline(deadline: Instant) -> Result<(), Box<dyn std::error::Error>> {
    if Instant::now() >= deadline {
        Err(deadline_exceeded("query timed out").into())
    } else {
        Ok(())
    }
}

pub(super) fn parse_sql_parameters(
    values: &[String],
) -> Result<std::collections::BTreeMap<String, SqlParameter>, CliError> {
    let mut parameters = std::collections::BTreeMap::new();
    for value in values {
        let (name, raw) = value
            .split_once('=')
            .ok_or_else(|| invalid_argument("query parameters must use NAME=VALUE"))?;
        if name.is_empty()
            || name.len() > 64
            || !name.bytes().enumerate().all(|(index, byte)| {
                byte == b'_'
                    || byte.is_ascii_alphanumeric() && (index > 0 || byte.is_ascii_alphabetic())
            })
        {
            return Err(invalid_argument(
                "query parameter names must be ASCII identifiers",
            ));
        }
        if raw.len() > 16 * 1024 * 1024 {
            return Err(invalid_argument(
                "query parameter value exceeds the fixed size limit",
            ));
        }
        let parameter = match raw {
            value if value.starts_with("text:") => SqlParameter::Text(value[5..].to_string()),
            value if value.starts_with("int:") => SqlParameter::Int64(
                value[4..]
                    .parse()
                    .map_err(|_| invalid_argument("query int parameter must fit i64"))?,
            ),
            value if value.starts_with("float:") => {
                let number = value[6..]
                    .parse::<f64>()
                    .map_err(|_| invalid_argument("query float parameter must fit f64"))?;
                if !number.is_finite() {
                    return Err(invalid_argument("query float parameter must be finite"));
                }
                SqlParameter::Float64(number)
            }
            value if value.starts_with("bool:") => match &value[5..] {
                "true" => SqlParameter::Bool(true),
                "false" => SqlParameter::Bool(false),
                _ => {
                    return Err(invalid_argument(
                        "query bool parameter must be true or false",
                    ));
                }
            },
            "null" => SqlParameter::Null,
            "true" => SqlParameter::Bool(true),
            "false" => SqlParameter::Bool(false),
            _ if integer_text(raw) => SqlParameter::Int64(raw.parse().map_err(|_| {
                invalid_argument("query integer parameter is outside the i64 range")
            })?),
            _ => SqlParameter::Text(raw.to_string()),
        };
        if parameters.insert(name.to_string(), parameter).is_some() {
            return Err(invalid_argument("query parameter names must be unique"));
        }
    }
    Ok(parameters)
}

fn integer_text(value: &str) -> bool {
    let digits = value
        .strip_prefix('-')
        .or_else(|| value.strip_prefix('+'))
        .unwrap_or(value);
    !digits.is_empty() && digits.bytes().all(|byte| byte.is_ascii_digit())
}

pub(super) fn diagnostic_timing(enabled: bool, stage: &str, started: Instant) {
    diagnostic_duration(enabled, stage, started.elapsed());
}

pub(super) fn diagnostic_duration(enabled: bool, stage: &str, duration: Duration) {
    if enabled {
        eprintln!(
            "diagnostic.stage={stage},elapsed_ms={}",
            duration.as_millis()
        );
    }
}

pub(super) fn source_supports_table(capabilities: &[String], table: &str) -> bool {
    table == "agents" || capabilities.iter().any(|candidate| candidate == table)
}

pub(super) fn rewrite_control_query(sql: &str) -> Result<Option<String>, CliError> {
    let trimmed = sql.trim();
    let statement_start = sql.len().saturating_sub(sql.trim_start().len());
    let statement = trimmed.strip_suffix(';').unwrap_or(trimmed).trim_end();
    let words = statement.split_whitespace().collect::<Vec<_>>();
    if words.len() == 2
        && words[0].eq_ignore_ascii_case("SHOW")
        && words[1].eq_ignore_ascii_case("TABLES")
    {
        return Ok(Some(
            "SELECT table_name, table_kind FROM aql_tables ORDER BY table_name".to_string(),
        ));
    }
    if words.len() == 2
        && (words[0].eq_ignore_ascii_case("DESCRIBE") || words[0].eq_ignore_ascii_case("DESC"))
    {
        let table = words[1].to_ascii_lowercase();
        if !QUERY_SCHEMAS.iter().any(|schema| schema.name == table) {
            let location = statement
                .find(words[1])
                .map(|index| line_column(sql, statement_start + index));
            let error = invalid_argument(format!("unknown table `{table}`")).with_stage("control");
            return Err(match location {
                Some((line, column)) => error.with_location(line, column),
                None => error,
            });
        }
        let escaped = table.replace('\'', "''");
        return Ok(Some(format!(
            "SELECT column_name, data_type, nullable, access_class FROM aql_columns WHERE table_name = '{escaped}' ORDER BY ordinal_position"
        )));
    }
    Ok(None)
}

fn line_column(input: &str, byte_index: usize) -> (u64, u64) {
    let prefix = &input[..byte_index];
    let line = prefix
        .chars()
        .filter(|character| *character == '\n')
        .count() as u64
        + 1;
    let column = prefix
        .rsplit_once('\n')
        .map_or(prefix, |(_, line)| line)
        .chars()
        .count() as u64
        + 1;
    (line, column)
}
