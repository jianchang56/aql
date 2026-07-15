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
