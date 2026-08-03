//! Shared toolbox for the read-only source adapters.
//!
//! The Claude Code, Codex, Kimi Code, and OpenCode adapters historically
//! carried private copies of these small safety primitives, and the copies
//! had started to diverge. Every function here is behavior-pinned by the
//! adapters' test suites: adapters delegate to this module so a fix or
//! hardening lands once instead of four times.

use std::io::{BufRead, BufReader, Read};
use std::path::Path;

use aql_model::{AccessClass, SessionRecord};

use crate::{
    AdapterError, ColumnCapability, ColumnName, Literal, Predicate, PushdownState, ScanRequest,
};

/// Returns whether `name` is part of the authorized projection.
#[must_use]
pub fn projected(projection: &[ColumnName], name: &str) -> bool {
    projection.iter().any(|column| column.as_str() == name)
}

/// Reports the pushdown accuracy of `request.limit` and clears it when the
/// scan cannot honor it exactly.
///
/// This form is for scans that never apply predicates: any offered predicate
/// means the engine must re-check, so the limit is dropped from the request
/// and reported [`PushdownState::Unsupported`]. Scans that do apply exact
/// predicates use [`limit_pushdown`] with their own predicate verdict
/// instead.
pub fn normalize_limit(request: &mut ScanRequest) -> Option<PushdownState> {
    let predicates_empty = request.predicates.is_empty();
    let (effective, state) = limit_pushdown(request, predicates_empty);
    request.limit = effective;
    state
}

/// Computes the effective limit a stream may apply plus its report state.
///
/// The offered limit survives only when `predicates_exact` holds (every
/// offered predicate is applied exactly, or none was offered) and no ordering
/// was requested; otherwise the engine must re-check and the stream must not
/// truncate early.
pub fn limit_pushdown(
    request: &ScanRequest,
    predicates_exact: bool,
) -> (Option<u64>, Option<PushdownState>) {
    let effective = request
        .limit
        .filter(|_| predicates_exact && request.order_hint.is_empty());
    let state = request.limit.map(|_| {
        if effective.is_some() {
            PushdownState::Exact
        } else {
            PushdownState::Unsupported
        }
    });
    (effective, state)
}

/// Appends one content fragment to an accumulating projected value, enforcing
/// the single-value byte budget before allocating.
pub fn append_content(
    current: &mut Option<String>,
    value: &str,
    maximum: u64,
) -> Result<(), AdapterError> {
    let separator = usize::from(current.is_some());
    let existing = current.as_ref().map_or(0, String::len);
    let total = existing
        .checked_add(separator)
        .and_then(|value_len| value_len.checked_add(value.len()))
        .ok_or_else(|| AdapterError::BudgetExceeded {
            resource: "single_value_bytes".to_string(),
            actual: u64::MAX,
        })?;
    if total as u64 > maximum {
        return Err(AdapterError::BudgetExceeded {
            resource: "single_value_bytes".to_string(),
            actual: total as u64,
        });
    }
    let target = current.get_or_insert_with(String::new);
    if !target.is_empty() {
        target.push('\n');
    }
    target.push_str(value);
    Ok(())
}

/// Reads one newline-terminated record without materializing more than
/// `maximum` payload bytes.
///
/// The caller-owned `buffer` is cleared on entry and reused across calls so
/// iteration does not allocate a fresh vector per line. Returns whether the
/// record was newline-terminated and the consumed byte count; the payload
/// without the newline is left in `buffer`. A record longer than `maximum`
/// fails with [`AdapterError::BudgetExceeded`] naming `resource`; a read
/// failure is reported as permission denial at `stage`, mirroring the
/// sanitized error model every adapter applies to source reads.
pub fn read_limited_line<R: Read>(
    reader: &mut BufReader<R>,
    buffer: &mut Vec<u8>,
    maximum: usize,
    resource: &str,
    stage: &str,
) -> Result<Option<(bool, usize)>, AdapterError> {
    buffer.clear();
    let mut consumed = 0;
    loop {
        let chunk = reader
            .fill_buf()
            .map_err(|_| AdapterError::PermissionDenied {
                stage: stage.to_string(),
            })?;
        if chunk.is_empty() {
            return if buffer.is_empty() {
                Ok(None)
            } else {
                Ok(Some((false, consumed)))
            };
        }
        let newline = chunk.iter().position(|byte| *byte == b'\n');
        let take = newline.map_or(chunk.len(), |index| index + 1);
        let payload = if newline.is_some() { take - 1 } else { take };
        if buffer.len() + payload > maximum {
            return Err(AdapterError::BudgetExceeded {
                resource: resource.to_string(),
                actual: (buffer.len() + payload) as u64,
            });
        }
        buffer.extend_from_slice(&chunk[..payload]);
        reader.consume(take);
        consumed += take;
        if newline.is_some() {
            return Ok(Some((true, consumed)));
        }
    }
}

/// Builds a `file:` URI that opens a SQLite database with immutable
/// semantics.
///
/// The path is percent-encoded byte-wise so URI delimiters (`?`, `#`, `%`)
/// and non-ASCII bytes in the data root cannot corrupt the query string.
/// Windows normalization runs first: verbatim `\\?\` prefixes are reduced to
/// their logical form, drive-letter paths gain the `///` prefix, and
/// backslashes become forward slashes so the URI keeps its shape on every
/// platform. `stage` names the sanitized error stage used when the path is
/// not valid UTF-8.
pub fn immutable_uri(database: &Path, stage: &str) -> Result<String, AdapterError> {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    let text = database
        .to_str()
        .ok_or_else(|| AdapterError::UnsupportedFormat {
            stage: stage.to_string(),
        })?;
    let text = normalize_windows_verbatim(text);
    let mut uri = String::with_capacity(text.len() + "file:///?immutable=1".len());
    uri.push_str("file:");
    let bytes = text.as_bytes();
    if bytes.len() >= 2 && bytes[0].is_ascii_alphabetic() && bytes[1] == b':' {
        uri.push_str("///");
    }
    for &byte in bytes {
        match byte {
            b'\\' => uri.push('/'),
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'/' | b':' | b'-' | b'_' | b'.' | b'~' => {
                uri.push(char::from(byte))
            }
            _ => {
                uri.push('%');
                uri.push(char::from(HEX[usize::from(byte >> 4)]));
                uri.push(char::from(HEX[usize::from(byte & 0x0F)]));
            }
        }
    }
    uri.push_str("?immutable=1");
    Ok(uri)
}

#[cfg(windows)]
fn normalize_windows_verbatim(text: &str) -> std::borrow::Cow<'_, str> {
    if let Some(rest) = text.strip_prefix(r"\\?\UNC\") {
        return std::borrow::Cow::Owned(format!(r"\\{rest}"));
    }
    std::borrow::Cow::Borrowed(text.strip_prefix(r"\\?\").unwrap_or(text))
}

#[cfg(not(windows))]
fn normalize_windows_verbatim(text: &str) -> &str {
    text
}

/// Per-adapter declaration of the session columns that support exact
/// conservative predicate pushdown.
///
/// Adapters legitimately differ here — the declaration expresses what each
/// source matches exactly — while [`session_predicate_state`] and
/// [`session_matches`] provide the single shared evaluation logic, so the
/// divergence lives in data rather than in forked code.
#[derive(Clone, Copy, Debug)]
pub struct SessionPredicateCapabilities {
    /// Text columns usable in `Eq`/`In` with text literals.
    pub eq_text: &'static [&'static str],
    /// Boolean columns usable in `Eq`/`In` with boolean literals.
    pub eq_bool: &'static [&'static str],
    /// Nullable columns usable in `IsNull`.
    pub is_null: &'static [&'static str],
}

/// Reports whether one session predicate is applied exactly under
/// `capabilities`.
#[must_use]
pub fn session_predicate_state(
    capabilities: &SessionPredicateCapabilities,
    predicate: &Predicate,
) -> PushdownState {
    let exact = match predicate {
        Predicate::Eq(column, literal) => session_literal_supported(capabilities, column, literal),
        Predicate::In(column, literals) => {
            !literals.is_empty()
                && literals
                    .iter()
                    .all(|literal| session_literal_supported(capabilities, column, literal))
        }
        Predicate::IsNull(column) => capabilities.is_null.contains(&column.as_str()),
        Predicate::And(predicates) => predicates.iter().all(|predicate| {
            session_predicate_state(capabilities, predicate) == PushdownState::Exact
        }),
        Predicate::Range { .. } | Predicate::Unsupported(_) => false,
    };
    if exact {
        PushdownState::Exact
    } else {
        PushdownState::Unsupported
    }
}

/// Evaluates one session predicate against a canonical record.
///
/// `Range` and opaque `Unsupported` predicates pass (`true`) so the engine
/// stays responsible for them; literal predicates outside `capabilities`
/// never match, mirroring the previous per-adapter matchers.
#[must_use]
pub fn session_matches(
    capabilities: &SessionPredicateCapabilities,
    session: &SessionRecord,
    predicate: &Predicate,
) -> bool {
    match predicate {
        Predicate::Eq(column, literal) => {
            session_literal_supported(capabilities, column, literal)
                && session_value_matches(session, column, literal)
        }
        Predicate::In(column, literals) => literals.iter().any(|literal| {
            session_literal_supported(capabilities, column, literal)
                && session_value_matches(session, column, literal)
        }),
        Predicate::IsNull(column) => {
            capabilities.is_null.contains(&column.as_str())
                && session_value_is_null(session, column)
        }
        Predicate::And(predicates) => predicates
            .iter()
            .all(|predicate| session_matches(capabilities, session, predicate)),
        Predicate::Range { .. } | Predicate::Unsupported(_) => true,
    }
}

fn session_literal_supported(
    capabilities: &SessionPredicateCapabilities,
    column: &ColumnName,
    literal: &Literal,
) -> bool {
    match literal {
        Literal::Text(_) => capabilities.eq_text.contains(&column.as_str()),
        Literal::Bool(_) => capabilities.eq_bool.contains(&column.as_str()),
        Literal::Null | Literal::Integer(_) => false,
    }
}

fn session_value_matches(session: &SessionRecord, column: &ColumnName, literal: &Literal) -> bool {
    match (column.as_str(), literal) {
        ("session_id", Literal::Text(value)) => session.session_id.as_str() == value,
        ("native_id", Literal::Text(value)) => session.native_id.as_str() == value,
        ("source_id", Literal::Text(value)) => session.source_id.as_str() == value,
        ("agent_id", Literal::Text(value)) => &session.agent_id == value,
        ("status", Literal::Text(value)) => session.status.as_ref() == Some(value),
        ("archived", Literal::Bool(value)) => session.archived == Some(*value),
        _ => false,
    }
}

fn session_value_is_null(session: &SessionRecord, column: &ColumnName) -> bool {
    match column.as_str() {
        "status" => session.status.is_none(),
        "archived" => session.archived.is_none(),
        _ => false,
    }
}

/// Returns the canonical access class declared for a canonical column.
///
/// This is the single declaration behind every adapter's `capabilities` and
/// `schema` output — pinned by this module's tests — so a column's
/// sensitivity can no longer drift between adapters. Unknown names fail
/// closed to [`AccessClass::Secret`], which no session grant allows.
#[must_use]
pub fn column_access(name: &str) -> AccessClass {
    match name {
        "content" | "content_json" | "name" | "preview" | "title" => AccessClass::Content,
        "cwd" | "path" | "project" => AccessClass::Path,
        "arguments" => AccessClass::ToolInput,
        "output" => AccessClass::ToolOutput,
        "agent_id" | "archived" | "artifact_id" | "bucket_start" | "cached_tokens"
        | "child_session_id" | "created_at" | "duration_ms" | "edge_id" | "edge_kind"
        | "ended_at" | "error_count" | "exit_code" | "input_tokens" | "is_error" | "kind"
        | "media_type" | "message_count" | "message_id" | "model" | "native_edge_id"
        | "native_id" | "namespace" | "output_tokens" | "parent_session_id" | "provider"
        | "role" | "sequence" | "session_id" | "size_bytes" | "source_id" | "started_at"
        | "status" | "tokens_used" | "tool_call_count" | "tool_call_id" | "tool_name"
        | "total_tokens" | "updated_at" | "usage_id" => AccessClass::Safe,
        _ => AccessClass::Secret,
    }
}

/// Builds a [`ColumnCapability`] using the canonical access class for `name`.
#[must_use]
pub fn column_capability(name: &str) -> ColumnCapability {
    ColumnCapability {
        name: ColumnName::new(name),
        access: column_access(name),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn projection_membership_is_exact() {
        let projection = vec![ColumnName::new("session_id"), ColumnName::new("content")];
        assert!(projected(&projection, "session_id"));
        assert!(projected(&projection, "content"));
        assert!(!projected(&projection, "cwd"));
        assert!(!projected(&[], "session_id"));
    }

    #[test]
    fn normalize_limit_keeps_exact_limit_without_predicates_or_ordering() {
        let mut request = ScanRequest {
            limit: Some(7),
            ..scan_request()
        };
        let state = normalize_limit(&mut request);
        assert_eq!(state, Some(PushdownState::Exact));
        assert_eq!(request.limit, Some(7));
    }

    #[test]
    fn normalize_limit_drops_limit_when_predicates_or_ordering_exist() {
        let mut request = ScanRequest {
            limit: Some(7),
            predicates: vec![crate::Predicate::IsNull(ColumnName::new("status"))],
            ..scan_request()
        };
        let state = normalize_limit(&mut request);
        assert_eq!(state, Some(PushdownState::Unsupported));
        assert_eq!(request.limit, None);

        let mut request = ScanRequest {
            limit: Some(0),
            order_hint: vec![crate::OrderingHint {
                column: ColumnName::new("updated_at"),
                descending: true,
            }],
            ..scan_request()
        };
        let state = normalize_limit(&mut request);
        assert_eq!(state, Some(PushdownState::Unsupported));
        assert_eq!(request.limit, None);

        let mut request = scan_request();
        assert_eq!(normalize_limit(&mut request), None);
        assert_eq!(request.limit, None);
    }

    #[test]
    fn limit_pushdown_honors_the_caller_predicate_verdict() {
        let mut request = ScanRequest {
            limit: Some(3),
            predicates: vec![crate::Predicate::IsNull(ColumnName::new("status"))],
            ..scan_request()
        };
        let (effective, state) = limit_pushdown(&request, true);
        assert_eq!(effective, Some(3));
        assert_eq!(state, Some(PushdownState::Exact));
        assert_eq!(request.limit, Some(3), "the request is left untouched");

        let (effective, state) = limit_pushdown(&request, false);
        assert_eq!(effective, None);
        assert_eq!(state, Some(PushdownState::Unsupported));

        request.limit = None;
        assert_eq!(limit_pushdown(&request, true), (None, None));
    }

    #[test]
    fn append_content_separates_fragments_and_enforces_the_budget() {
        let mut current = None;
        append_content(&mut current, "first", 64).expect("first fragment fits");
        append_content(&mut current, "second", 64).expect("second fragment fits");
        assert_eq!(current.as_deref(), Some("first\nsecond"));

        let error = append_content(&mut current, "x", 4).expect_err("budget is enforced");
        assert_eq!(
            error,
            AdapterError::BudgetExceeded {
                resource: "single_value_bytes".to_string(),
                actual: 14,
            }
        );
    }

    #[test]
    fn read_limited_line_splits_records_across_buffer_fills() {
        let input: &[u8] = b"first\nsecond\npartial";
        let mut reader = BufReader::with_capacity(4, input);
        let mut buffer = Vec::new();
        let mut lines = Vec::new();
        while let Some((complete, consumed)) =
            read_limited_line(&mut reader, &mut buffer, 1024, "record_bytes", "read_stage")
                .expect("reads succeed")
        {
            lines.push((complete, consumed, buffer.clone()));
        }
        assert_eq!(
            lines,
            vec![
                (true, 6, b"first".to_vec()),
                (true, 7, b"second".to_vec()),
                (false, 7, b"partial".to_vec()),
            ]
        );
    }

    #[test]
    fn read_limited_line_reports_eof_and_limit_exhaustion() {
        let mut reader = BufReader::new(&b""[..]);
        let mut buffer = Vec::new();
        assert_eq!(
            read_limited_line(&mut reader, &mut buffer, 1024, "record_bytes", "read_stage")
                .expect("empty input is a clean EOF"),
            None
        );

        let mut reader = BufReader::new(&b"ab"[..]);
        let result = read_limited_line(&mut reader, &mut buffer, 2, "record_bytes", "read_stage")
            .expect("a record at the exact limit fits");
        assert_eq!(result, Some((false, 2)));
        assert_eq!(buffer, b"ab");

        let mut reader = BufReader::new(&b"abcd\n"[..]);
        let error = read_limited_line(&mut reader, &mut buffer, 3, "record_bytes", "read_stage")
            .expect_err("oversized records exceed the budget");
        assert_eq!(
            error,
            AdapterError::BudgetExceeded {
                resource: "record_bytes".to_string(),
                actual: 4,
            }
        );
    }

    #[test]
    fn immutable_uri_encodes_posix_paths_minimally() {
        let uri = immutable_uri(Path::new("/data/opencode.db"), "stage").expect("URI builds");
        assert_eq!(uri, "file:/data/opencode.db?immutable=1");
    }

    #[test]
    fn immutable_uri_normalizes_drive_letters_on_every_platform() {
        let uri =
            immutable_uri(Path::new(r"C:\Users\tester\opencode.db"), "stage").expect("URI builds");
        assert_eq!(uri, "file:///C:/Users/tester/opencode.db?immutable=1");
    }

    #[cfg(windows)]
    #[test]
    fn immutable_uri_reduces_verbatim_prefixes() {
        let uri = immutable_uri(Path::new(r"\\?\C:\data\db.sqlite"), "stage").expect("URI builds");
        assert_eq!(uri, "file:///C:/data/db.sqlite?immutable=1");

        let uri = immutable_uri(Path::new(r"\\?\UNC\server\share\db.sqlite"), "stage")
            .expect("URI builds");
        assert_eq!(uri, "file://server/share/db.sqlite?immutable=1");
    }

    #[test]
    fn immutable_uri_percent_encodes_delimiters_and_non_ascii() {
        let cases = [
            (
                "/data/my db.sqlite",
                "file:/data/my%20db.sqlite?immutable=1",
            ),
            (
                "/data/中文.db",
                "file:/data/%E4%B8%AD%E6%96%87.db?immutable=1",
            ),
            ("/data/100%.db", "file:/data/100%25.db?immutable=1"),
            ("/data/a#b.db", "file:/data/a%23b.db?immutable=1"),
            ("/data/a?b.db", "file:/data/a%3Fb.db?immutable=1"),
        ];
        for (input, expected) in cases {
            assert_eq!(
                immutable_uri(Path::new(input), "stage").expect("URI builds"),
                expected,
                "input {input}",
            );
        }
    }

    #[test]
    fn session_predicate_state_follows_the_declared_capabilities() {
        let kimi = SessionPredicateCapabilities {
            eq_text: &["session_id", "native_id", "source_id", "agent_id", "status"],
            eq_bool: &["archived"],
            is_null: &["status", "archived"],
        };
        let opencode = SessionPredicateCapabilities {
            eq_text: &["session_id", "native_id", "source_id", "agent_id"],
            eq_bool: &["archived"],
            is_null: &["status"],
        };
        let status_eq = Predicate::Eq(
            ColumnName::new("status"),
            Literal::Text("active".to_string()),
        );
        assert_eq!(
            session_predicate_state(&kimi, &status_eq),
            PushdownState::Exact
        );
        assert_eq!(
            session_predicate_state(&opencode, &status_eq),
            PushdownState::Unsupported
        );

        let archived_null = Predicate::IsNull(ColumnName::new("archived"));
        assert_eq!(
            session_predicate_state(&kimi, &archived_null),
            PushdownState::Exact
        );
        assert_eq!(
            session_predicate_state(&opencode, &archived_null),
            PushdownState::Unsupported
        );

        let status_null = Predicate::IsNull(ColumnName::new("status"));
        assert_eq!(
            session_predicate_state(&opencode, &status_null),
            PushdownState::Exact
        );

        let empty_in = Predicate::In(ColumnName::new("session_id"), Vec::new());
        assert_eq!(
            session_predicate_state(&kimi, &empty_in),
            PushdownState::Unsupported
        );

        let wrong_literal = Predicate::Eq(ColumnName::new("session_id"), Literal::Integer(1));
        assert_eq!(
            session_predicate_state(&kimi, &wrong_literal),
            PushdownState::Unsupported
        );

        let nested = Predicate::And(vec![
            Predicate::Eq(
                ColumnName::new("session_id"),
                Literal::Text("s".to_string()),
            ),
            Predicate::And(vec![Predicate::Eq(
                ColumnName::new("archived"),
                Literal::Bool(false),
            )]),
        ]);
        assert_eq!(
            session_predicate_state(&kimi, &nested),
            PushdownState::Exact
        );

        let range = Predicate::Range {
            column: ColumnName::new("updated_at"),
            lower: Some(Literal::Integer(1)),
            upper: None,
        };
        assert_eq!(
            session_predicate_state(&kimi, &range),
            PushdownState::Unsupported
        );
        assert_eq!(
            session_predicate_state(&kimi, &Predicate::And(Vec::new())),
            PushdownState::Exact
        );
    }

    #[test]
    fn session_matches_respects_capabilities_and_values() {
        let kimi = SessionPredicateCapabilities {
            eq_text: &["session_id", "native_id", "source_id", "agent_id", "status"],
            eq_bool: &["archived"],
            is_null: &["status", "archived"],
        };
        let opencode = SessionPredicateCapabilities {
            eq_text: &["session_id", "native_id", "source_id", "agent_id"],
            eq_bool: &["archived"],
            is_null: &["status"],
        };
        let session = session_record();
        let status_eq = Predicate::Eq(
            ColumnName::new("status"),
            Literal::Text("active".to_string()),
        );
        assert!(session_matches(&kimi, &session, &status_eq));
        assert!(
            !session_matches(&opencode, &session, &status_eq),
            "unsupported capability never matches"
        );

        let session_in = Predicate::In(
            ColumnName::new("session_id"),
            vec![
                Literal::Text("other".to_string()),
                Literal::Text("agent:abc:session".to_string()),
            ],
        );
        assert!(session_matches(&opencode, &session, &session_in));

        let archived_null = Predicate::IsNull(ColumnName::new("archived"));
        assert!(session_matches(&kimi, &session, &archived_null));
        assert!(!session_matches(&opencode, &session, &archived_null));

        let status_null = Predicate::IsNull(ColumnName::new("status"));
        assert!(!session_matches(&opencode, &session, &status_null));

        let archived_eq = Predicate::Eq(ColumnName::new("archived"), Literal::Bool(false));
        assert!(
            !session_matches(&kimi, &session, &archived_eq),
            "archived is None, not false"
        );

        let range = Predicate::Range {
            column: ColumnName::new("updated_at"),
            lower: None,
            upper: None,
        };
        assert!(
            session_matches(&opencode, &session, &range),
            "engine-owned predicates pass through"
        );

        let and = Predicate::And(vec![
            Predicate::Eq(
                ColumnName::new("agent_id"),
                Literal::Text("main".to_string()),
            ),
            status_null,
        ]);
        assert!(!session_matches(&kimi, &session, &and));
    }

    #[test]
    fn column_access_matches_the_canonical_declaration() {
        let content = ["content", "content_json", "name", "preview", "title"];
        for name in content {
            assert_eq!(column_access(name), AccessClass::Content, "{name}");
        }
        let path = ["cwd", "path", "project"];
        for name in path {
            assert_eq!(column_access(name), AccessClass::Path, "{name}");
        }
        assert_eq!(column_access("arguments"), AccessClass::ToolInput);
        assert_eq!(column_access("output"), AccessClass::ToolOutput);
        let safe = [
            "agent_id",
            "archived",
            "artifact_id",
            "bucket_start",
            "cached_tokens",
            "child_session_id",
            "created_at",
            "duration_ms",
            "edge_id",
            "edge_kind",
            "ended_at",
            "error_count",
            "exit_code",
            "input_tokens",
            "is_error",
            "kind",
            "media_type",
            "message_count",
            "message_id",
            "model",
            "native_edge_id",
            "native_id",
            "namespace",
            "output_tokens",
            "parent_session_id",
            "provider",
            "role",
            "sequence",
            "session_id",
            "size_bytes",
            "source_id",
            "started_at",
            "status",
            "tokens_used",
            "tool_call_count",
            "tool_call_id",
            "tool_name",
            "total_tokens",
            "updated_at",
            "usage_id",
        ];
        for name in safe {
            assert_eq!(column_access(name), AccessClass::Safe, "{name}");
        }
        assert_eq!(column_access("password"), AccessClass::Secret);
        assert_eq!(column_access(""), AccessClass::Secret);
    }

    fn session_record() -> SessionRecord {
        SessionRecord {
            session_id: aql_model::EntityId::new("agent:abc:session"),
            native_id: aql_model::NativeId::new("session"),
            source_id: aql_model::SourceId::new("agent:abc"),
            agent_id: "main".to_string(),
            title: None,
            preview: None,
            cwd: None,
            project: None,
            model: None,
            provider: None,
            created_at: None,
            updated_at: None,
            status: Some("active".to_string()),
            archived: None,
            message_count: None,
            tool_call_count: None,
            tokens_used: None,
            identity_confidence: aql_model::IdentityConfidence::Exact,
            snapshot_state: aql_model::SnapshotState::Weak,
            provenance: std::collections::BTreeMap::new(),
            extensions: std::collections::BTreeMap::new(),
        }
    }

    fn scan_request() -> ScanRequest {
        ScanRequest {
            source: aql_model::SourceManifest {
                source_id: aql_model::SourceId::new("source"),
                agent_id: "agent".to_string(),
                display_name: "Agent".to_string(),
                data_root_token: "root".to_string(),
                format_fingerprint: "fingerprint".to_string(),
                capabilities: Vec::new(),
                snapshot: None,
                warnings: Vec::new(),
            },
            table: crate::TableName::Sessions,
            projection: Vec::new(),
            predicates: Vec::new(),
            limit: None,
            order_hint: Vec::new(),
            access: crate::AccessGrant::default(),
            budget: crate::ResourceBudget::default(),
            cancellation: crate::CancellationToken::default(),
            snapshot: None,
        }
    }
}
