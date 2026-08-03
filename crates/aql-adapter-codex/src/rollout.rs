use std::cell::Cell;
use std::io::{BufReader, Read};

use aql_adapter_api::util::read_limited_line;
use aql_adapter_api::{AdapterError, AdapterWarningKind};
use chrono::{DateTime, Utc};
use serde::de::{DeserializeSeed, Deserializer, IgnoredAny, MapAccess, SeqAccess, Visitor};
use serde_json::Value as JsonValue;

pub(crate) const MAX_ROLLOUT_RECORD_BYTES: usize = 1024 * 1024;

#[derive(Default)]
pub(crate) struct ReadFields {
    pub content: bool,
    pub arguments: bool,
    pub output: bool,
    pub artifacts: bool,
    pub artifact_content: bool,
}

#[derive(Default)]
pub(crate) struct ParsedArtifactChange {
    pub path: String,
    pub change_type: Option<String>,
    pub move_path: Option<String>,
    pub content: Option<String>,
    pub unified_diff: Option<String>,
}

#[derive(Default)]
pub(crate) struct ParsedPayload {
    pub item_type: Option<String>,
    pub role: Option<String>,
    pub name: Option<String>,
    pub call_id: Option<String>,
    pub content: Option<String>,
    pub arguments: Option<JsonValue>,
    pub output: Option<String>,
    pub changes: Vec<ParsedArtifactChange>,
}

#[derive(Default)]
pub(crate) struct ParsedEvent {
    pub event_type: Option<String>,
    pub timestamp: Option<DateTime<Utc>>,
    pub payload: ParsedPayload,
}

#[cfg(test)]
pub(crate) struct ParseResult {
    pub warnings: Vec<AdapterWarningKind>,
}

pub(crate) struct NextEvent {
    pub event: Option<ParsedEvent>,
    pub warnings: Vec<AdapterWarningKind>,
    pub bytes_read: u64,
}

pub(crate) fn parse_next<R: Read>(
    reader: &mut BufReader<R>,
    fields: &ReadFields,
    buffer: &mut Vec<u8>,
) -> Result<NextEvent, AdapterError> {
    let mut bytes_read = 0_u64;
    loop {
        let Some((complete, consumed)) = read_limited_line(
            reader,
            buffer,
            MAX_ROLLOUT_RECORD_BYTES,
            "codex_rollout_record_bytes",
            "codex_record_read",
        )?
        else {
            return Ok(NextEvent {
                event: None,
                warnings: Vec::new(),
                bytes_read,
            });
        };
        bytes_read += consumed as u64;
        if buffer.iter().all(|byte| byte.is_ascii_whitespace()) {
            if complete {
                continue;
            }
            return Ok(NextEvent {
                event: None,
                warnings: Vec::new(),
                bytes_read,
            });
        }
        let failure: Cell<Option<&'static str>> = Cell::new(None);
        let mut deserializer = serde_json::Deserializer::from_slice(buffer);
        let parsed = (EventSeed {
            fields,
            failure: &failure,
        })
        .deserialize(&mut deserializer)
        .and_then(|event| deserializer.end().map(|()| event));
        return match parsed {
            Ok(event) => {
                let warnings = if event.event_type.as_deref().is_some_and(|kind| {
                    !matches!(
                        kind,
                        "session_meta"
                            | "turn_context"
                            | "event_msg"
                            | "response_item"
                            | "compacted"
                    )
                }) {
                    vec![AdapterWarningKind::UnknownEvent]
                } else {
                    Vec::new()
                };
                Ok(NextEvent {
                    event: Some(event),
                    warnings,
                    bytes_read,
                })
            }
            Err(_) if !complete => Ok(NextEvent {
                event: None,
                warnings: vec![AdapterWarningKind::TruncatedRecord],
                bytes_read,
            }),
            Err(_) => Err(AdapterError::CorruptSource {
                stage: failure.get().unwrap_or("parse_rollout").to_string(),
            }),
        };
    }
}

#[cfg(test)]
pub(crate) fn parse_stream<R: Read, F>(
    mut reader: BufReader<R>,
    fields: ReadFields,
    mut consume: F,
) -> Result<ParseResult, AdapterError>
where
    F: FnMut(ParsedEvent) -> bool,
{
    let mut warnings = Vec::new();
    let mut buffer = Vec::new();
    loop {
        let next = parse_next(&mut reader, &fields, &mut buffer)?;
        warnings.extend(next.warnings);
        match next.event {
            Some(event) => {
                if !consume(event) {
                    return Ok(ParseResult { warnings });
                }
            }
            None => return Ok(ParseResult { warnings }),
        }
    }
}

struct EventSeed<'a> {
    fields: &'a ReadFields,
    failure: &'a Cell<Option<&'static str>>,
}

impl<'de> DeserializeSeed<'de> for EventSeed<'_> {
    type Value = ParsedEvent;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_map(EventVisitor {
            fields: self.fields,
            failure: self.failure,
        })
    }
}

struct EventVisitor<'a> {
    fields: &'a ReadFields,
    failure: &'a Cell<Option<&'static str>>,
}

impl<'de> Visitor<'de> for EventVisitor<'_> {
    type Value = ParsedEvent;

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("a Codex rollout event")
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut event = ParsedEvent::default();
        while let Some(key) = map.next_key::<String>()? {
            match key.as_str() {
                "type" => event.event_type = Some(map.next_value()?),
                "timestamp" => {
                    let value: String = map.next_value()?;
                    match DateTime::parse_from_rfc3339(&value) {
                        Ok(parsed) => event.timestamp = Some(parsed.with_timezone(&Utc)),
                        Err(_) => {
                            self.failure.set(Some("codex_timestamp"));
                            return Err(serde::de::Error::custom("invalid codex timestamp"));
                        }
                    }
                }
                "payload" => {
                    event.payload = map.next_value_seed(PayloadSeed {
                        fields: self.fields,
                        failure: self.failure,
                    })?;
                }
                _ => {
                    map.next_value::<IgnoredAny>()?;
                }
            }
        }
        Ok(event)
    }
}

struct PayloadSeed<'a> {
    fields: &'a ReadFields,
    failure: &'a Cell<Option<&'static str>>,
}

impl<'de> DeserializeSeed<'de> for PayloadSeed<'_> {
    type Value = ParsedPayload;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_map(PayloadVisitor {
            fields: self.fields,
            failure: self.failure,
        })
    }
}

struct PayloadVisitor<'a> {
    fields: &'a ReadFields,
    failure: &'a Cell<Option<&'static str>>,
}

impl<'de> Visitor<'de> for PayloadVisitor<'_> {
    type Value = ParsedPayload;

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("a Codex rollout payload")
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut payload = ParsedPayload::default();
        while let Some(key) = map.next_key::<String>()? {
            match key.as_str() {
                "type" => payload.item_type = Some(map.next_value()?),
                "role" => payload.role = Some(map.next_value()?),
                "name" => payload.name = Some(map.next_value()?),
                "call_id" => payload.call_id = Some(map.next_value()?),
                "content" if self.fields.content => {
                    payload.content = Some(map.next_value_seed(ContentSeed)?);
                }
                "arguments" if self.fields.arguments => {
                    let value: String = map.next_value()?;
                    match serde_json::from_str(&value) {
                        Ok(parsed) => payload.arguments = Some(parsed),
                        Err(_) => {
                            self.failure.set(Some("codex_arguments"));
                            return Err(serde::de::Error::custom("invalid codex arguments"));
                        }
                    }
                }
                "output" if self.fields.output => payload.output = Some(map.next_value()?),
                "changes" if self.fields.artifacts => {
                    payload.changes = map.next_value_seed(ChangesSeed {
                        include_content: self.fields.artifact_content,
                    })?;
                }
                _ => {
                    map.next_value::<IgnoredAny>()?;
                }
            }
        }
        Ok(payload)
    }
}

struct ChangesSeed {
    include_content: bool,
}

impl<'de> DeserializeSeed<'de> for ChangesSeed {
    type Value = Vec<ParsedArtifactChange>;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_map(ChangesVisitor {
            include_content: self.include_content,
        })
    }
}

struct ChangesVisitor {
    include_content: bool,
}

impl<'de> Visitor<'de> for ChangesVisitor {
    type Value = Vec<ParsedArtifactChange>;

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("a patch changes object")
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut changes = Vec::new();
        while let Some(path) = map.next_key::<String>()? {
            let mut change = map.next_value_seed(ChangeSeed {
                include_content: self.include_content,
            })?;
            change.path = path;
            changes.push(change);
        }
        Ok(changes)
    }
}

struct ChangeSeed {
    include_content: bool,
}

impl<'de> DeserializeSeed<'de> for ChangeSeed {
    type Value = ParsedArtifactChange;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_map(ChangeVisitor {
            include_content: self.include_content,
        })
    }
}

struct ChangeVisitor {
    include_content: bool,
}

impl<'de> Visitor<'de> for ChangeVisitor {
    type Value = ParsedArtifactChange;

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("a patch change")
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut change = ParsedArtifactChange::default();
        while let Some(key) = map.next_key::<String>()? {
            match key.as_str() {
                "type" => change.change_type = Some(map.next_value()?),
                "move_path" => change.move_path = Some(map.next_value()?),
                "content" if self.include_content => change.content = Some(map.next_value()?),
                "unified_diff" if self.include_content => {
                    change.unified_diff = Some(map.next_value()?)
                }
                _ => {
                    map.next_value::<IgnoredAny>()?;
                }
            }
        }
        Ok(change)
    }
}

struct ContentSeed;

impl<'de> DeserializeSeed<'de> for ContentSeed {
    type Value = String;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_seq(ContentVisitor)
    }
}

struct ContentVisitor;

impl<'de> Visitor<'de> for ContentVisitor {
    type Value = String;

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("a message content array")
    }

    fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let mut content = Vec::new();
        while let Some(text) = sequence.next_element_seed(ContentItemSeed)? {
            if let Some(text) = text {
                content.push(text);
            }
        }
        Ok(content.join("\n"))
    }
}

struct ContentItemSeed;

impl<'de> DeserializeSeed<'de> for ContentItemSeed {
    type Value = Option<String>;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_map(ContentItemVisitor)
    }
}

struct ContentItemVisitor;

impl<'de> Visitor<'de> for ContentItemVisitor {
    type Value = Option<String>;

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("a message content item")
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut text = None;
        while let Some(key) = map.next_key::<String>()? {
            if key == "text" {
                text = Some(map.next_value()?);
            } else {
                map.next_value::<IgnoredAny>()?;
            }
        }
        Ok(text)
    }
}

#[cfg(test)]
mod tests {
    use std::io::{BufReader, Cursor, Read};

    use super::*;

    #[test]
    fn fixed_scan_boundary_excludes_appended_events() {
        let initial = b"{\"type\":\"session_meta\",\"payload\":{}}\n";
        let appended = b"{\"type\":\"future_fixture_event\",\"payload\":{}}\n";
        let mut bytes = initial.to_vec();
        bytes.extend_from_slice(appended);
        let mut events = 0;
        let parsed = parse_stream(
            BufReader::new(Cursor::new(bytes).take(initial.len() as u64)),
            ReadFields::default(),
            |_| {
                events += 1;
                true
            },
        )
        .expect("bounded rollout must parse");
        assert_eq!(events, 1);
        assert!(parsed.warnings.is_empty());
    }

    #[test]
    fn parse_next_advances_exactly_one_event() {
        let bytes = b"{\"type\":\"session_meta\",\"payload\":{}}\n{\"type\":\"turn_context\",\"payload\":{}}\n";
        let mut reader = BufReader::new(Cursor::new(bytes));
        let fields = ReadFields::default();
        let mut buffer = Vec::new();

        let first = parse_next(&mut reader, &fields, &mut buffer).expect("first event must parse");
        assert_eq!(
            first.event.and_then(|event| event.event_type).as_deref(),
            Some("session_meta")
        );

        let second =
            parse_next(&mut reader, &fields, &mut buffer).expect("second event must parse");
        assert_eq!(
            second.event.and_then(|event| event.event_type).as_deref(),
            Some("turn_context")
        );

        let end = parse_next(&mut reader, &fields, &mut buffer).expect("end of stream is valid");
        assert!(end.event.is_none());
        assert!(end.warnings.is_empty());
    }

    #[test]
    fn incomplete_boundary_tail_is_reported_without_losing_prior_events() {
        let bytes = b"{\"type\":\"session_meta\",\"payload\":{}}\n{\"type\":";
        let mut events = 0;
        let parsed = parse_stream(
            BufReader::new(Cursor::new(bytes)),
            ReadFields::default(),
            |_| {
                events += 1;
                true
            },
        )
        .expect("truncated tail is a warning");
        assert_eq!(events, 1);
        assert_eq!(parsed.warnings, vec![AdapterWarningKind::TruncatedRecord]);
    }

    #[test]
    fn corrupt_arguments_fail_closed_with_a_stable_stage() {
        let bytes = b"{\"timestamp\":\"2026-01-01T00:00:03Z\",\"type\":\"response_item\",\"payload\":{\"type\":\"function_call\",\"name\":\"example_tool\",\"call_id\":\"call-fixture-1\",\"arguments\":\"{unclosed\"}}\n";
        let mut reader = BufReader::new(Cursor::new(&bytes[..]));
        let fields = ReadFields {
            arguments: true,
            ..ReadFields::default()
        };
        let error = match parse_next(&mut reader, &fields, &mut Vec::new()) {
            Ok(_) => panic!("corrupt arguments must fail the record"),
            Err(error) => error,
        };
        assert!(matches!(
            error,
            AdapterError::CorruptSource { stage } if stage == "codex_arguments"
        ));
    }

    #[test]
    fn corrupt_timestamp_fails_closed_with_a_stable_stage() {
        let bytes =
            b"{\"timestamp\":\"not-a-timestamp\",\"type\":\"session_meta\",\"payload\":{}}\n";
        let mut reader = BufReader::new(Cursor::new(&bytes[..]));
        let error = match parse_next(&mut reader, &ReadFields::default(), &mut Vec::new()) {
            Ok(_) => panic!("corrupt timestamp must fail the record"),
            Err(error) => error,
        };
        assert!(matches!(
            error,
            AdapterError::CorruptSource { stage } if stage == "codex_timestamp"
        ));
    }

    #[test]
    fn oversized_record_fails_before_materialization() {
        let mut bytes = b"{\"type\":\"".to_vec();
        bytes.extend(std::iter::repeat_n(b'x', MAX_ROLLOUT_RECORD_BYTES));
        bytes.extend_from_slice(b"\"}\n");
        let error = match parse_next(
            &mut BufReader::new(Cursor::new(bytes)),
            &ReadFields::default(),
            &mut Vec::new(),
        ) {
            Ok(_) => panic!("oversized record must fail the bounded read"),
            Err(error) => error,
        };
        assert!(matches!(
            error,
            AdapterError::BudgetExceeded { resource, .. } if resource == "codex_rollout_record_bytes"
        ));
    }
}
