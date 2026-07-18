use std::io::{BufRead, BufReader, Read};

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

/// Reads one newline-terminated record without materializing more than
/// `maximum` payload bytes, mirroring the bounded readers used by the Claude
/// Code and Kimi Code adapters. Returns the payload without the newline,
/// whether the record was newline-terminated, and the consumed byte count.
pub(crate) fn read_limited_line<R: Read>(
    reader: &mut BufReader<R>,
    maximum: usize,
    resource: &str,
) -> Result<Option<(Vec<u8>, bool, usize)>, AdapterError> {
    let mut output = Vec::new();
    let mut consumed = 0;
    loop {
        let buffer = reader
            .fill_buf()
            .map_err(|_| AdapterError::PermissionDenied {
                stage: "codex_record_read".to_string(),
            })?;
        if buffer.is_empty() {
            return if output.is_empty() {
                Ok(None)
            } else {
                Ok(Some((output, false, consumed)))
            };
        }
        let newline = buffer.iter().position(|byte| *byte == b'\n');
        let take = newline.map_or(buffer.len(), |index| index + 1);
        let payload = if newline.is_some() { take - 1 } else { take };
        if output.len() + payload > maximum {
            return Err(AdapterError::BudgetExceeded {
                resource: resource.to_string(),
                actual: (output.len() + payload) as u64,
            });
        }
        output.extend_from_slice(&buffer[..payload]);
        reader.consume(take);
        consumed += take;
        if newline.is_some() {
            return Ok(Some((output, true, consumed)));
        }
    }
}

pub(crate) fn parse_next<R: Read>(
    reader: &mut BufReader<R>,
    fields: &ReadFields,
) -> Result<NextEvent, AdapterError> {
    let mut bytes_read = 0_u64;
    loop {
        let Some((line, complete, consumed)) = read_limited_line(
            reader,
            MAX_ROLLOUT_RECORD_BYTES,
            "codex_rollout_record_bytes",
        )?
        else {
            return Ok(NextEvent {
                event: None,
                warnings: Vec::new(),
                bytes_read,
            });
        };
        bytes_read += consumed as u64;
        if line.iter().all(|byte| byte.is_ascii_whitespace()) {
            if complete {
                continue;
            }
            return Ok(NextEvent {
                event: None,
                warnings: Vec::new(),
                bytes_read,
            });
        }
        let mut deserializer = serde_json::Deserializer::from_slice(&line);
        let parsed = (EventSeed { fields })
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
                stage: "parse_rollout".to_string(),
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
    loop {
        let next = parse_next(&mut reader, &fields)?;
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
}

impl<'de> DeserializeSeed<'de> for EventSeed<'_> {
    type Value = ParsedEvent;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_map(EventVisitor {
            fields: self.fields,
        })
    }
}

struct EventVisitor<'a> {
    fields: &'a ReadFields,
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
                    event.timestamp = DateTime::parse_from_rfc3339(&value)
                        .ok()
                        .map(|value| value.with_timezone(&Utc));
                }
                "payload" => {
                    event.payload = map.next_value_seed(PayloadSeed {
                        fields: self.fields,
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
}

impl<'de> DeserializeSeed<'de> for PayloadSeed<'_> {
    type Value = ParsedPayload;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_map(PayloadVisitor {
            fields: self.fields,
        })
    }
}

struct PayloadVisitor<'a> {
    fields: &'a ReadFields,
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
                    payload.arguments = serde_json::from_str(&value).ok();
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

        let first = parse_next(&mut reader, &fields).expect("first event must parse");
        assert_eq!(
            first.event.and_then(|event| event.event_type).as_deref(),
            Some("session_meta")
        );

        let second = parse_next(&mut reader, &fields).expect("second event must parse");
        assert_eq!(
            second.event.and_then(|event| event.event_type).as_deref(),
            Some("turn_context")
        );

        let end = parse_next(&mut reader, &fields).expect("end of stream is valid");
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
    fn oversized_record_fails_before_materialization() {
        let mut bytes = b"{\"type\":\"".to_vec();
        bytes.extend(std::iter::repeat_n(b'x', MAX_ROLLOUT_RECORD_BYTES));
        bytes.extend_from_slice(b"\"}\n");
        let error = match parse_next(
            &mut BufReader::new(Cursor::new(bytes)),
            &ReadFields::default(),
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
