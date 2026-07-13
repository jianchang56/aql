use std::io::Read;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use aql_adapter_api::AdapterWarningKind;
use chrono::{DateTime, Utc};
use serde::de::{DeserializeSeed, Deserializer, IgnoredAny, MapAccess, SeqAccess, Visitor};
use serde_json::Value as JsonValue;

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

pub(crate) fn parse_next(
    reader: &mut impl Read,
    fields: &ReadFields,
) -> Result<NextEvent, serde_json::Error> {
    let bytes_read = Arc::new(AtomicU64::new(0));
    let non_whitespace = Arc::new(AtomicU64::new(0));
    let tracking_reader = TrackingReader {
        inner: reader,
        bytes_read: bytes_read.clone(),
        non_whitespace: non_whitespace.clone(),
    };
    let mut deserializer = serde_json::Deserializer::from_reader(tracking_reader);
    match (EventSeed { fields }).deserialize(&mut deserializer) {
        Ok(event) => {
            let warnings = if event.event_type.as_deref().is_some_and(|kind| {
                !matches!(
                    kind,
                    "session_meta" | "turn_context" | "event_msg" | "response_item" | "compacted"
                )
            }) {
                vec![AdapterWarningKind::UnknownEvent]
            } else {
                Vec::new()
            };
            Ok(NextEvent {
                event: Some(event),
                warnings,
                bytes_read: bytes_read.load(Ordering::Acquire),
            })
        }
        Err(error) if error.is_eof() => Ok(NextEvent {
            event: None,
            warnings: if non_whitespace.load(Ordering::Acquire) > 0 {
                vec![AdapterWarningKind::TruncatedRecord]
            } else {
                Vec::new()
            },
            bytes_read: bytes_read.load(Ordering::Acquire),
        }),
        Err(error) => Err(error),
    }
}

#[cfg(test)]
pub(crate) fn parse_stream<F>(
    reader: impl Read,
    fields: ReadFields,
    mut consume: F,
) -> Result<ParseResult, serde_json::Error>
where
    F: FnMut(ParsedEvent) -> bool,
{
    let mut reader = reader;
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

struct TrackingReader<R> {
    inner: R,
    bytes_read: Arc<AtomicU64>,
    non_whitespace: Arc<AtomicU64>,
}

impl<R: Read> Read for TrackingReader<R> {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        if buffer.is_empty() {
            return Ok(0);
        }
        let count = self.inner.read(&mut buffer[..1])?;
        self.bytes_read.fetch_add(count as u64, Ordering::Relaxed);
        if count == 1 && !buffer[0].is_ascii_whitespace() {
            self.non_whitespace.fetch_add(1, Ordering::Relaxed);
        }
        Ok(count)
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
    use std::io::{Cursor, Read};

    use super::*;

    #[test]
    fn fixed_scan_boundary_excludes_appended_events() {
        let initial = b"{\"type\":\"session_meta\",\"payload\":{}}\n";
        let appended = b"{\"type\":\"future_fixture_event\",\"payload\":{}}\n";
        let mut bytes = initial.to_vec();
        bytes.extend_from_slice(appended);
        let mut events = 0;
        let parsed = parse_stream(
            Cursor::new(bytes).take(initial.len() as u64),
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
        let mut reader = Cursor::new(bytes);
        let fields = ReadFields::default();

        let first = parse_next(&mut reader, &fields).expect("first event must parse");
        assert_eq!(
            first.event.and_then(|event| event.event_type).as_deref(),
            Some("session_meta")
        );
        assert!(reader.position() < bytes.len() as u64);

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
        let parsed = parse_stream(Cursor::new(bytes), ReadFields::default(), |_| {
            events += 1;
            true
        })
        .expect("truncated tail is a warning");
        assert_eq!(events, 1);
        assert_eq!(parsed.warnings, vec![AdapterWarningKind::TruncatedRecord]);
    }
}
