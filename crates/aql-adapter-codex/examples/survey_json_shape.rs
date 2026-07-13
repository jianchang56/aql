use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::fs::File;
use std::io::BufReader;
use std::process::ExitCode;

use serde::de::{Deserialize, Deserializer, MapAccess, SeqAccess, Visitor};

#[derive(Clone, Debug, Default)]
struct Shape {
    kinds: BTreeSet<&'static str>,
    fields: BTreeMap<String, Shape>,
    element: Option<Box<Shape>>,
}

impl Shape {
    fn scalar(kind: &'static str) -> Self {
        Self {
            kinds: BTreeSet::from([kind]),
            fields: BTreeMap::new(),
            element: None,
        }
    }

    fn merge(&mut self, other: Self) {
        self.kinds.extend(other.kinds);
        for (key, value) in other.fields {
            self.fields.entry(key).or_default().merge(value);
        }
        if let Some(other_element) = other.element {
            self.element
                .get_or_insert_with(|| Box::new(Shape::default()))
                .merge(*other_element);
        }
    }

    fn flatten(&self, prefix: &str, output: &mut BTreeMap<String, BTreeSet<&'static str>>) {
        if !prefix.is_empty() && !self.kinds.is_empty() {
            output
                .entry(prefix.to_string())
                .or_default()
                .extend(self.kinds.iter().copied());
        }
        for (key, child) in &self.fields {
            let path = if prefix.is_empty() {
                key.clone()
            } else {
                format!("{prefix}.{key}")
            };
            child.flatten(&path, output);
        }
        if let Some(element) = &self.element {
            let path = if prefix.is_empty() {
                "[]".to_string()
            } else {
                format!("{prefix}[]")
            };
            element.flatten(&path, output);
        }
    }
}

impl<'de> Deserialize<'de> for Shape {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_any(ShapeVisitor)
    }
}

struct ShapeVisitor;

impl<'de> Visitor<'de> for ShapeVisitor {
    type Value = Shape;

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("any JSON value")
    }

    fn visit_bool<E>(self, _value: bool) -> Result<Self::Value, E> {
        Ok(Shape::scalar("bool"))
    }

    fn visit_i64<E>(self, _value: i64) -> Result<Self::Value, E> {
        Ok(Shape::scalar("number"))
    }

    fn visit_u64<E>(self, _value: u64) -> Result<Self::Value, E> {
        Ok(Shape::scalar("number"))
    }

    fn visit_f64<E>(self, _value: f64) -> Result<Self::Value, E> {
        Ok(Shape::scalar("number"))
    }

    fn visit_str<E>(self, _value: &str) -> Result<Self::Value, E> {
        Ok(Shape::scalar("string"))
    }

    fn visit_string<E>(self, _value: String) -> Result<Self::Value, E> {
        Ok(Shape::scalar("string"))
    }

    fn visit_none<E>(self) -> Result<Self::Value, E> {
        Ok(Shape::scalar("null"))
    }

    fn visit_unit<E>(self) -> Result<Self::Value, E> {
        Ok(Shape::scalar("null"))
    }

    fn visit_some<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        Shape::deserialize(deserializer)
    }

    fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let mut element = Shape::default();
        while let Some(shape) = sequence.next_element::<Shape>()? {
            element.merge(shape);
        }
        Ok(Shape {
            kinds: BTreeSet::from(["array"]),
            fields: BTreeMap::new(),
            element: Some(Box::new(element)),
        })
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut fields = BTreeMap::new();
        while let Some(key) = map.next_key::<String>()? {
            let value = map.next_value::<Shape>()?;
            fields
                .entry(key)
                .or_insert_with(Shape::default)
                .merge(value);
        }
        Ok(Shape {
            kinds: BTreeSet::from(["object"]),
            fields,
            element: None,
        })
    }
}

fn survey_file(path: &str, combined: &mut Shape) -> Result<u64, String> {
    let file = File::open(path).map_err(|error| error.to_string())?;
    let reader = BufReader::new(file);
    let stream = serde_json::Deserializer::from_reader(reader).into_iter::<Shape>();
    let mut documents = 0;
    for document in stream {
        match document {
            Ok(shape) => {
                combined.merge(shape);
                documents += 1;
            }
            Err(error) if error.is_eof() => break,
            Err(error) => return Err(format!("invalid_json:{error}")),
        }
    }
    Ok(documents)
}

fn redact_dynamic_path_keys(shape: &mut Shape) {
    let Some(changes) = shape
        .fields
        .get_mut("payload")
        .and_then(|payload| payload.fields.get_mut("changes"))
    else {
        return;
    };
    let mut merged = Shape::default();
    for (_, value) in std::mem::take(&mut changes.fields) {
        merged.merge(value);
    }
    if !merged.kinds.is_empty() || !merged.fields.is_empty() || merged.element.is_some() {
        changes.fields.insert("<path-key>".to_string(), merged);
    }
}

fn main() -> ExitCode {
    let paths: Vec<String> = env::args().skip(1).collect();
    if paths.is_empty() {
        eprintln!("usage=survey_json_shape <jsonl>...");
        return ExitCode::from(2);
    }

    let mut combined = Shape::default();
    let mut documents = 0_u64;
    for path in &paths {
        match survey_file(path, &mut combined) {
            Ok(count) => documents += count,
            Err(error) => {
                eprintln!("error={error}");
                return ExitCode::from(3);
            }
        }
    }

    redact_dynamic_path_keys(&mut combined);
    let mut flattened = BTreeMap::new();
    combined.flatten("", &mut flattened);
    println!("documents={documents}");
    for (path, kinds) in flattened {
        println!(
            "shape.{path}={}",
            kinds.into_iter().collect::<Vec<_>>().join("|")
        );
    }
    ExitCode::SUCCESS
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn patch_change_paths_are_never_rendered_as_shape_keys() {
        let mut shape: Shape = serde_json::from_str(
            r#"{"payload":{"changes":{"/synthetic/private/file.txt":{"type":"add","content":"synthetic"}}}}"#,
        )
        .expect("synthetic shape must parse");
        redact_dynamic_path_keys(&mut shape);
        let mut flattened = BTreeMap::new();
        shape.flatten("", &mut flattened);
        assert!(flattened.keys().all(|key| !key.contains("private")));
        assert!(flattened.contains_key("payload.changes.<path-key>.type"));
    }
}
