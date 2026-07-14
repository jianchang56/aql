use super::*;

pub(super) fn batches_to_json_limited(
    batches: &[RecordBatch],
    max_bytes: u64,
) -> Result<String, Box<dyn std::error::Error>> {
    let mut rendered = LimitedString::new(max_bytes);
    rendered.push_str("[")?;
    let mut first = true;
    for batch in batches {
        for row_index in 0..batch.num_rows() {
            if !first {
                rendered.push_str(",")?;
            }
            let row = serde_json::to_string(&batch_row_to_value(batch, row_index)?)?;
            rendered.push_str(&row)?;
            first = false;
        }
    }
    rendered.push_str("]")?;
    Ok(rendered.finish())
}

pub(super) fn batches_to_table_limited(
    batches: &[RecordBatch],
    max_bytes: u64,
) -> Result<String, Box<dyn std::error::Error>> {
    let mut rendered = LimitedString::new(max_bytes);
    let pretty = datafusion::arrow::util::pretty::pretty_format_batches(batches)?;
    if std::fmt::Write::write_fmt(&mut rendered, format_args!("{pretty}")).is_err() {
        if rendered.overflowed {
            return Err("resource budget exceeded: output_bytes".into());
        }
        return Err("table rendering failed".into());
    }
    Ok(rendered.finish())
}

pub(super) fn batches_to_jsonl_limited(
    batches: &[RecordBatch],
    max_bytes: u64,
) -> Result<String, Box<dyn std::error::Error>> {
    let mut rendered = LimitedString::new(max_bytes);
    let mut first = true;
    for batch in batches {
        for row_index in 0..batch.num_rows() {
            if !first {
                rendered.push_str("\n")?;
            }
            let row = serde_json::to_string(&batch_row_to_value(batch, row_index)?)?;
            rendered.push_str(&row)?;
            first = false;
        }
    }
    Ok(rendered.finish())
}

pub(super) struct CsvRendering {
    pub(super) rendered: String,
    pub(super) formula_escaped: bool,
}

#[cfg(test)]
pub(super) fn batches_to_csv(
    batches: &[RecordBatch],
) -> Result<CsvRendering, Box<dyn std::error::Error>> {
    batches_to_csv_limited(batches, u64::MAX)
}

pub(super) fn batches_to_csv_limited(
    batches: &[RecordBatch],
    max_bytes: u64,
) -> Result<CsvRendering, Box<dyn std::error::Error>> {
    let Some(first) = batches.first() else {
        return Ok(CsvRendering {
            rendered: String::new(),
            formula_escaped: false,
        });
    };
    let schema = first.schema();
    let mut rendered = LimitedString::new(max_bytes);
    for (index, field) in schema.fields().iter().enumerate() {
        if index > 0 {
            rendered.push_str(",")?;
        }
        rendered.push_str(&csv_quote(field.name(), false)?)?;
    }
    rendered.push_str("\r\n")?;
    let mut formula_escaped = false;
    for batch in batches {
        if batch.schema() != schema {
            return Err("CSV batches have inconsistent schemas".into());
        }
        for row_index in 0..batch.num_rows() {
            for column_index in 0..batch.num_columns() {
                if column_index > 0 {
                    rendered.push_str(",")?;
                }
                let cell = csv_arrow_cell(batch, column_index, row_index, &mut formula_escaped)?;
                rendered.push_str(&cell)?;
            }
            rendered.push_str("\r\n")?;
        }
    }
    Ok(CsvRendering {
        rendered: rendered.finish(),
        formula_escaped,
    })
}

struct LimitedString {
    value: String,
    max_bytes: u64,
    overflowed: bool,
}

impl LimitedString {
    fn new(max_bytes: u64) -> Self {
        Self {
            value: String::new(),
            max_bytes,
            overflowed: false,
        }
    }

    fn push_str(&mut self, value: &str) -> Result<(), Box<dyn std::error::Error>> {
        let next = (self.value.len() as u64)
            .checked_add(value.len() as u64)
            .ok_or("rendered output size overflow")?;
        if next > self.max_bytes {
            return Err("resource budget exceeded: output_bytes".into());
        }
        self.value.push_str(value);
        Ok(())
    }

    fn finish(self) -> String {
        self.value
    }
}

impl std::fmt::Write for LimitedString {
    fn write_str(&mut self, value: &str) -> std::fmt::Result {
        let next = (self.value.len() as u64)
            .checked_add(value.len() as u64)
            .ok_or(std::fmt::Error)?;
        if next > self.max_bytes {
            self.overflowed = true;
            return Err(std::fmt::Error);
        }
        self.value.push_str(value);
        Ok(())
    }
}

fn csv_arrow_cell(
    batch: &RecordBatch,
    column_index: usize,
    row_index: usize,
    formula_escaped: &mut bool,
) -> Result<String, Box<dyn std::error::Error>> {
    use datafusion::arrow::array::Array;
    use datafusion::arrow::datatypes::DataType;

    if batch.column(column_index).is_null(row_index) {
        return Ok("\\N".to_string());
    }
    let field = batch.schema().field(column_index).clone();
    let value = arrow_json_value(batch, column_index, row_index)?;
    match value {
        serde_json::Value::Null => Ok("\\N".to_string()),
        serde_json::Value::Bool(value) => Ok(value.to_string()),
        serde_json::Value::Number(value) => Ok(value.to_string()),
        serde_json::Value::String(mut value) => {
            let formula_sensitive = matches!(field.data_type(), DataType::Utf8)
                && !matches!(
                    field.name().as_str(),
                    "capabilities" | "content_json" | "arguments"
                );
            if formula_sensitive && starts_csv_formula(&value) {
                value.insert(0, '\'');
                *formula_escaped = true;
            }
            let force_quote = value.is_empty() || value == "\\N";
            csv_quote(&value, force_quote)
        }
        serde_json::Value::Array(_) | serde_json::Value::Object(_) => {
            csv_quote(&serde_json::to_string(&value)?, false)
        }
    }
}

fn starts_csv_formula(value: &str) -> bool {
    value
        .as_bytes()
        .first()
        .is_some_and(|byte| matches!(byte, b'=' | b'+' | b'-' | b'@' | b'\t' | b'\r'))
}

fn csv_quote(value: &str, force: bool) -> Result<String, Box<dyn std::error::Error>> {
    if value.chars().any(|character| {
        character == '\0' || (character.is_control() && !matches!(character, '\t' | '\r' | '\n'))
    }) {
        return Err("CSV text contains an unsupported control character".into());
    }
    let quoted = force
        || value
            .as_bytes()
            .iter()
            .any(|byte| matches!(byte, b',' | b'"' | b'\r' | b'\n'));
    if !quoted {
        return Ok(value.to_string());
    }
    let mut output = String::with_capacity(value.len() + 2);
    output.push('"');
    for character in value.chars() {
        if character == '"' {
            output.push('"');
        }
        output.push(character);
    }
    output.push('"');
    Ok(output)
}

#[cfg(test)]
pub(super) fn batches_to_values(
    batches: &[RecordBatch],
) -> Result<Vec<serde_json::Value>, Box<dyn std::error::Error>> {
    let mut rows = Vec::new();
    for batch in batches {
        for row_index in 0..batch.num_rows() {
            rows.push(batch_row_to_value(batch, row_index)?);
        }
    }
    Ok(rows)
}

pub(super) fn batch_row_to_value(
    batch: &RecordBatch,
    row_index: usize,
) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
    let mut row = serde_json::Map::new();
    for (column_index, field) in batch.schema().fields().iter().enumerate() {
        row.insert(
            field.name().clone(),
            arrow_json_value(batch, column_index, row_index)?,
        );
    }
    Ok(serde_json::Value::Object(row))
}

pub(super) fn arrow_json_value(
    batch: &RecordBatch,
    column_index: usize,
    row_index: usize,
) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
    use datafusion::arrow::array::{
        Array, BooleanArray, Int64Array, StringArray, TimestampMillisecondArray,
    };
    use datafusion::arrow::datatypes::DataType;

    let array = batch.column(column_index);
    if array.is_null(row_index) {
        return Ok(serde_json::Value::Null);
    }
    let field = batch.schema().field(column_index).clone();
    match field.data_type() {
        DataType::Utf8 => {
            let value = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or("invalid UTF-8 Arrow array")?
                .value(row_index);
            if matches!(
                field.name().as_str(),
                "capabilities" | "content_json" | "arguments"
            ) {
                Ok(serde_json::from_str(value)?)
            } else {
                Ok(serde_json::Value::String(value.to_string()))
            }
        }
        DataType::Int64 => Ok(serde_json::Value::Number(
            array
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or("invalid Int64 Arrow array")?
                .value(row_index)
                .into(),
        )),
        DataType::Boolean => Ok(serde_json::Value::Bool(
            array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or("invalid Boolean Arrow array")?
                .value(row_index),
        )),
        DataType::Timestamp(datafusion::arrow::datatypes::TimeUnit::Millisecond, _) => {
            let millis = array
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .ok_or("invalid timestamp Arrow array")?
                .value(row_index);
            let timestamp = chrono::DateTime::from_timestamp_millis(millis)
                .ok_or("timestamp is outside the supported range")?;
            Ok(serde_json::Value::String(timestamp.to_rfc3339()))
        }
        _ => Err("unsupported Arrow output type".into()),
    }
}
