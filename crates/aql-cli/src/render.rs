use super::*;

pub(super) fn batches_to_json(
    batches: &[RecordBatch],
) -> Result<String, Box<dyn std::error::Error>> {
    Ok(serde_json::to_string(&batches_to_values(batches)?)?)
}

pub(super) fn batches_to_jsonl(
    batches: &[RecordBatch],
) -> Result<String, Box<dyn std::error::Error>> {
    let rows = batches_to_values(batches)?;
    Ok(rows
        .into_iter()
        .map(|row| serde_json::to_string(&row))
        .collect::<Result<Vec<_>, _>>()?
        .join("\n"))
}

pub(super) struct CsvRendering {
    pub(super) rendered: String,
    pub(super) formula_escaped: bool,
}

pub(super) fn validate_csv_options(
    output: Output,
    formulas: CsvFormulaMode,
    acknowledge_raw: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    if output != Output::Csv && (formulas != CsvFormulaMode::Safe || acknowledge_raw) {
        return Err("CSV formula options require --output csv".into());
    }
    if output == Output::Csv && formulas == CsvFormulaMode::Raw && !acknowledge_raw {
        return Err("raw CSV formulas require --acknowledge-raw-csv-formulas".into());
    }
    if formulas == CsvFormulaMode::Safe && acknowledge_raw {
        return Err("--acknowledge-raw-csv-formulas requires --csv-formulas raw".into());
    }
    Ok(())
}

pub(super) fn batches_to_csv(
    batches: &[RecordBatch],
    formulas: CsvFormulaMode,
) -> Result<CsvRendering, Box<dyn std::error::Error>> {
    let Some(first) = batches.first() else {
        return Ok(CsvRendering {
            rendered: String::new(),
            formula_escaped: false,
        });
    };
    let schema = first.schema();
    let mut rendered = String::new();
    for (index, field) in schema.fields().iter().enumerate() {
        if index > 0 {
            rendered.push(',');
        }
        rendered.push_str(&csv_quote(field.name(), false)?);
    }
    rendered.push_str("\r\n");
    let mut formula_escaped = false;
    for batch in batches {
        if batch.schema() != schema {
            return Err("CSV batches have inconsistent schemas".into());
        }
        for row_index in 0..batch.num_rows() {
            for column_index in 0..batch.num_columns() {
                if column_index > 0 {
                    rendered.push(',');
                }
                let cell = csv_arrow_cell(
                    batch,
                    column_index,
                    row_index,
                    formulas,
                    &mut formula_escaped,
                )?;
                rendered.push_str(&cell);
            }
            rendered.push_str("\r\n");
        }
    }
    Ok(CsvRendering {
        rendered,
        formula_escaped,
    })
}

fn csv_arrow_cell(
    batch: &RecordBatch,
    column_index: usize,
    row_index: usize,
    formulas: CsvFormulaMode,
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
            if formula_sensitive && formulas == CsvFormulaMode::Safe && starts_csv_formula(&value) {
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
