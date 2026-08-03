use super::*;
use std::borrow::Cow;
use std::io::{Seek, SeekFrom};
use unicode_width::UnicodeWidthStr;

pub(super) struct RenderSummary {
    pub(super) returned_rows: usize,
    pub(super) formula_escaped: bool,
}

pub(super) struct StreamingRenderer {
    state: RendererState,
    budget: ResourceBudget,
    returned_rows: usize,
}

enum RendererState {
    Table(TableSpool),
    Json {
        first: bool,
    },
    Jsonl {
        first: bool,
    },
    Csv {
        schema: Option<datafusion::arrow::datatypes::SchemaRef>,
        formula_escaped: bool,
    },
}

impl StreamingRenderer {
    pub(super) fn new(
        output: Output,
        budget: ResourceBudget,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let state = match output {
            Output::Table => RendererState::Table(TableSpool::new()?),
            Output::Json => RendererState::Json { first: true },
            Output::Jsonl => RendererState::Jsonl { first: true },
            Output::Csv => RendererState::Csv {
                schema: None,
                formula_escaped: false,
            },
        };
        Ok(Self {
            state,
            budget,
            returned_rows: 0,
        })
    }

    pub(super) fn start(
        &mut self,
        writer: &mut impl Write,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if matches!(self.state, RendererState::Json { .. }) {
            write_budgeted(writer, b"[", &self.budget)?;
        }
        Ok(())
    }

    pub(super) fn write_batch(
        &mut self,
        writer: &mut impl Write,
        batch: &RecordBatch,
    ) -> Result<(), Box<dyn std::error::Error>> {
        self.returned_rows = self
            .returned_rows
            .checked_add(batch.num_rows())
            .ok_or("returned row count overflow")?;
        match &mut self.state {
            RendererState::Table(table) => table.append_batch(batch, &self.budget)?,
            RendererState::Json { first } => {
                let names = batch_field_names(batch);
                for row_index in 0..batch.num_rows() {
                    if !*first {
                        write_budgeted(writer, b",", &self.budget)?;
                    }
                    let row = serde_json::to_vec(&batch_row_to_value(batch, &names, row_index)?)?;
                    write_budgeted(writer, &row, &self.budget)?;
                    *first = false;
                }
            }
            RendererState::Jsonl { first } => {
                let names = batch_field_names(batch);
                for row_index in 0..batch.num_rows() {
                    if !*first {
                        write_budgeted(writer, b"\n", &self.budget)?;
                    }
                    let row = serde_json::to_vec(&batch_row_to_value(batch, &names, row_index)?)?;
                    write_budgeted(writer, &row, &self.budget)?;
                    *first = false;
                }
            }
            RendererState::Csv {
                schema,
                formula_escaped,
            } => {
                if let Some(expected) = schema {
                    if batch.schema() != *expected {
                        return Err("CSV batches have inconsistent schemas".into());
                    }
                } else {
                    *schema = Some(batch.schema());
                    for (index, field) in batch.schema().fields().iter().enumerate() {
                        if index > 0 {
                            write_budgeted(writer, b",", &self.budget)?;
                        }
                        let mut name = field.name().clone();
                        if starts_csv_formula(&name) {
                            name.insert(0, '\'');
                            *formula_escaped = true;
                        }
                        let header = csv_quote(&name, false)?;
                        write_budgeted(writer, header.as_bytes(), &self.budget)?;
                    }
                    write_budgeted(writer, b"\r\n", &self.budget)?;
                }
                for row_index in 0..batch.num_rows() {
                    for column_index in 0..batch.num_columns() {
                        if column_index > 0 {
                            write_budgeted(writer, b",", &self.budget)?;
                        }
                        let cell = csv_arrow_cell(batch, column_index, row_index, formula_escaped)?;
                        write_budgeted(writer, cell.as_bytes(), &self.budget)?;
                    }
                    write_budgeted(writer, b"\r\n", &self.budget)?;
                }
            }
        }
        Ok(())
    }

    pub(super) fn finish(
        mut self,
        writer: &mut impl Write,
    ) -> Result<RenderSummary, Box<dyn std::error::Error>> {
        let formula_escaped = match &mut self.state {
            RendererState::Table(table) => {
                table.render(writer, &self.budget)?;
                false
            }
            RendererState::Json { .. } => {
                write_budgeted(writer, b"]\n", &self.budget)?;
                false
            }
            RendererState::Jsonl { .. } => {
                write_budgeted(writer, b"\n", &self.budget)?;
                false
            }
            RendererState::Csv {
                schema,
                formula_escaped,
            } => {
                if schema.is_none() {
                    write_budgeted(writer, b"\n", &self.budget)?;
                }
                *formula_escaped
            }
        };
        Ok(RenderSummary {
            returned_rows: self.returned_rows,
            formula_escaped,
        })
    }
}

fn write_budgeted(
    writer: &mut impl Write,
    bytes: &[u8],
    budget: &ResourceBudget,
) -> Result<(), Box<dyn std::error::Error>> {
    budget.charge_output_bytes(bytes.len() as u64)?;
    writer.write_all(bytes)?;
    Ok(())
}

struct TableSpool {
    file: io::BufWriter<fs::File>,
    schema: Option<datafusion::arrow::datatypes::SchemaRef>,
    widths: Vec<usize>,
    rows: usize,
    data_lines: u64,
    data_extra_bytes: u64,
}

impl TableSpool {
    fn new() -> io::Result<Self> {
        Ok(Self {
            file: io::BufWriter::new(tempfile::tempfile()?),
            schema: None,
            widths: Vec::new(),
            rows: 0,
            data_lines: 0,
            data_extra_bytes: 0,
        })
    }

    fn append_batch(
        &mut self,
        batch: &RecordBatch,
        budget: &ResourceBudget,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(schema) = &self.schema {
            if batch.schema() != *schema {
                return Err("table batches have inconsistent schemas".into());
            }
        } else {
            self.schema = Some(batch.schema());
            self.widths = batch
                .schema()
                .fields()
                .iter()
                .map(|field| max_line_width(&sanitize_table_text(field.name())))
                .collect();
        }
        let options =
            datafusion::arrow::util::display::FormatOptions::default().with_display_error(true);
        let formatters = batch
            .columns()
            .iter()
            .map(|column| {
                datafusion::arrow::util::display::ArrayFormatter::try_new(column.as_ref(), &options)
            })
            .collect::<Result<Vec<_>, _>>()?;
        for row_index in 0..batch.num_rows() {
            let cells = formatters
                .iter()
                .map(|formatter| {
                    let raw = formatter.value(row_index).to_string();
                    match sanitize_table_text(&raw) {
                        Cow::Borrowed(_) => raw,
                        Cow::Owned(sanitized) => sanitized,
                    }
                })
                .collect::<Vec<_>>();
            let height = cells
                .iter()
                .map(|cell| cell.split('\n').count())
                .max()
                .unwrap_or(1);
            self.data_lines = self
                .data_lines
                .checked_add(height as u64)
                .ok_or("table output size overflow")?;
            for (column_index, cell) in cells.iter().enumerate() {
                for line in cell.split('\n') {
                    let width = UnicodeWidthStr::width(line);
                    self.widths[column_index] = self.widths[column_index].max(width);
                    self.data_extra_bytes = self
                        .data_extra_bytes
                        .checked_add((line.len() - width) as u64)
                        .ok_or("table output size overflow")?;
                }
                let bytes = cell.as_bytes();
                self.file.write_all(&(bytes.len() as u64).to_le_bytes())?;
                self.file.write_all(bytes)?;
            }
            self.rows = self.rows.checked_add(1).ok_or("table row count overflow")?;
            self.ensure_within_budget(budget)?;
        }
        self.ensure_within_budget(budget)?;
        Ok(())
    }

    fn ensure_within_budget(
        &self,
        budget: &ResourceBudget,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let estimated = self.estimated_output_bytes()?;
        let available = budget
            .max_output_bytes
            .saturating_sub(budget.output_bytes_used());
        if estimated > available {
            return Err(aql_adapter_api::AdapterError::BudgetExceeded {
                resource: "output_bytes".to_string(),
                actual: budget.output_bytes_used().saturating_add(estimated),
            }
            .into());
        }
        Ok(())
    }

    fn estimated_output_bytes(&self) -> Result<u64, Box<dyn std::error::Error>> {
        let Some(schema) = &self.schema else {
            return Ok(1);
        };
        let header_lines = schema
            .fields()
            .iter()
            .map(|field| sanitize_table_text(field.name()).split('\n').count())
            .max()
            .unwrap_or(1) as u64;
        let header_extra = schema.fields().iter().try_fold(0_u64, |total, field| {
            sanitize_table_text(field.name())
                .split('\n')
                .try_fold(total, |total, line| {
                    total
                        .checked_add((line.len() - UnicodeWidthStr::width(line)) as u64)
                        .ok_or("table output size overflow")
                })
        })?;
        let line_width = self.widths.iter().try_fold(1_u64, |total, width| {
            total
                .checked_add(*width as u64 + 3)
                .ok_or("table output size overflow")
        })?;
        let lines = 3_u64
            .checked_add(header_lines)
            .and_then(|value| value.checked_add(self.data_lines))
            .ok_or("table output size overflow")?;
        line_width
            .checked_add(1)
            .and_then(|width| width.checked_mul(lines))
            .and_then(|value| value.checked_add(header_extra))
            .and_then(|value| value.checked_add(self.data_extra_bytes))
            .ok_or_else(|| "table output size overflow".into())
    }

    fn render(
        &mut self,
        writer: &mut impl Write,
        budget: &ResourceBudget,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let Some(schema) = self.schema.clone() else {
            write_budgeted(writer, b"\n", budget)?;
            return Ok(());
        };
        self.ensure_within_budget(budget)?;
        write_separator(writer, &self.widths, budget)?;
        let headers = schema
            .fields()
            .iter()
            .map(|field| sanitize_table_text(field.name()).into_owned())
            .collect::<Vec<_>>();
        write_table_row(writer, &headers, &self.widths, budget)?;
        write_separator(writer, &self.widths, budget)?;
        self.file.flush()?;
        let mut spool_file = self.file.get_ref();
        spool_file.seek(SeekFrom::Start(0))?;
        let mut reader = io::BufReader::new(spool_file);
        for _ in 0..self.rows {
            let mut cells = Vec::with_capacity(self.widths.len());
            for _ in &self.widths {
                let mut length = [0_u8; 8];
                reader.read_exact(&mut length)?;
                let length = usize::try_from(u64::from_le_bytes(length))
                    .map_err(|_| "table cell is too large")?;
                let mut bytes = vec![0_u8; length];
                reader.read_exact(&mut bytes)?;
                cells.push(String::from_utf8(bytes)?);
            }
            write_table_row(writer, &cells, &self.widths, budget)?;
        }
        write_separator(writer, &self.widths, budget)?;
        Ok(())
    }
}

fn max_line_width(value: &str) -> usize {
    value
        .split('\n')
        .map(UnicodeWidthStr::width)
        .max()
        .unwrap_or(0)
}

fn sanitize_table_text(value: &str) -> Cow<'_, str> {
    let is_dangerous = |character: char| character.is_control() && character != '\n';
    if !value.chars().any(is_dangerous) {
        return Cow::Borrowed(value);
    }
    let mut sanitized = String::with_capacity(value.len());
    for character in value.chars() {
        if is_dangerous(character) {
            sanitized.push('\u{FFFD}');
        } else {
            sanitized.push(character);
        }
    }
    Cow::Owned(sanitized)
}

fn write_separator(
    writer: &mut impl Write,
    widths: &[usize],
    budget: &ResourceBudget,
) -> Result<(), Box<dyn std::error::Error>> {
    write_budgeted(writer, b"+", budget)?;
    for width in widths {
        write_budgeted(writer, vec![b'-'; width + 2].as_slice(), budget)?;
        write_budgeted(writer, b"+", budget)?;
    }
    write_budgeted(writer, b"\n", budget)?;
    Ok(())
}

fn write_table_row(
    writer: &mut impl Write,
    cells: &[String],
    widths: &[usize],
    budget: &ResourceBudget,
) -> Result<(), Box<dyn std::error::Error>> {
    let lines = cells
        .iter()
        .map(|cell| cell.split('\n').collect::<Vec<_>>())
        .collect::<Vec<_>>();
    let height = lines.iter().map(Vec::len).max().unwrap_or(1);
    let padding = vec![b' '; widths.iter().copied().max().map_or(1, |width| width + 1)];
    for line_index in 0..height {
        write_budgeted(writer, b"|", budget)?;
        for (column_index, width) in widths.iter().enumerate() {
            let value = lines[column_index].get(line_index).copied().unwrap_or("");
            write_budgeted(writer, b" ", budget)?;
            write_budgeted(writer, value.as_bytes(), budget)?;
            let padding_len = width.saturating_sub(UnicodeWidthStr::width(value)) + 1;
            write_budgeted(writer, &padding[..padding_len], budget)?;
            write_budgeted(writer, b"|", budget)?;
        }
        write_budgeted(writer, b"\n", budget)?;
    }
    Ok(())
}

#[cfg(test)]
pub(super) struct CsvRendering {
    pub(super) rendered: String,
    pub(super) formula_escaped: bool,
}

#[cfg(test)]
pub(super) fn batches_to_csv(
    batches: &[RecordBatch],
) -> Result<CsvRendering, Box<dyn std::error::Error>> {
    let (rendered, summary) = render_batches_for_test(Output::Csv, batches, u64::MAX)?;
    Ok(CsvRendering {
        rendered: if batches.is_empty() {
            String::new()
        } else {
            rendered
        },
        formula_escaped: summary.formula_escaped,
    })
}

#[cfg(test)]
pub(super) fn render_batches_for_test(
    output: Output,
    batches: &[RecordBatch],
    max_bytes: u64,
) -> Result<(String, RenderSummary), Box<dyn std::error::Error>> {
    let budget = ResourceBudget {
        max_output_bytes: max_bytes,
        ..ResourceBudget::default()
    };
    let mut writer = Vec::new();
    let mut renderer = StreamingRenderer::new(output, budget)?;
    renderer.start(&mut writer)?;
    for batch in batches {
        renderer.write_batch(&mut writer, batch)?;
    }
    let summary = renderer.finish(&mut writer)?;
    Ok((String::from_utf8(writer)?, summary))
}

#[cfg(test)]
pub(super) fn batches_to_json_limited(
    batches: &[RecordBatch],
    max_bytes: u64,
) -> Result<String, Box<dyn std::error::Error>> {
    Ok(render_batches_for_test(Output::Json, batches, max_bytes)?.0)
}

#[cfg(test)]
pub(super) fn batches_to_jsonl_limited(
    batches: &[RecordBatch],
    max_bytes: u64,
) -> Result<String, Box<dyn std::error::Error>> {
    Ok(render_batches_for_test(Output::Jsonl, batches, max_bytes)?.0)
}

#[cfg(test)]
pub(super) fn batches_to_csv_limited(
    batches: &[RecordBatch],
    max_bytes: u64,
) -> Result<CsvRendering, Box<dyn std::error::Error>> {
    let (rendered, summary) = render_batches_for_test(Output::Csv, batches, max_bytes)?;
    Ok(CsvRendering {
        rendered,
        formula_escaped: summary.formula_escaped,
    })
}

#[cfg(test)]
pub(super) fn batches_to_table_limited(
    batches: &[RecordBatch],
    max_bytes: u64,
) -> Result<String, Box<dyn std::error::Error>> {
    Ok(render_batches_for_test(Output::Table, batches, max_bytes)?.0)
}

fn csv_arrow_cell(
    batch: &RecordBatch,
    column_index: usize,
    row_index: usize,
    formula_escaped: &mut bool,
) -> Result<String, Box<dyn std::error::Error>> {
    use datafusion::arrow::array::Array;

    if batch.column(column_index).is_null(row_index) {
        return Ok("\\N".to_string());
    }
    let value = arrow_json_value(batch, column_index, row_index)?;
    match value {
        serde_json::Value::Null => Ok("\\N".to_string()),
        serde_json::Value::Bool(value) => Ok(value.to_string()),
        serde_json::Value::Number(value) => Ok(value.to_string()),
        serde_json::Value::String(mut value) => {
            // Formula safety never depends on the output column name: aliases
            // are user-controllable, so every string cell is escaped the same way.
            if starts_csv_formula(&value) {
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
    let first = value.as_bytes().first();
    if first.is_some_and(|byte| matches!(byte, b'=' | b'+' | b'-' | b'@' | b'\t' | b'\r')) {
        return true;
    }
    // Some spreadsheet imports trim leading whitespace before detecting
    // formulas, so whitespace-prefixed formula characters are dangerous too.
    value
        .trim_start_matches([' ', '\t', '\r', '\n'])
        .as_bytes()
        .first()
        .is_some_and(|byte| matches!(byte, b'=' | b'+' | b'-' | b'@'))
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
        let names = batch_field_names(batch);
        for row_index in 0..batch.num_rows() {
            rows.push(batch_row_to_value(batch, &names, row_index)?);
        }
    }
    Ok(rows)
}

fn batch_field_names(batch: &RecordBatch) -> Vec<String> {
    batch
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect()
}

pub(super) fn batch_row_to_value(
    batch: &RecordBatch,
    names: &[String],
    row_index: usize,
) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
    let mut row = serde_json::Map::with_capacity(names.len());
    for (column_index, name) in names.iter().enumerate() {
        row.insert(
            name.clone(),
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
        Array, BooleanArray, Float64Array, Int32Array, Int64Array, StringArray,
        TimestampMillisecondArray,
    };
    use datafusion::arrow::datatypes::DataType;

    let array = batch.column(column_index);
    if array.is_null(row_index) {
        return Ok(serde_json::Value::Null);
    }
    let schema = batch.schema();
    let field = schema.field(column_index);
    match field.data_type() {
        DataType::Utf8 => {
            let value = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or("invalid UTF-8 Arrow array")?
                .value(row_index);
            // Canonical JSON columns are marked by the engine through Arrow
            // field metadata: output names are user-controllable aliases and
            // cannot decide whether a text cell holds embedded JSON.
            if field
                .metadata()
                .get(aql_engine_datafusion::JSON_TYPE_METADATA_KEY)
                .is_some_and(|kind| kind == aql_engine_datafusion::JSON_TYPE_METADATA_VALUE)
            {
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
        DataType::Int32 => Ok(serde_json::Value::Number(
            array
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or("invalid Int32 Arrow array")?
                .value(row_index)
                .into(),
        )),
        DataType::Float64 => {
            let value = array
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or("invalid Float64 Arrow array")?
                .value(row_index);
            let number = serde_json::Number::from_f64(value)
                .ok_or("non-finite Float64 output is unsupported")?;
            Ok(serde_json::Value::Number(number))
        }
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
        data_type => Err(format!("unsupported Arrow output type: {data_type:?}").into()),
    }
}
