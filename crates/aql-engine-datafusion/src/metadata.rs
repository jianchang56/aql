use super::*;

#[derive(Clone)]
pub(super) enum MetadataCell {
    Text(String),
    Bool(bool),
    Int64(i64),
}

pub(super) fn metadata_rows(table: &str, sources: &[FederatedSource]) -> Vec<Vec<MetadataCell>> {
    match table {
        "aql_tables" => QUERY_SCHEMAS
            .iter()
            .map(|schema| {
                vec![
                    MetadataCell::Text(schema.name.to_string()),
                    MetadataCell::Text(
                        if schema.name.starts_with("aql_") {
                            "metadata"
                        } else {
                            "canonical"
                        }
                        .to_string(),
                    ),
                ]
            })
            .collect(),
        "aql_columns" => QUERY_SCHEMAS
            .iter()
            .flat_map(|schema| {
                schema
                    .columns
                    .iter()
                    .enumerate()
                    .map(move |(index, column)| {
                        vec![
                            MetadataCell::Text(schema.name.to_string()),
                            MetadataCell::Text(column.name.to_string()),
                            MetadataCell::Int64(index as i64 + 1),
                            MetadataCell::Text(data_type_name(column.data_type).to_string()),
                            MetadataCell::Bool(column.nullable),
                            MetadataCell::Text(access_class_name(column.access).to_string()),
                        ]
                    })
            })
            .collect(),
        "aql_sources" => sources
            .iter()
            .map(|source| {
                vec![
                    MetadataCell::Text(source.manifest.source_id.to_string()),
                    MetadataCell::Text(source.manifest.agent_id.clone()),
                    MetadataCell::Text(source.manifest.display_name.clone()),
                    MetadataCell::Text(source.manifest.format_fingerprint.clone()),
                    MetadataCell::Text(
                        if source.manifest.snapshot.is_some() {
                            "weak"
                        } else {
                            "unavailable"
                        }
                        .to_string(),
                    ),
                ]
            })
            .collect(),
        "aql_capabilities" => sources
            .iter()
            .flat_map(|source| {
                QUERY_SCHEMAS
                    .iter()
                    .filter(|schema| !schema.name.starts_with("aql_"))
                    .map(move |schema| {
                        vec![
                            MetadataCell::Text(source.manifest.source_id.to_string()),
                            MetadataCell::Text(schema.name.to_string()),
                            MetadataCell::Bool(
                                schema.name == "agents"
                                    || source
                                        .manifest
                                        .capabilities
                                        .iter()
                                        .any(|capability| capability == schema.name),
                            ),
                        ]
                    })
            })
            .collect(),
        _ => Vec::new(),
    }
}

pub(super) fn metadata_array(rows: &[Vec<MetadataCell>], index: usize) -> Result<ArrayRef> {
    let Some(first) = rows.first().and_then(|row| row.get(index)) else {
        return Err(DataFusionError::Plan(
            "metadata column is unavailable".to_string(),
        ));
    };
    match first {
        MetadataCell::Text(_) => Ok(Arc::new(StringArray::from(
            rows.iter()
                .map(|row| match row.get(index) {
                    Some(MetadataCell::Text(value)) => Some(value.as_str()),
                    _ => None,
                })
                .collect::<Vec<_>>(),
        ))),
        MetadataCell::Bool(_) => Ok(Arc::new(BooleanArray::from(
            rows.iter()
                .map(|row| match row.get(index) {
                    Some(MetadataCell::Bool(value)) => Some(*value),
                    _ => None,
                })
                .collect::<Vec<_>>(),
        ))),
        MetadataCell::Int64(_) => Ok(Arc::new(Int64Array::from(
            rows.iter()
                .map(|row| match row.get(index) {
                    Some(MetadataCell::Int64(value)) => Some(*value),
                    _ => None,
                })
                .collect::<Vec<_>>(),
        ))),
    }
}

fn data_type_name(data_type: QueryDataType) -> &'static str {
    match data_type {
        QueryDataType::Text => "VARCHAR",
        QueryDataType::Int64 => "BIGINT",
        QueryDataType::Bool => "BOOLEAN",
        QueryDataType::Timestamp => "TIMESTAMP",
        QueryDataType::Json => "JSON",
    }
}

fn access_class_name(access: AccessClass) -> &'static str {
    match access {
        AccessClass::Safe => "safe",
        AccessClass::Path => "path",
        AccessClass::Content => "content",
        AccessClass::ToolInput => "tool-input",
        AccessClass::ToolOutput => "tool-output",
        AccessClass::Secret => "secret",
    }
}
