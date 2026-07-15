use super::*;

#[derive(Clone)]
pub(super) enum MetadataValue {
    Text(String),
    Bool(bool),
    Int64(i64),
}

pub(super) fn rows(table: &str, sources: &[FederatedSource]) -> Vec<Vec<MetadataValue>> {
    match table {
        "aql_tables" => QUERY_SCHEMAS
            .iter()
            .map(|schema| {
                vec![
                    MetadataValue::Text(schema.name.to_string()),
                    MetadataValue::Text(
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
                            MetadataValue::Text(schema.name.to_string()),
                            MetadataValue::Text(column.name.to_string()),
                            MetadataValue::Int64(index as i64 + 1),
                            MetadataValue::Text(data_type_name(column.data_type).to_string()),
                            MetadataValue::Bool(column.nullable),
                            MetadataValue::Text(access_class_name(column.access).to_string()),
                        ]
                    })
            })
            .collect(),
        "aql_sources" => sources
            .iter()
            .map(|source| {
                vec![
                    MetadataValue::Text(source.manifest.source_id.to_string()),
                    MetadataValue::Text(source.manifest.agent_id.clone()),
                    MetadataValue::Text(source.manifest.display_name.clone()),
                    MetadataValue::Text(source.manifest.format_fingerprint.clone()),
                    MetadataValue::Text(
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
                            MetadataValue::Text(source.manifest.source_id.to_string()),
                            MetadataValue::Text(schema.name.to_string()),
                            MetadataValue::Bool(
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

pub(super) fn array(rows: &[Vec<MetadataValue>], index: usize) -> Result<ArrayRef> {
    let Some(first) = rows.first().and_then(|row| row.get(index)) else {
        return Err(DataFusionError::Plan(
            "metadata column is unavailable".to_string(),
        ));
    };
    match first {
        MetadataValue::Text(_) => Ok(Arc::new(StringArray::from(
            rows.iter()
                .map(|row| match row.get(index) {
                    Some(MetadataValue::Text(value)) => Some(value.clone()),
                    _ => None,
                })
                .collect::<Vec<_>>(),
        ))),
        MetadataValue::Bool(_) => Ok(Arc::new(BooleanArray::from(
            rows.iter()
                .map(|row| match row.get(index) {
                    Some(MetadataValue::Bool(value)) => Some(*value),
                    _ => None,
                })
                .collect::<Vec<_>>(),
        ))),
        MetadataValue::Int64(_) => Ok(Arc::new(Int64Array::from(
            rows.iter()
                .map(|row| match row.get(index) {
                    Some(MetadataValue::Int64(value)) => Some(*value),
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
