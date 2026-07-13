use std::collections::HashMap;
use std::time::Instant;

use gitql_ast::types::DataType;
use gitql_ast::types::integer::IntType;
use gitql_ast::types::text::TextType;
use gitql_core::environment::Environment;
use gitql_core::object::Row;
use gitql_core::schema::Schema;
use gitql_core::values::Value;
use gitql_core::values::integer::IntValue;
use gitql_core::values::null::NullValue;
use gitql_core::values::text::TextValue;
use gitql_engine::data_provider::DataProvider;
use gitql_engine::engine;
use gitql_parser::parser;
use gitql_parser::tokenizer::Tokenizer;
use gitql_std::aggregation::{aggregation_function_signatures, aggregation_functions};
use gitql_std::standard::{standard_function_signatures, standard_functions};

struct SyntheticProvider {
    rows: usize,
}

impl DataProvider for SyntheticProvider {
    fn provide(&self, table: &str, selected_columns: &[String]) -> Result<Vec<Row>, String> {
        if table != "sessions" {
            return Err("unsupported table".to_string());
        }
        let mut rows = Vec::with_capacity(self.rows);
        for index in 0..self.rows {
            let mut values: Vec<Box<dyn Value>> = Vec::with_capacity(selected_columns.len());
            for column in selected_columns {
                match column.as_str() {
                    "agent_id" => values.push(Box::new(TextValue::new("codex".to_string()))),
                    "session_id" => values.push(Box::new(TextValue::new(format!(
                        "codex:fixture:session-{index:07}"
                    )))),
                    "model" => {
                        values.push(Box::new(TextValue::new("example-model".to_string())))
                    }
                    "updated_at" => values.push(Box::new(IntValue::new(index as i64))),
                    _ => values.push(Box::new(NullValue)),
                }
            }
            rows.push(Row { values });
        }
        Ok(rows)
    }
}

fn environment() -> Environment {
    let fields = vec!["agent_id", "session_id", "model", "updated_at"];
    let mut names = HashMap::new();
    names.insert("sessions", fields);
    let mut types: HashMap<&'static str, Box<dyn DataType>> = HashMap::new();
    types.insert("agent_id", Box::new(TextType));
    types.insert("session_id", Box::new(TextType));
    types.insert("model", Box::new(TextType));
    types.insert("updated_at", Box::new(IntType));
    let mut env = Environment::new(Schema {
        tables_fields_names: names,
        tables_fields_types: types,
    });
    env.with_standard_functions(&standard_function_signatures(), standard_functions());
    env.with_aggregation_functions(&aggregation_function_signatures(), aggregation_functions());
    env
}

fn execute(query: &str, rows: usize) -> Result<usize, String> {
    let mut env = environment();
    let tokens = Tokenizer::tokenize(query).map_err(|_| "tokenize failed".to_string())?;
    let queries =
        parser::parse_gql(tokens, &mut env).map_err(|_| "parse failed".to_string())?;
    let provider: Box<dyn DataProvider> = Box::new(SyntheticProvider { rows });
    let results = engine::evaluate(&mut env, &provider, queries)?;
    let count = results
        .into_iter()
        .filter_map(|result| match result {
            engine::EvaluationResult::SelectedGroups(object) => {
                Some(object.groups.into_iter().map(|group| group.rows.len()).sum::<usize>())
            }
            _ => None,
        })
        .sum();
    Ok(count)
}

fn main() -> Result<(), String> {
    let rows = std::env::args()
        .nth(1)
        .and_then(|value| value.parse().ok())
        .unwrap_or(100_000);
    let query_name = std::env::args().nth(2).unwrap_or_else(|| "q1".to_string());
    let query = match query_name.as_str() {
        "q1" => "SELECT agent_id, session_id, model, updated_at FROM sessions WHERE updated_at >= 50000 ORDER BY updated_at DESC LIMIT 20",
        "q2" => "SELECT agent_id, COUNT(agent_id) AS session_count FROM sessions GROUP BY agent_id ORDER BY session_count DESC",
        _ => return Err("unknown query".to_string()),
    };
    let started = Instant::now();
    let output_rows = execute(query, rows)?;
    let elapsed = started.elapsed();
    println!(
        "{}",
        serde_json::json!({
            "engine": "gitql",
            "query": query_name,
            "input_rows": rows,
            "output_rows": output_rows,
            "elapsed_ms": elapsed.as_millis(),
            "streaming_output": false,
            "cancellation_api": false,
            "resource_budget_api": false
        })
    );
    Ok(())
}
