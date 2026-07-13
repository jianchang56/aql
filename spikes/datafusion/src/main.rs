use std::sync::{Arc, Mutex};
use std::time::Instant;

use async_trait::async_trait;
use datafusion::arrow::array::{ArrayRef, Int64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::{Session, TableProvider};
use datafusion::datasource::MemTable;
use datafusion::error::Result;
use datafusion::logical_expr::{Expr, TableType};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;

#[derive(Debug, Default)]
struct ObservedScan {
    projection: Vec<usize>,
    limit: Option<usize>,
}

#[derive(Debug)]
struct SyntheticProvider {
    schema: SchemaRef,
    rows: usize,
    observed: Arc<Mutex<ObservedScan>>,
    delay_ms: u64,
}

impl SyntheticProvider {
    fn new(rows: usize, observed: Arc<Mutex<ObservedScan>>, delay_ms: u64) -> Self {
        let schema = Arc::new(Schema::new(vec![
            Field::new("agent_id", DataType::Utf8, false),
            Field::new("session_id", DataType::Utf8, false),
            Field::new("model", DataType::Utf8, false),
            Field::new("updated_at", DataType::Int64, false),
        ]));
        Self {
            schema,
            rows,
            observed,
            delay_ms,
        }
    }

    fn array(&self, index: usize) -> ArrayRef {
        match index {
            0 => Arc::new(StringArray::from_iter_values(
                std::iter::repeat_n("codex", self.rows),
            )),
            1 => Arc::new(StringArray::from_iter_values(
                (0..self.rows).map(|index| format!("codex:fixture:session-{index:07}")),
            )),
            2 => Arc::new(StringArray::from_iter_values(std::iter::repeat_n(
                "example-model",
                self.rows,
            ))),
            3 => Arc::new(Int64Array::from_iter_values(
                (0..self.rows).map(|value| value as i64),
            )),
            _ => unreachable!("projection index is validated by DataFusion"),
        }
    }
}

#[async_trait]
impl TableProvider for SyntheticProvider {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if self.delay_ms > 0 {
            tokio::time::sleep(std::time::Duration::from_millis(self.delay_ms)).await;
        }
        let projection = projection
            .cloned()
            .unwrap_or_else(|| (0..self.schema.fields().len()).collect());
        *self.observed.lock().expect("scan observation lock") = ObservedScan {
            projection: projection.clone(),
            limit,
        };
        let projected_schema = Arc::new(self.schema.project(&projection)?);
        let arrays = projection.iter().map(|index| self.array(*index)).collect();
        let batch = RecordBatch::try_new(projected_schema.clone(), arrays)?;
        let table = MemTable::try_new(projected_schema, vec![vec![batch]])?;
        table.scan(state, None, &[], limit).await
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let rows = std::env::args()
        .nth(1)
        .and_then(|value| value.parse().ok())
        .unwrap_or(100_000);
    let observed = Arc::new(Mutex::new(ObservedScan::default()));
    let query_name = std::env::args().nth(2).unwrap_or_else(|| "q1".to_string());
    let provider = Arc::new(SyntheticProvider::new(
        rows,
        observed.clone(),
        u64::from(query_name == "cancel") * 10_000,
    ));
    let context = SessionContext::new();
    context.register_table("sessions", provider)?;
    if query_name == "cancel" {
        let started = Instant::now();
        let task = tokio::spawn(async move {
            context
                .sql("SELECT * FROM sessions")
                .await?
                .collect()
                .await
        });
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        task.abort();
        let cancelled = task.await.is_err_and(|error| error.is_cancelled());
        println!(
            "{}",
            serde_json::json!({
                "engine": "datafusion",
                "query": "cancel",
                "cancelled": cancelled,
                "cancel_elapsed_ms": started.elapsed().as_millis()
            })
        );
        return Ok(());
    }
    let query = match query_name.as_str() {
        "q1" => "SELECT agent_id, session_id, model, updated_at FROM sessions WHERE updated_at >= 50000 ORDER BY updated_at DESC LIMIT 20",
        "q2" => "SELECT agent_id, COUNT(*) AS session_count FROM sessions GROUP BY agent_id ORDER BY session_count DESC",
        _ => return Err(datafusion::error::DataFusionError::Plan(
            "unknown query".to_string(),
        )),
    };
    let started = Instant::now();
    let batches = context.sql(query).await?.collect().await?;
    let elapsed = started.elapsed();
    let output_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
    let observed = observed.lock().expect("scan observation lock");
    println!(
        "{}",
        serde_json::json!({
            "engine": "datafusion",
            "query": query_name,
            "input_rows": rows,
            "output_rows": output_rows,
            "elapsed_ms": elapsed.as_millis(),
            "projection": observed.projection,
            "provider_limit": observed.limit,
            "streaming_output": true,
            "cancellation_api": true,
            "resource_budget_api": true
        })
    );
    Ok(())
}
