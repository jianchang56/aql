use aql_adapter_api::{
    AccessGrant, AdapterError, AdapterSchema, AgentAdapter, CancellationToken, Capabilities,
    ColumnCapability, ColumnName, ProbeRequest, ProbeResult, PushdownReport, PushdownState,
    ResourceBudget, ScanRequest, ScanResult, SnapshotReport, SnapshotStrength, TableName,
    check_scan_state, validate_projection_access,
};
use aql_model::{AccessClass, SnapshotToken, SourceId, SourceManifest};

struct FakeAdapter;

impl FakeAdapter {
    fn manifest() -> SourceManifest {
        SourceManifest {
            source_id: SourceId::new("fake:fixture"),
            agent_id: "fake".to_string(),
            display_name: "Synthetic Fake Adapter".to_string(),
            data_root_token: "fixture-root".to_string(),
            format_fingerprint: "fake-v0".to_string(),
            capabilities: vec!["sessions".to_string()],
            snapshot: Some(SnapshotToken::new("snapshot-1")),
            warnings: Vec::new(),
        }
    }
}

impl AgentAdapter for FakeAdapter {
    fn id(&self) -> &'static str {
        "fake"
    }

    fn probe(&self, request: &ProbeRequest) -> Result<ProbeResult, AdapterError> {
        if request.data_root == "missing" {
            return Err(AdapterError::NotFound {
                stage: "probe".to_string(),
            });
        }
        Ok(ProbeResult {
            manifests: vec![Self::manifest()],
        })
    }

    fn capabilities(&self, _manifest: &SourceManifest) -> Capabilities {
        Capabilities {
            tables: vec![TableName::Sessions],
            columns: self.schema(_manifest).columns,
            snapshot_strength: SnapshotStrength::Strong,
        }
    }

    fn schema(&self, _manifest: &SourceManifest) -> AdapterSchema {
        AdapterSchema {
            columns: vec![
                ColumnCapability {
                    name: ColumnName::new("session_id"),
                    access: AccessClass::Safe,
                },
                ColumnCapability {
                    name: ColumnName::new("title"),
                    access: AccessClass::Content,
                },
            ],
        }
    }

    fn scan(&self, request: ScanRequest) -> Result<ScanResult, AdapterError> {
        validate_projection_access(
            &request.projection,
            &self.schema(&request.source),
            request.access,
        )?;
        check_scan_state(&request.cancellation, &request.budget, 1, 0)?;
        Ok(ScanResult {
            records: Box::new(std::iter::empty()),
            pushdown: PushdownReport {
                predicates: request
                    .predicates
                    .iter()
                    .map(|_| PushdownState::Unsupported)
                    .collect(),
                limit: request.limit.map(|_| PushdownState::Exact),
                ordering: request
                    .order_hint
                    .iter()
                    .map(|_| PushdownState::Unsupported)
                    .collect(),
            },
            diagnostics: Default::default(),
            snapshot: SnapshotReport {
                token: request.snapshot,
                strength: SnapshotStrength::Strong,
                stale: false,
            },
        })
    }
}

fn request() -> ScanRequest {
    ScanRequest {
        source: FakeAdapter::manifest(),
        table: TableName::Sessions,
        projection: vec![ColumnName::new("session_id")],
        predicates: Vec::new(),
        limit: Some(1),
        order_hint: Vec::new(),
        access: AccessGrant::default(),
        budget: ResourceBudget::default(),
        cancellation: CancellationToken::default(),
        snapshot: Some(SnapshotToken::new("snapshot-1")),
    }
}

#[test]
fn fake_adapter_passes_probe_schema_and_scan_contract() {
    let adapter = FakeAdapter;
    let probe = adapter
        .probe(&ProbeRequest {
            data_root: "fixture".to_string(),
        })
        .expect("fixture probe must succeed");
    assert_eq!(probe.manifests.len(), 1);
    assert_eq!(adapter.capabilities(&probe.manifests[0]).tables.len(), 1);

    let result = adapter.scan(request()).expect("safe scan must succeed");
    assert_eq!(result.pushdown.limit, Some(PushdownState::Exact));
    assert_eq!(result.records.count(), 0);
}

#[test]
fn fake_adapter_rejects_sensitive_projection_before_scan() {
    let adapter = FakeAdapter;
    let mut request = request();
    request.projection = vec![ColumnName::new("title")];
    let error = match adapter.scan(request) {
        Ok(_) => panic!("content projection must fail without a grant"),
        Err(error) => error,
    };
    assert_eq!(
        error,
        AdapterError::AccessDenied {
            column: "title".to_string()
        }
    );
}

#[test]
fn fake_adapter_observes_cancellation_and_budget() {
    let adapter = FakeAdapter;
    let cancelled = request();
    cancelled.cancellation.cancel();
    assert!(matches!(
        adapter.scan(cancelled),
        Err(AdapterError::Cancelled)
    ));

    let mut expired = request();
    expired.budget.max_records = 0;
    assert!(matches!(
        adapter.scan(expired),
        Err(AdapterError::BudgetExceeded { .. })
    ));
}
