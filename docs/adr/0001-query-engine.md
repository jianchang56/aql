# ADR 0001: Select DataFusion as the AQL MVP query engine

## Status

Accepted for Phase 0/MVP.

## Context

AQL requires a SQL engine that preserves the engine-independent Adapter Contract, receives projection information before source access, leaves unsupported predicates for engine-side evaluation, does not apply unsafe limits, supports cancellation and resource controls, and stays below 512 MiB RSS on the 1M-row synthetic metadata benchmark.

The candidates were GitQL 0.43.0 and DataFusion 54.0.0. Both spikes used the same four-column synthetic sessions schema and the queries defined in `docs/engine-spike-scorecard.md`.

## Candidates

### GitQL 0.43.0

Advantages:

- Small binary and dependency graph.
- Simple customizable schema and DataProvider.
- Q1 and Q2 SQL execute correctly.

Disqualifying results:

- H6 failed: the public evaluation API is synchronous and has no cancellation token or async abort boundary.
- H7 failed: the engine has no memory/resource budget interface; the provider can reject source scans, but sort/group/evaluation allocations remain unbounded.
- Output is fully materialized before it is returned.

### DataFusion 54.0.0

Advantages:

- `TableProvider::scan` receives projection, filters and limit separately.
- Unsupported filters remain in the logical/physical plan.
- Async execution can be aborted; the spike cancelled a delayed scan in 22 ms.
- Streaming RecordBatch output and runtime memory controls.
- Q2 demonstrated projection `[0]`, proving unused columns are not requested from the provider.

Costs:

- Release binary was approximately 112 MB versus 3.7 MB for GitQL.
- Dependency graph and first build time are substantially larger.
- AQL requires a conversion layer between Canonical Records and Arrow arrays.

## Hard-gate results

| Gate | GitQL | DataFusion |
|---|---|---|
| H1 engine-independent Adapter/Model | Pass | Pass |
| H2 projection before source access | Pass | Pass |
| H3 sensitive access rejection | Pass via shared AQL layer | Pass via shared AQL layer |
| H4 unsupported predicate correctness | Pass | Pass |
| H5 safe limit semantics | Pass | Pass |
| H6 cancellation | **Fail** | Pass, 22 ms |
| H7 resource budgets | **Fail** | Pass |
| H8 1M rows under 512 MiB | Pass, 412 MB | Pass, 111 MB |
| H9 no engine fork | Pass | Pass |
| H10 macOS/Linux delivery | Pass | Pass |

GitQL is eliminated before weighted scoring because hard gates H6 and H7 failed.

## Performance evidence

Q1, 1M rows, five release runs:

| Engine | Median | Maximum | Peak RSS |
|---|---:|---:|---:|
| GitQL | 19,401 ms | 19,488 ms | 411,795,456 bytes |
| DataFusion | 57 ms | 68 ms | 110,837,760 bytes |

Full evidence:

- `benchmarks/engine/gitql.json`
- `benchmarks/engine/datafusion.json`

## Decision

Use DataFusion 54.0.0 as the only production query engine for the AQL MVP.

The GitQL spike remains isolated under `spikes/gitql` as reproducible decision evidence. It must not be referenced by the production workspace or exposed through a runtime engine switch.

## Consequences

- Add a single `aql-engine-datafusion` production crate.
- Adapter API and Canonical Model remain free of Arrow/DataFusion types.
- AQL planner access checks run before DataFusion table registration/execution.
- DataFusion TableProvider converts only projected Canonical fields into Arrow arrays.
- Build size and compile time become explicit packaging concerns.

## Rejected alternative

GitQL was not rejected for performance alone. It was rejected because cancellation and engine-level resource budgeting are hard MVP safety requirements and cannot be added without modifying/forking the engine.

## Migration triggers

Re-evaluate only if DataFusion prevents required source pushdown, cannot meet supported-platform packaging constraints, introduces an unfixable SQL correctness issue, or exceeds the documented resource budget on typical datasets.

