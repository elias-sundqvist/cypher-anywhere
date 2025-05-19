# CypherAnywhere – Design & Architecture Document

## 1. Vision & Goals

CypherAnywhere is a modular, storage‑agnostic library that allows developers to execute **Cypher** queries against *any* underlying data store – from an in‑memory JSON object to a full‑blown SQL or distributed database – provided the store exposes a small, easy‑to‑implement adapter interface. Key objectives:

1. **Cypher Front‑End** – Support a pragmatic subset of Neo4j Cypher 5.x (extensible to the full spec over time).
2. **Pluggable Storage Adapters** – Simple, stateless interfaces that translate physical access into the engine’s logical operators.
3. **Rule‑ & Cost‑Based Optimizer** – A query planner that leverages adapter‑supplied index metadata and statistics to emit efficient physical plans.
4. **Typed, Testable, Observable** – Type‑safe core (TypeScript), 100 % unit‑coverage on the optimizer, and robust tracing hooks.
5. **Small‑Footprint MVP** – First milestone: read‑only single‑graph queries with pattern matching, filtering, projections and aggregations.

---

## 2. High‑Level Architecture

```
┌────────────┐    ┌────────────┐    ┌─────────────────────┐    ┌──────────────┐
│  Cypher    │    │ Logical    │    │   Physical Plan     │    │ Storage       │
│  Parser    │ →  │  Plan      │ →  │   & Execution       │ →  │ Adapter(s)    │
│  & AST     │    │ Generator  │    │   Engine            │    │ (JSON | SQL…) │
└────────────┘    └────────────┘    └─────────────────────┘    └──────────────┘
        ▲                   ▲                   ▲                     ▲
        │                   │                   │                     │
        │           ┌───────┴───────┐   ┌───────┴────────┐   ┌────────┴───────┐
        │           │   Rule‑Based  │   │ Cost‑Based     │   │ Index & Stats   │
        └────────── │   Rewrites    │   │  Optimizer     │   │ Catalog        │
                    └───────────────┘   └────────────────┘   └────────────────┘
```

### Layer Responsibilities

| Layer                                | Responsibilities                                                                                       |
| ------------------------------------ | ------------------------------------------------------------------------------------------------------ |
| **Parser & AST**                     | Tokenize and parse Cypher into an abstract syntax tree with semantic checks.                           |
| **Logical Plan**                     | Transform AST into an algebraic graph‑relational logical plan.                                         |
| **Rule‑Based Rewriter**              | Canonicalize, push‑down filters, decompose predicates, deduplicate patterns.                           |
| **Cost‑Based Optimizer**             | Use adapter‑supplied statistics and index metadata to choose join order, access path, and projections. |
| **Physical Plan & Execution Engine** | Translate logical operators into executable iterators, then stream results.                            |
| **Storage Adapter**                  | Provide primitive graph access methods plus an index & statistics catalog.                             |

---

## 3. Project Layout

```
cypher‑anywhere/
├── packages/
│   ├── core/                # language, planner, engine
│   │   ├── parser/
│   │   ├── logical/
│   │   ├── optimizer/
│   │   ├── physical/
│   │   └── execution/
│   ├── adapters/
│   │   ├── json‑adapter/
│   │   └── sql‑adapter/
│   ├── cli/                 # REPL & batch runner
│   └── examples/
├── docs/                    # rendered site via Docusaurus
├── tests/                   # jest test‑suites (core + adapters)
└── benchmarks/              # micro & macro benchmarks
```

> **Tip :** keep adapters in their own NPM workspaces so external contributors can publish separate packages.

---

## 4. Core TypeScript Interfaces (excerpt)

```ts
// packages/core/src/storage/StorageAdapter.ts
export interface StorageAdapter {
  // Node access
  getNodeById(id: NodeId): Promise<NodeRecord | null>;
  scanNodes(spec: NodeScanSpec): AsyncIterable<NodeRecord>;

  // Relationship access
  getRelationshipById(id: RelId): Promise<RelRecord | null>;
  scanRelationships(spec: RelScanSpec): AsyncIterable<RelRecord>;

  // Schema & statistics
  listIndexes(): Promise<IndexMetadata[]>;  // e.g. label+prop indexes
  getStatistics(): Promise<GraphStatistics>; // counts, histograms, NDVs

  // Optional transactional hooks (for future write support)
  beginTransaction?(): Promise<TransactionCtx>;
  commit?(tx: TransactionCtx): Promise<void>;
  rollback?(tx: TransactionCtx): Promise<void>;
}

export interface IndexMetadata {
  label?: string;
  type?: string;           // relationship type for rel indexes
  properties: string[];
  unique: boolean;
  selectivity?: number;    // 0 < s ≤ 1
}
```

Rule‑based and cost‑based optimizers operate over **LogicalPlan** nodes (`Projection`, `Filter`, `Expand`, `HashJoin`, …). A thin **CostModel** interface retrieves selectivities & cardinalities from the adapter’s `getStatistics()` and from index metadata.

---

## 5. Query Planning Pipeline

1. **Parse & Validate** – Ensure labels, relationship types and properties referenced exist (if adapter exposes schema).
2. **Logical Plan Generation** – Convert patterns into adjacency‑list algebraic operators.
3. **Heuristic Rewrites** – Push filters to scans, collapse expansions, eliminate dead columns.
4. **Cost Estimation** – Bottom‑up dynamic‑programming join enumeration (Selinger‑style) using selectivity estimates.
5. **Physical Plan Selection** – Replace logical operators with `IndexSeek`, `LabelScan`, `HashJoin`, `NestedLoop`, etc.
6. **Execution** – Pull‑based iterator model (`.next()`), supporting early out & back‑pressure.

### Index Utilisation

* **Node/Rel Exact Match** – `MATCH (p:Person {id:42})` ⟶ *IndexSeek* if primary‑key index exists.
* **Property Ranges** – Range tree operators (`>`, `<`, `BETWEEN`) convert to *IndexRangeSeek*.
* **Composite Indexes** – Multi‑property indexes considered for equality prefixes first, then ranges.

---

## 6. Index & Statistics Catalog

Adapters may provide one of two strategies:

* **Static Catalog** – JSON/YAML descriptor checked into the project; simplest for read‑only datasets.
* **Self‑Reporting** – For RDBMS backends, translate `SHOW INDEXES` / `pg_stats` into `IndexMetadata` & `GraphStatistics` objects.

**Statistics API** (simplified):

```ts
interface GraphStatistics {
  nodeCount(label?: string): Promise<number>;
  relCount(type?: string): Promise<number>;
  distinctValues(label: string, property: string): Promise<number>;
  histogram(label: string, property: string, buckets: number): Promise<HistogramBucket[]>;
}
```

---

## 7. Extending / Writing a New Storage Adapter

1. **Implement `StorageAdapter`** – The minimal API (6‑10 methods).
2. **Describe Indexes & Stats** – Hard‑code JSON or translate native catalog.
3. **Register Adapter** – Export an `AdapterFactory` and add to `@cypher-anywhere/adapters` index.
4. **Write Integration Tests** – Use the shared `adapter‑conformance` test‑suite (131 tests/ backend).

> A *hello‑world* `JsonAdapter` (in‑memory) ships with the core tests to illustrate the process.

---

## 8. Example Usage

```ts
import { CypherEngine } from "@cypher-anywhere/core";
import { JsonAdapter } from "@cypher-anywhere/json-adapter";

const adapter = new JsonAdapter({
  datasetPath: "./data/movies.json",
  indexes: [
    { label: "Movie", properties: ["title"], unique: false },
    { label: "Person", properties: ["name"], unique: false }
  ]
});

const engine = new CypherEngine({ adapter });

const result = await engine.run(
  `MATCH (p:Person {name: $name})-[:ACTED_IN]->(m:Movie)
   WHERE m.released >= $year
   RETURN m.title AS title, m.released AS year` ,
  { name: "Keanu Reeves", year: 1999 }
);

for await (const row of result) {
  console.log(row.title, row.year);
}
```

---

## 9. Testing & Benchmarks

| Suite                   | Description                                                                                |
| ----------------------- | ------------------------------------------------------------------------------------------ |
| **Core‑logic**          | AST ↔ Plan ↔ Physical operator correctness (Jest).                                         |
| **Adapter Conformance** | 131 scenario‑based tests auto‑run for every adapter package.                               |
| **Benchmark Harness**   | Data‑volume scaling from 1× to 1000× on the LDBC SNB dataset measuring t₉₉ execution time. |

---


## 11. Contribution Guidelines & Licensing

* **License:** MIT (OSI‑approved, permissive).
* **Conventional Commits** + **Semantic Release** for automated versioning.
* All PRs require test coverage ≥ 90 %.
* CLA‑based contributions for external devs.

---

## 12. Future Ideas

* Distributed query execution (Spark / Dataflow plugins).
* Graph algorithms & GDS API compatibility.
* Cypher‑to‑SQL transpilation for OLAP pushes.
* Graph‑data‑frames adapter for Arrow IPC streaming.

---

### Glossary

* **AST** – Abstract Syntax Tree
* **NDV** – Number of Distinct Values (cardinality)
* **LDBC SNB** – Linked Data Benchmark Council Social Network Benchmark

---

> **Next Steps**
>
> 1. Align on the MVP Cypher subset.
> 2. Decide on TypeScript vs. language‑agnostic core with WASM parser.
> 3. Spike a *JsonAdapter* + mini‑engine to validate ergonomics.
