# CypherAnywhere

**Status: Work in Progress** – This project is under active development and not yet ready for general use.

CypherAnywhere is a modular, storage-agnostic library that lets you run Cypher queries against a variety of data stores via simple, pluggable adapters. The project aims to provide a pragmatic subset of Neo4j Cypher with an extensible architecture for query planning and execution.

## Features

- **Cypher Front-End:** Initial support for a practical subset of Cypher 5.x.
- **Pluggable Storage Adapters:** Implement a small adapter interface to connect JSON, SQL, or other stores.
- **Rule- & Cost-Based Optimizer:** Generates efficient physical plans using adapter index metadata and statistics.
- **Typed & Testable Core:** TypeScript implementation with thorough unit tests and tracing hooks.
- **Small-Footprint MVP:** Focused on read-only queries with pattern matching, filtering, projections, and aggregations.

## Architecture

```
┌────────────┐    ┌────────────┐    ┌─────────────────────┐    ┌──────────────┐
│  Cypher    │    │ Logical    │    │   Physical Plan     │    │ Storage       │
│  Parser    │ →  │  Plan      │ →  │   & Execution       │ →  │ Adapter(s)    │
│  & AST     │    │ Generator  │    │   Engine            │    │ (JSON | SQL…) │
└────────────┘    └────────────┘    └─────────────────────┘    └──────────────┘
```

The parser produces an abstract syntax tree that is transformed into a logical plan. A rule-based rewriter and cost-based optimizer apply metadata supplied by the adapter. The physical plan is then executed using iterators that access the underlying store through the adapter interface.

## Project Layout

```
cypher-anywhere/
├── packages/
│   ├── core/                # parser, logical planner, optimizer, engine
│   ├── adapters/            # storage adapters such as json-adapter, sql-adapter
│   ├── cli/                 # command-line interface
│   └── examples/
├── docs/                    # documentation site (Docusaurus)
├── tests/                   # Jest test suites
└── benchmarks/              # micro & macro benchmarks
```

## Example

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


For more details, see the [design document](./design_document.md).
