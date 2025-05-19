# CypherAnywhere

**Status: Work in Progress** – This project is under active development and not yet ready for general use.

CypherAnywhere is a modular, storage-agnostic library that lets you run Cypher queries against a variety of data stores via simple, pluggable adapters. The project aims to provide a pragmatic subset of Neo4j Cypher with an extensible architecture for query planning and execution.

## Features

- **Cypher Front-End:** Initial support for a practical subset of Cypher 5.x.
- **Pluggable Storage Adapters:** Implement a small adapter interface to connect JSON, SQL, or other stores.
- **Rule- & Cost-Based Optimizer:** Generates efficient physical plans using adapter index metadata and statistics.
- **Typed & Testable Core:** TypeScript implementation with thorough unit tests and tracing hooks.
- **Small-Footprint MVP:** Focused on read-only queries with pattern matching, filtering, projections, and aggregations.

## Project Layout

```text
cypher-anywhere/
├── packages/
│   ├── core/                # parser, logical planner, engine
│   └── adapters/            # storage adapters (json-adapter, ...)
├── tests/                   # Jest test suites
└── design_document.md       # architecture overview
```

## Example

This repository currently contains a small proof-of-concept implementation. Only a trivial query form is supported:

```ts
import { CypherEngine } from '@cypher-anywhere/core';
import { JsonAdapter } from '@cypher-anywhere/json-adapter';

const adapter = new JsonAdapter({ datasetPath: './tests/data/sample.json' });
const engine = new CypherEngine({ adapter });

for await (const row of engine.run('MATCH (n) RETURN n')) {
  console.log(row.n);
}
```

To run the example:

```bash
npm install
npm test    # executes the Jest suite
```

For more details, see the [design document](./design_document.md).
