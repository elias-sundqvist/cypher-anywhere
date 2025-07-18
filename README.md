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

Builds across these packages are orchestrated using [Turborepo](https://turbo.build).
Running `npm run build` will invoke Turbo to compile each workspace in the
correct dependency order.

## Example

This repository currently contains a small proof-of-concept implementation. Only a trivial query form is supported:

```ts
import { CypherEngine } from '@cypher-anywhere/core';
import { JsonAdapter } from '@cypher-anywhere/json-adapter';
import { SqlJsAdapter } from '@cypher-anywhere/sqljs-adapter';

const adapter = new SqlJsAdapter({ datasetPath: './tests/data/sample.json' });
const engine = new CypherEngine({ adapter });

for await (const row of engine.run('MATCH (n) RETURN n')) {
  console.log(row.n);
}
```

## GitHub Pages Demo

A small demo is included under the `docs/` directory. After building the
packages and the demo bundle you can open `docs/index.html` locally or via
GitHub Pages to experiment with Cypher queries against an in-memory JSON
graph.
Updates to the `master` branch are automatically deployed to GitHub Pages by the
`Deploy Demo` workflow.

```bash
npm run build
npm run build:demo
```

The page shows the current graph data and lets you run queries that modify it.

To run the example:

```bash
npm install
npm test    # executes the Jest suite
```

For more details, see the [design document](./design_document.md).

## Release & Publishing

Version bumps are automated via the `Bump Version` workflow. The workflow
pushes the updated commit and tag using the built-in `GITHUB_TOKEN` and then
manually triggers the `Publish to npm` workflow via `workflow_dispatch`. No
personal access token is required.
