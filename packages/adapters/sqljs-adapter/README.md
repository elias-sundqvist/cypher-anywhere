# @cypher-anywhere/sqljs-adapter

Storage adapter backed by [sql.js](https://github.com/sql-js/sql.js) which provides an in-memory SQLite database compiled to WebAssembly.

## Installation

```bash
npm install @cypher-anywhere/sqljs-adapter
```

## Quick start

```ts
import { CypherEngine } from '@cypher-anywhere/core';
import { SqlJsAdapter } from '@cypher-anywhere/sqljs-adapter';

const adapter = new SqlJsAdapter({
  // start with no data
  dataset: { nodes: [], relationships: [] }
});

const engine = new CypherEngine({ adapter });
```

Cypher queries can create and query data once the adapter is initialised.

### Loading initial JSON

You may also pass an initial `dataset` or `datasetPath`. The JSON shape matches the other adapters:

```json
{
  "nodes": [
    { "id": 1, "labels": ["Person"], "properties": { "name": "Alice" } }
  ],
  "relationships": [
    { "id": 2, "type": "KNOWS", "startNode": 1, "endNode": 1, "properties": {} }
  ]
}
```

This data is loaded into the in-memory SQL database before the engine runs.
