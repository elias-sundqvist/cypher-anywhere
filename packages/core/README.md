# @cypher-anywhere/core

Core query engine that parses and executes Cypher queries. It is storage agnostic and works with any adapter that implements the `StorageAdapter` interface.

## Usage

Install together with a storage adapter:

```bash
npm install @cypher-anywhere/core @cypher-anywhere/json-adapter
```

Create an engine either with an initial JSON dataset or start with an empty dataset.

```ts
import { CypherEngine } from '@cypher-anywhere/core';
import { JsonAdapter } from '@cypher-anywhere/json-adapter';

const adapter = new JsonAdapter({
  // start empty
  dataset: { nodes: [], relationships: [] }
});

const engine = new CypherEngine({ adapter });

// use Cypher queries to create data
await engine.run('CREATE (:Person {name:"Alice"})');
```

### Loading an initial dataset

Instead of starting empty you can pass `dataset` or `datasetPath` when constructing an adapter. The JSON must follow this shape:

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

The engine can then query or update the graph using Cypher.
