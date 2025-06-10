# @cypher-anywhere/json-adapter

In-memory adapter that stores graph data in a plain JavaScript object.
It can be used for testing or small applications where persistence is not required.

## Installation

```bash
npm install @cypher-anywhere/json-adapter
```

## Using with the core engine

```ts
import { CypherEngine } from '@cypher-anywhere/core';
import { JsonAdapter } from '@cypher-anywhere/json-adapter';

const adapter = new JsonAdapter({
  // start with an empty graph
  dataset: { nodes: [], relationships: [] }
});

const engine = new CypherEngine({ adapter });
```

You can now create and query nodes and relationships using Cypher.

### Loading initial data

Instead of starting empty you may provide `dataset` or `datasetPath` when creating the adapter. The JSON should look like this:

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

The data can then be modified with Cypher commands.
