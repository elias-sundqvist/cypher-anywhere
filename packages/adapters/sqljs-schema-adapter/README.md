# @cypher-anywhere/sqljs-schema-adapter

Adapter that maps an existing SQL schema to the CypherAnywhere engine using sql.js.
It reads from SQL tables based on a supplied schema description.

## Installation

```bash
npm install @cypher-anywhere/sqljs-schema-adapter
```

## Example

```ts
import { CypherEngine } from '@cypher-anywhere/core';
import { SqlJsSchemaAdapter } from '@cypher-anywhere/sqljs-schema-adapter';

const adapter = new SqlJsSchemaAdapter({
  schema: {
    nodes: [
      { table: 'people', id: 'id', properties: ['name'] }
    ],
    relationships: []
  },
  // optional setup can populate the SQL database
  setup(db) {
    db.run('CREATE TABLE people (id INTEGER PRIMARY KEY, name TEXT)');
  }
});

const engine = new CypherEngine({ adapter });
```

### Starting with data

Inside the `setup` function you may insert rows parsed from JSON. The JSON format is the same as the other adapters:

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

If you do not insert any rows, the engine starts with an empty graph and you can create data using Cypher queries.
