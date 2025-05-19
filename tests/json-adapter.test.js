const test = require('node:test');
const assert = require('node:assert');
const { JsonAdapter } = require('../packages/adapters/json-adapter/dist');
const { CypherEngine } = require('../packages/core/dist');
const path = require('path');

const datasetPath = path.join(__dirname, 'data', 'sample.json');

test('JsonAdapter + CypherEngine returns all nodes for MATCH (n) RETURN n', async (t) => {
  const adapter = new JsonAdapter({ datasetPath });
  const engine = new CypherEngine({ adapter });
  const results = [];
  for await (const row of engine.run('MATCH (n) RETURN n')) {
    results.push(row.n);
  }
  assert.strictEqual(results.length, 2);
  assert.ok(results[0].labels.includes('Person'));
});

// Query with label filter
test('CypherEngine supports MATCH (n:Person) RETURN n', async () => {
  const adapter = new JsonAdapter({ datasetPath });
  const engine = new CypherEngine({ adapter });
  const results = [];
  for await (const row of engine.run('MATCH (n:Person) RETURN n')) {
    results.push(row.n);
  }
  assert.strictEqual(results.length, 1);
  assert.deepStrictEqual(results[0].properties.name, 'Keanu Reeves');
});

// Create then match
test('CypherEngine CREATE followed by MATCH finds new node', async () => {
  const adapter = new JsonAdapter({ datasetPath });
  const engine = new CypherEngine({ adapter });
  for await (const _ of engine.run('CREATE (n:Person {name:"Alice"})')) {}
  const results = [];
  for await (const row of engine.run('MATCH (n:Person {name:"Alice"}) RETURN n')) {
    results.push(row.n);
  }
  assert.strictEqual(results.length, 1);
  assert.strictEqual(results[0].properties.name, 'Alice');
});

// Merge existing
test('CypherEngine MERGE finds existing node', async () => {
  const adapter = new JsonAdapter({ datasetPath });
  const engine = new CypherEngine({ adapter });
  const out = [];
  for await (const row of engine.run('MERGE (n:Person {name:"Keanu Reeves"}) RETURN n')) {
    out.push(row.n);
  }
  assert.strictEqual(out.length, 1);
  assert.strictEqual(out[0].properties.name, 'Keanu Reeves');
});

test('JsonAdapter transaction rollback discards changes', async () => {
  const adapter = new JsonAdapter({ datasetPath });
  const tx = await adapter.beginTransaction();
  await adapter.createNode(['Person'], { name: 'Bob' });
  await adapter.rollback(tx);
  const engine = new CypherEngine({ adapter });
  const res = [];
  for await (const row of engine.run('MATCH (n:Person {name:"Bob"}) RETURN n')) {
    res.push(row.n);
  }
  assert.strictEqual(res.length, 0);
});
