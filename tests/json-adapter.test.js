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

test('JsonAdapter rollback restores indexes', async () => {
  const adapter = new JsonAdapter({
    dataset: { nodes: [], relationships: [] },
    indexes: [{ label: 'Person', properties: ['name'], unique: true }]
  });
  const tx = await adapter.beginTransaction();
  await adapter.createNode(['Person'], { name: 'Tmp' });
  await adapter.rollback(tx);
  const results = [];
  for await (const n of adapter.indexLookup('Person', 'name', 'Tmp')) results.push(n);
  assert.strictEqual(results.length, 0);
});

test('JsonAdapter node property updates and deletion', async () => {
  const adapter = new JsonAdapter({ datasetPath });
  const node = await adapter.createNode(['Test'], { x: 1 });
  await adapter.updateNodeProperties(node.id, { x: 2 });
  const fetched = await adapter.getNodeById(node.id);
  assert.strictEqual(fetched.properties.x, 2);
  await adapter.deleteNode(node.id);
  const deleted = await adapter.getNodeById(node.id);
  assert.strictEqual(deleted, null);
});

test('JsonAdapter relationship lifecycle', async () => {
  const adapter = new JsonAdapter({ datasetPath });
  const rel = await adapter.createRelationship('KNOWS', 1, 2, { since: 2020 });
  await adapter.updateRelationshipProperties(rel.id, { since: 2021 });
  const fetched = await adapter.getRelationshipById(rel.id);
  assert.strictEqual(fetched.properties.since, 2021);
  await adapter.deleteRelationship(rel.id);
  const gone = await adapter.getRelationshipById(rel.id);
  assert.strictEqual(gone, null);
});

test('CypherEngine node update and delete via SET and DELETE', async () => {
  const adapter = new JsonAdapter({ datasetPath });
  const engine = new CypherEngine({ adapter });
  for await (const _ of engine.run('CREATE (n:Tmp {name:"T"})')) {}
  for await (const row of engine.run('MATCH (n:Tmp {name:"T"}) SET n.age = 5 RETURN n')) {
    assert.strictEqual(row.n.properties.age, 5);
  }
  for await (const _ of engine.run('MATCH (n:Tmp {name:"T"}) DELETE n')) {}
  const remaining = [];
  for await (const row of engine.run('MATCH (n:Tmp {name:"T"}) RETURN n')) { remaining.push(row); }
  assert.strictEqual(remaining.length, 0);
});

test('CypherEngine relationship lifecycle via queries', async () => {
  const adapter = new JsonAdapter({ datasetPath });
  const engine = new CypherEngine({ adapter });
  let rel;
  for await (const row of engine.run('CREATE (a:A)-[r:REL {x:1}]->(b:B) RETURN r')) {
    rel = row.r;
  }
  for await (const row of engine.run('MATCH ()-[r:REL {x:1}]->() SET r.x = 2 RETURN r')) {
    assert.strictEqual(row.r.properties.x, 2);
  }
  for await (const _ of engine.run('MATCH ()-[r:REL {x:2}]->() DELETE r')) {}
  const out = [];
  for await (const row of engine.run('MATCH ()-[r:REL]->() RETURN r')) { out.push(row); }
  assert.strictEqual(out.length, 0);
});

test('CypherEngine multi-statement merge creates relationship between matches', async () => {
  const adapter = new JsonAdapter({ datasetPath });
  const engine = new CypherEngine({ adapter });
  const script = `
    MATCH (p:Person {name:"Keanu Reeves"}) RETURN p;
    MATCH (m:Movie {title:"The Matrix"}) RETURN m;
    MERGE (p)-[r:ACTED_IN]->(m) RETURN r
  `;
  const res = [];
  for await (const row of engine.run(script)) {
    if (row.r) res.push(row.r);
  }
  assert.strictEqual(res.length, 1);
  assert.strictEqual(res[0].type, 'ACTED_IN');
});
