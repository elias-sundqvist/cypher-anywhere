const test = require('node:test');
const assert = require('node:assert');
const { JsonAdapter } = require('../packages/adapters/json-adapter/dist');
const { CypherEngine } = require('../packages/core/dist');

function makeDataset(n) {
  const nodes = [];
  for (let i = 0; i < n; i++) {
    nodes.push({ id: i + 1, labels: ['Person'], properties: { name: 'name' + i } });
  }
  return { nodes, relationships: [] };
}

test('planner uses index when available', async () => {
  const data = makeDataset(10);
  const adapter = new JsonAdapter({ dataset: data, indexes: [{ label: 'Person', properties: ['name'], unique: true }] });
  let used = false;
  const orig = adapter.indexLookup.bind(adapter);
  adapter.indexLookup = async function*(label, prop, value) {
    used = true;
    for await (const n of orig(label, prop, value)) {
      yield n;
    }
  };
  const engine = new CypherEngine({ adapter });
  const out = [];
  for await (const row of engine.run('MATCH (n:Person {name:"name5"}) RETURN n')) {
    out.push(row.n);
  }
  assert.strictEqual(out.length, 1);
  assert.ok(used);
});

test('planner uses index for WHERE equality', async () => {
  const data = makeDataset(10);
  const adapter = new JsonAdapter({
    dataset: data,
    indexes: [{ label: 'Person', properties: ['name'], unique: true }]
  });
  let used = false;
  const orig = adapter.indexLookup.bind(adapter);
  adapter.indexLookup = async function*(label, prop, value) {
    used = true;
    for await (const n of orig(label, prop, value)) {
      yield n;
    }
  };
  const engine = new CypherEngine({ adapter });
  const out = [];
  for await (const row of engine.run('MATCH (n:Person) WHERE n.name = "name5" RETURN n')) {
    out.push(row.n);
  }
  assert.strictEqual(out.length, 1);
  assert.ok(used);
});

test('index lookup faster than scan on large dataset', async () => {
  const data = makeDataset(10000);
  const query = 'MATCH (n:Person {name:"name9999"}) RETURN n';

  const scanAdapter = new JsonAdapter({ dataset: JSON.parse(JSON.stringify(data)) });
  const scanEngine = new CypherEngine({ adapter: scanAdapter });
  const t1 = process.hrtime.bigint();
  for await (const _ of scanEngine.run(query)) {}
  const durScan = Number(process.hrtime.bigint() - t1);

  const idxAdapter = new JsonAdapter({ dataset: JSON.parse(JSON.stringify(data)), indexes: [{ label: 'Person', properties: ['name'], unique: true }] });
  const idxEngine = new CypherEngine({ adapter: idxAdapter });
  const t2 = process.hrtime.bigint();
  for await (const _ of idxEngine.run(query)) {}
  const durIdx = Number(process.hrtime.bigint() - t2);

  assert.ok(durIdx < durScan, `index ${durIdx} >= scan ${durScan}`);
});

test('planner uses index for WHERE IN list', async () => {
  const data = makeDataset(10);
  const adapter = new JsonAdapter({
    dataset: data,
    indexes: [{ label: 'Person', properties: ['name'], unique: true }]
  });
  let used = false;
  const orig = adapter.indexLookup.bind(adapter);
  adapter.indexLookup = async function*(label, prop, value) {
    used = true;
    for await (const n of orig(label, prop, value)) {
      yield n;
    }
  };
  const engine = new CypherEngine({ adapter });
  const out = [];
  for await (const row of engine.run('MATCH (n:Person) WHERE n.name IN ["name5"] RETURN n')) {
    out.push(row.n);
  }
  assert.strictEqual(out.length, 1);
  assert.ok(used);
});

test('planner uses getNodeById for WHERE id(n) equality', async () => {
  const data = makeDataset(10);
  const adapter = new JsonAdapter({ dataset: data });
  let used = false;
  const orig = adapter.getNodeById.bind(adapter);
  adapter.getNodeById = async id => {
    used = true;
    return await orig(id);
  };
  const engine = new CypherEngine({ adapter });
  const out = [];
  for await (const row of engine.run('MATCH (n) WHERE id(n) = 5 RETURN n')) {
    out.push(row.n);
  }
  assert.strictEqual(out.length, 1);
  assert.ok(used);
});
