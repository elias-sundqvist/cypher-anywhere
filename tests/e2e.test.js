const test = require('node:test');
const assert = require('node:assert');
const { JsonAdapter } = require('../packages/adapters/json-adapter/dist');
const { CypherEngine } = require('../packages/core/dist');

const baseData = {
  nodes: [
    { id: 1, labels: ['Person'], properties: { name: 'Alice' } },
    { id: 2, labels: ['Person'], properties: { name: 'Bob' } },
    { id: 3, labels: ['Movie'], properties: { title: 'The Matrix', released: 1999 } },
    { id: 4, labels: ['Movie'], properties: { title: 'John Wick', released: 2014 } },
    { id: 5, labels: ['Person', 'Actor'], properties: { name: 'Carol' } },
    { id: 6, labels: ['Genre'], properties: { name: 'Action' } }
  ],
  relationships: [
    { id: 7, type: 'ACTED_IN', startNode: 1, endNode: 3, properties: { role: 'Neo' } },
    { id: 8, type: 'ACTED_IN', startNode: 1, endNode: 4, properties: { role: 'John' } },
    { id: 9, type: 'ACTED_IN', startNode: 2, endNode: 4, properties: { role: 'Buddy' } },
    { id: 10, type: 'IN_GENRE', startNode: 3, endNode: 6, properties: {} }
  ]
};

const adapterFactories = {
  json: () => new JsonAdapter({
    dataset: JSON.parse(JSON.stringify(baseData)),
    indexes: [
      { label: 'Person', properties: ['name'], unique: true },
      { label: 'Movie', properties: ['title'], unique: true }
    ]
  })
};

function runOnAdapters(name, fn) {
  for (const [kind, factory] of Object.entries(adapterFactories)) {
    test(`${name} (${kind})`, async () => {
      const adapter = factory();
      const engine = new CypherEngine({ adapter });
      await fn(engine, adapter);
    });
  }
}

runOnAdapters('MATCH all nodes', async engine => {
  const out = [];
  for await (const row of engine.run('MATCH (n) RETURN n')) out.push(row.n);
  assert.strictEqual(out.length, baseData.nodes.length);
});

runOnAdapters('MATCH with label', async engine => {
  const out = [];
  for await (const row of engine.run('MATCH (n:Person) RETURN n')) out.push(row.n);
  assert.strictEqual(out.length, 3);
});

runOnAdapters('MATCH with property filter', async engine => {
  const out = [];
  for await (const row of engine.run('MATCH (n:Person {name:"Alice"}) RETURN n'))
    out.push(row.n);
  assert.strictEqual(out.length, 1);
  assert.strictEqual(out[0].properties.name, 'Alice');
});

runOnAdapters('CREATE node then MATCH finds it', async engine => {
  for await (const _ of engine.run('CREATE (n:Person {name:"Dave"})')) {}
  const out = [];
  for await (const row of engine.run('MATCH (n:Person {name:"Dave"}) RETURN n'))
    out.push(row.n);
  assert.strictEqual(out.length, 1);
});

runOnAdapters('MERGE existing node returns same id', async engine => {
  let node;
  for await (const row of engine.run('MERGE (n:Person {name:"Alice"}) RETURN n'))
    node = row.n;
  assert.strictEqual(node.id, 1);
});

runOnAdapters('MERGE creates node when missing', async engine => {
  let node;
  for await (const row of engine.run('MERGE (n:Person {name:"Eve"}) RETURN n')) node = row.n;
  assert.ok(node.id > 6);
  const out = [];
  for await (const row of engine.run('MATCH (n:Person {name:"Eve"}) RETURN n')) out.push(row.n);
  assert.strictEqual(out.length, 1);
});

runOnAdapters('SET updates node property', async engine => {
  for await (const row of engine.run('MATCH (n:Person {name:"Bob"}) SET n.age = 30 RETURN n')) {
    assert.strictEqual(row.n.properties.age, 30);
  }
});

runOnAdapters('DELETE removes node', async engine => {
  for await (const _ of engine.run('MATCH (n:Person {name:"Carol"}) DELETE n')) {}
  const out = [];
  for await (const row of engine.run('MATCH (n:Person {name:"Carol"}) RETURN n')) out.push(row.n);
  assert.strictEqual(out.length, 0);
});

runOnAdapters('CREATE relationship between new nodes', async engine => {
  let rel;
  for await (const row of engine.run('CREATE (a:TmpA)-[r:REL]->(b:TmpB) RETURN r')) rel = row.r;
  assert.strictEqual(rel.type, 'REL');
});

runOnAdapters('MATCH relationship and update property', async engine => {
  for await (const row of engine.run('MATCH ()-[r:ACTED_IN {role:"John"}]->() SET r.test = true RETURN r')) {
    assert.strictEqual(row.r.properties.test, true);
  }
});

runOnAdapters('MATCH relationship with property filter update', async engine => {
  for await (const row of engine.run('MATCH ()-[r:ACTED_IN {role:"Neo"}]->() SET r.mark = 1 RETURN r')) {
    assert.strictEqual(row.r.properties.mark, 1);
  }
});

runOnAdapters('update relationship property', async engine => {
  for await (const row of engine.run('MATCH ()-[r:ACTED_IN {role:"Buddy"}]->() SET r.year = 2014 RETURN r')) {
    assert.strictEqual(row.r.properties.year, 2014);
  }
});

runOnAdapters('delete relationship', async engine => {
  for await (const _ of engine.run('MATCH ()-[r:IN_GENRE]->() DELETE r')) {}
  const out = [];
  for await (const row of engine.run('MATCH ()-[r:IN_GENRE]->() RETURN r')) out.push(row.r);
  assert.strictEqual(out.length, 0);
});

runOnAdapters('merge relationship between existing nodes', async engine => {
  const script = 'MATCH (p:Person {name:"Alice"}) RETURN p; MATCH (m:Movie {title:"John Wick"}) RETURN m; MERGE (p)-[r:ACTED_IN]->(m) RETURN r';
  const out = [];
  for await (const row of engine.run(script)) if (row.r) out.push(row.r);
  assert.strictEqual(out.length, 1);
});

runOnAdapters('create node with label', async engine => {
  for await (const _ of engine.run('CREATE (n:Tester {name:"Greg"})')) {}
  const out = [];
  for await (const row of engine.run('MATCH (n:Tester {name:"Greg"}) RETURN n')) out.push(row.n);
  assert.strictEqual(out.length, 1);
});

runOnAdapters('match by secondary label', async engine => {
  const out = [];
  for await (const row of engine.run('MATCH (n:Actor) RETURN n')) out.push(row.n);
  assert.strictEqual(out.length, 1);
});

runOnAdapters('match with multiple labels', async engine => {
  const out = [];
  for await (const row of engine.run('MATCH (n:Person:Actor) RETURN n')) out.push(row.n);
  assert.strictEqual(out.length, 1);
  assert.strictEqual(out[0].properties.name, 'Carol');
});

runOnAdapters('create node with multiple labels', async engine => {
  let node;
  for await (const row of engine.run('CREATE (n:Multi:One {v:1}) RETURN n')) node = row.n;
  assert.deepStrictEqual(node.labels.sort(), ['Multi', 'One']);
});

runOnAdapters('merge node with multiple labels', async engine => {
  let node;
  for await (const row of engine.run('MERGE (n:Multi:One {v:1}) RETURN n')) node = row.n;
  assert.deepStrictEqual(node.labels.sort(), ['Multi', 'One']);
});

runOnAdapters('multi statement create and set', async engine => {
  const script = 'CREATE (n:Temp {x:1}) RETURN n; MATCH (n:Temp {x:1}) SET n.x = 2 RETURN n';
  const out = [];
  for await (const row of engine.run(script)) if (row.n) out.push(row.n);
  assert.strictEqual(out.pop().properties.x, 2);
});

runOnAdapters('multi statement merge and delete', async engine => {
  const script = 'MERGE (n:TempDel {x:1}) RETURN n; MATCH (n:TempDel {x:1}) DELETE n';
  for await (const _ of engine.run(script)) {}
  const out = [];
  for await (const row of engine.run('MATCH (n:TempDel {x:1}) RETURN n')) out.push(row.n);
  assert.strictEqual(out.length, 0);
});

runOnAdapters('index match for person', async engine => {
  const out = [];
  for await (const row of engine.run('MATCH (n:Person {name:"Alice"}) RETURN n')) out.push(row.n);
  assert.strictEqual(out.length, 1);
});

runOnAdapters('match set return node', async engine => {
  const out = [];
  for await (const row of engine.run('MATCH (n:Person {name:"Bob"}) SET n.age = 40 RETURN n')) out.push(row.n);
  assert.strictEqual(out[0].properties.age, 40);
});

runOnAdapters('match delete with property', async engine => {
  for await (const _ of engine.run('MATCH (n:Person {name:"Bob"}) DELETE n')) {}
  const out = [];
  for await (const row of engine.run('MATCH (n:Person {name:"Bob"}) RETURN n')) out.push(row.n);
  assert.strictEqual(out.length, 0);
});

runOnAdapters('merge node with property and return', async engine => {
  let node;
  for await (const row of engine.run('MERGE (n:Person {name:"Henry"}) RETURN n')) node = row.n;
  assert.ok(node);
  assert.strictEqual(node.properties.name, 'Henry');
});

runOnAdapters('create multiple nodes sequentially', async engine => {
  for await (const _ of engine.run('CREATE (n:Seq {i:1}); CREATE (n:Seq {i:2})')) {}
  const out = [];
  for await (const row of engine.run('MATCH (n:Seq) RETURN n')) out.push(row.n);
  assert.strictEqual(out.length, 2);
});

runOnAdapters('merge relationship prevents duplicates', async (engine, adapter) => {
  const script = 'MATCH (p:Person {name:"Alice"}) RETURN p; MATCH (m:Movie {title:"The Matrix"}) RETURN m; MERGE (p)-[r:ACTED_IN]->(m) RETURN r; MERGE (p)-[r2:ACTED_IN]->(m) RETURN r2';
  for await (const _ of engine.run(script)) {}
  const out = [];
  for await (const rel of adapter.scanRelationships()) {
    if (rel.type === 'ACTED_IN') out.push(rel);
  }
  assert.strictEqual(out.length, 3);
});

runOnAdapters('relationship deleted is gone', async engine => {
  for await (const _ of engine.run('MATCH ()-[r:ACTED_IN {role:"Buddy"}]->() DELETE r')) {}
  const out = [];
  for await (const row of engine.run('MATCH ()-[r:ACTED_IN {role:"Buddy"}]->() RETURN r')) out.push(row.r);
  assert.strictEqual(out.length, 0);
});

runOnAdapters('create relationship and return nodes', async engine => {
  const script = 'CREATE (a:CR1 {x:1})-[r:REL]->(b:CR2 {y:2}) RETURN r';
  for await (const row of engine.run(script)) {
    assert.strictEqual(row.r.type, 'REL');
  }
});

runOnAdapters('update node property multiple times', async engine => {
  const script = 'MATCH (n:Person {name:"Alice"}) SET n.age = 1 RETURN n; MATCH (n:Person {name:"Alice"}) SET n.age = 2 RETURN n';
  let last;
  for await (const row of engine.run(script)) if (row.n) last = row.n;
  assert.strictEqual(last.properties.age, 2);
});

runOnAdapters('adapter lists indexes', async (_engine, adapter) => {
  const indexes = await adapter.listIndexes();
  assert.ok(Array.isArray(indexes));
  assert.strictEqual(indexes.length, 2);
});

runOnAdapters('delete node also removes its relationships', async engine => {
  for await (const _ of engine.run('MATCH (n:Genre {name:"Action"}) DELETE n')) {}
  const outRel = [];
  for await (const row of engine.run('MATCH ()-[r:IN_GENRE]->() RETURN r')) outRel.push(row.r);
  assert.strictEqual(outRel.length, 0);
});

runOnAdapters('repeated merge node does not duplicate', async engine => {
  for await (const _ of engine.run('MERGE (n:Person {name:"Alice"})')) {}
  for await (const _ of engine.run('MERGE (n:Person {name:"Alice"})')) {}
  const out = [];
  for await (const row of engine.run('MATCH (n:Person {name:"Alice"}) RETURN n')) out.push(row.n);
  assert.strictEqual(out.length, 1);
});
