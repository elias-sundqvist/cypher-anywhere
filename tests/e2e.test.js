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

runOnAdapters('set property from other nodes via concatenation', async engine => {
  const script =
    'MATCH (a:Person {name:"Alice"}) RETURN a; ' +
    'MATCH (b:Person {name:"Bob"}) RETURN b; ' +
    'MATCH (m:Movie {title:"The Matrix"}) SET m.tag = a.name + "-" + b.name RETURN m';
  let row;
  for await (const r of engine.run(script)) row = r;
  assert.strictEqual(row.m.properties.tag, 'Alice-Bob');
});

runOnAdapters('set property on multiple nodes using expression', async engine => {
  const script =
    'MATCH (g:Genre {name:"Action"}) RETURN g; ' +
    'MATCH (p:Person) SET p.label = p.name + ":" + g.name RETURN p';
  const out = [];
  for await (const row of engine.run(script)) if (row.p) out.push(row.p);
  assert.strictEqual(out.length, 3);
  for (const p of out) {
    assert.strictEqual(p.properties.label, `${p.properties.name}:Action`);
  }
});

runOnAdapters('match with WHERE filter', async engine => {
  const out = [];
  for await (const row of engine.run('MATCH (n:Person) WHERE n.name = "Alice" RETURN n')) out.push(row.n);
  assert.strictEqual(out.length, 1);
  assert.strictEqual(out[0].properties.name, 'Alice');
});

runOnAdapters('match with WHERE inequality', async engine => {
  const out = [];
  for await (const row of engine.run('MATCH (m:Movie) WHERE m.released > 2000 RETURN m')) out.push(row.m);
  assert.strictEqual(out.length, 1);
  assert.strictEqual(out[0].properties.title, 'John Wick');
});

runOnAdapters('match with WHERE less than inequality', async engine => {
  const out = [];
  for await (const row of engine.run('MATCH (m:Movie) WHERE m.released < 2000 RETURN m')) out.push(row.m);
  assert.strictEqual(out.length, 1);
  assert.strictEqual(out[0].properties.title, 'The Matrix');
});

runOnAdapters('match relationship with WHERE', async engine => {
  const out = [];
  for await (const row of engine.run('MATCH ()-[r:ACTED_IN]->() WHERE r.role = "Neo" SET r.flag = true RETURN r')) out.push(row.r);
  assert.strictEqual(out.length, 1);
  assert.strictEqual(out[0].properties.flag, true);
});

runOnAdapters('match all ACTED_IN relationships', async engine => {
  const out = [];
  for await (const row of engine.run('MATCH ()-[r:ACTED_IN]->() RETURN r')) out.push(row.r);
  assert.strictEqual(out.length, 3);
});

runOnAdapters('match with WHERE using AND', async engine => {
  const out = [];
  for await (const row of engine.run('MATCH (n:Person) WHERE n.name = "Alice" AND n.name = "Bob" RETURN n')) out.push(row.n);
  assert.strictEqual(out.length, 0);
});

runOnAdapters('match with WHERE using OR', async engine => {
  const out = [];
  for await (const row of engine.run('MATCH (n:Person) WHERE n.name = "Alice" OR n.name = "Bob" RETURN n')) out.push(row.n);
  assert.strictEqual(out.length, 2);
});

runOnAdapters('match with WHERE using NOT', async engine => {
  const out = [];
  for await (const row of engine.run('MATCH (n:Person) WHERE NOT n.name = "Alice" RETURN n')) out.push(row.n);
  assert.strictEqual(out.length, 2);
});

runOnAdapters('match with WHERE not equals operator', async engine => {
  const out = [];
  for await (const row of engine.run('MATCH (n:Person) WHERE n.name <> "Alice" RETURN n')) out.push(row.n);
  assert.strictEqual(out.length, 2);
});

runOnAdapters('match with WHERE IN list', async engine => {
  const out = [];
  const q = 'MATCH (n:Person) WHERE n.name IN ["Alice", "Bob"] RETURN n';
  for await (const row of engine.run(q)) out.push(row.n.properties.name);
  assert.deepStrictEqual(out.sort(), ['Alice', 'Bob']);
});

runOnAdapters('FOREACH create multiple nodes', async engine => {
  for await (const _ of engine.run('FOREACH x IN [1,2,3] CREATE (n:Batch)')) {}
  const out = [];
  for await (const row of engine.run('MATCH (n:Batch) RETURN n')) out.push(row.n);
  assert.strictEqual(out.length, 3);
});

runOnAdapters('FOREACH variable drives SET', async engine => {
  const script =
    'CREATE (c:Counter {num:0}) RETURN c; ' +
    'FOREACH v IN [1,2,3] MATCH (c:Counter) SET c.num = v; ' +
    'MATCH (c:Counter) RETURN c';
  let last;
  for await (const row of engine.run(script)) if (row.c) last = row.c;
  assert.strictEqual(last.properties.num, 3);
});

runOnAdapters('FOREACH over path nodes sets property', async engine => {
  const script =
    'MATCH p=(a:Person {name:"Alice"})-[*]->(g:Genre {name:"Action"}); ' +
    'FOREACH n IN nodes(p) MATCH (n) SET n.marked = true; ' +
    'MATCH (n) WHERE n.marked = true RETURN n';
  const out = [];
  for await (const row of engine.run(script)) if (row.n) out.push(row.n);
  assert.strictEqual(out.length, 3);
});

runOnAdapters('multi-hop ->()-> chain returns final node', async engine => {
  const out = [];
  const q =
    'MATCH (p:Person {name:"Alice"})-[r1:ACTED_IN]->(m)-[r2:IN_GENRE]->(g) RETURN g';
  for await (const row of engine.run(q)) out.push(row.g);
  assert.strictEqual(out.length, 1);
  assert.strictEqual(out[0].properties.name, 'Action');
});

runOnAdapters('multi-hop ->()<- chain', async engine => {
  const out = [];
  const q =
    'MATCH (a:Person {name:"Alice"})-[r1:ACTED_IN]->(m)<-[r2:ACTED_IN]-(b:Person {name:"Bob"}) RETURN b';
  for await (const row of engine.run(q)) out.push(row.b);
  assert.strictEqual(out.length, 1);
  assert.strictEqual(out[0].properties.name, 'Bob');
});

runOnAdapters('multi-hop <-()-> chain', async engine => {
  const out = [];
  const q =
    'MATCH (m1:Movie {title:"The Matrix"})<-[r1:ACTED_IN]-(p:Person)-[r2:ACTED_IN]->(m2:Movie {title:"John Wick"}) RETURN m2';
  for await (const row of engine.run(q)) out.push(row.m2);
  assert.strictEqual(out.length, 1);
  assert.strictEqual(out[0].properties.title, 'John Wick');
});

runOnAdapters('multi-hop <-()<-, creating extra rel', async engine => {
  const setup =
    'MATCH (b:Person {name:"Bob"}) RETURN b; MATCH (c:Person {name:"Carol"}) RETURN c; MERGE (c)-[k:KNOWS]->(b) RETURN k';
  for await (const _ of engine.run(setup)) {}
  const out = [];
  const q =
    'MATCH (m:Movie {title:"John Wick"})<-[a:ACTED_IN]-(b:Person)<-[k:KNOWS]-(c:Person {name:"Carol"}) RETURN c';
  for await (const row of engine.run(q)) out.push(row.c);
  assert.strictEqual(out.length, 1);
  assert.strictEqual(out[0].properties.name, 'Carol');
});

runOnAdapters('multi-hop chain length 3', async engine => {
  const setup =
    'MATCH (g:Genre {name:"Action"}) RETURN g; CREATE (s:SubGenre {name:"Thriller"}) RETURN s; MERGE (g)-[r:HAS_SUB]->(s) RETURN r';
  for await (const _ of engine.run(setup)) {}
  const out = [];
  const q =
    'MATCH (p:Person {name:"Alice"})-[r1:ACTED_IN]->(m)-[r2:IN_GENRE]->(g)-[r3:HAS_SUB]->(s) RETURN s';
  for await (const row of engine.run(q)) out.push(row.s);
  assert.strictEqual(out.length, 1);
  assert.strictEqual(out[0].properties.name, 'Thriller');
});

runOnAdapters('return numeric addition expression', async engine => {
  const out = [];
  for await (const row of engine.run('MATCH (m:Movie) RETURN m.released+3')) out.push(row.value);
  assert.deepStrictEqual(out.sort(), [2002, 2017]);
});

runOnAdapters('create then set properties', async engine => {
  let node;
  for await (const row of engine.run("CREATE (p:Temp) SET p.name='Greg', p.age=32 RETURN p"))
    node = row.p;
  assert.strictEqual(node.properties.name, 'Greg');
  assert.strictEqual(node.properties.age, 32);
});

runOnAdapters('merge with ON CREATE SET', async engine => {
  let node;
  for await (const row of engine.run("MERGE (p:TempMerge {id:1}) ON CREATE SET p.flag=true RETURN p"))
    node = row.p;
  assert.strictEqual(node.properties.flag, true);
  for await (const row of engine.run("MERGE (p:TempMerge {id:1}) ON CREATE SET p.flag=false RETURN p"))
    node = row.p;
  assert.strictEqual(node.properties.flag, true);
});

runOnAdapters('create node with list property', async engine => {
  let ev;
  for await (const row of engine.run("CREATE (e:Event {tags:['neo4j','conf']}) RETURN e"))
    ev = row.e;
  assert.deepStrictEqual(ev.properties.tags, ['neo4j', 'conf']);
});

runOnAdapters('UNWIND literal list returns rows', async engine => {
  const out = [];
  for await (const row of engine.run('UNWIND [1,2,3] AS x RETURN x')) out.push(row.x);
  assert.deepStrictEqual(out.sort(), [1, 2, 3]);
});

runOnAdapters('UNWIND expression on list items', async engine => {
  const out = [];
  for await (const row of engine.run('UNWIND [1,2,3] AS x RETURN x + 1')) out.push(row.value);
  assert.deepStrictEqual(out.sort(), [2, 3, 4]);
});

runOnAdapters('UNWIND nodes from path', async engine => {
  const script =
    'MATCH p=(a:Person {name:"Alice"})-[*]->(g:Genre {name:"Action"}) RETURN p; ' +
    'UNWIND nodes(p) AS n RETURN n';
  const out = [];
  for await (const row of engine.run(script)) if (row.n) out.push(row.n);
  assert.strictEqual(out.length, 3);
});

runOnAdapters('OPTIONAL MATCH missing returns null row', async engine => {
  const out = [];
  for await (const row of engine.run('OPTIONAL MATCH (n:Missing) RETURN n')) out.push(row.n);
  assert.strictEqual(out.length, 1);
  assert.strictEqual(out[0], undefined);
});

runOnAdapters('OPTIONAL MATCH existing node returns it', async engine => {
  const out = [];
  const q = 'OPTIONAL MATCH (n:Person {name:"Alice"}) RETURN n';
  for await (const row of engine.run(q)) out.push(row.n);
  assert.strictEqual(out.length, 1);
  assert.strictEqual(out[0].properties.name, 'Alice');
});

runOnAdapters('ORDER BY with SKIP and LIMIT', async engine => {
  const q = 'MATCH (n:Person) RETURN n.name AS name ORDER BY n.name SKIP 1 LIMIT 1';
  const out = [];
  for await (const row of engine.run(q)) out.push(row.name);
  assert.deepStrictEqual(out, ['Bob']);
});

runOnAdapters('ORDER BY DESC', async engine => {
  const q = 'MATCH (m:Movie) RETURN m.released AS year ORDER BY year DESC';
  const out = [];
  for await (const row of engine.run(q)) out.push(row.year);
  assert.deepStrictEqual(out, [2014, 1999]);
});

runOnAdapters('RETURN multiple expressions with aliases', async engine => {
  const q = 'MATCH (m:Movie) RETURN m.title AS title, m.released AS year ORDER BY year';
  const out = [];
  for await (const row of engine.run(q)) out.push(`${row.title}-${row.year}`);
  assert.deepStrictEqual(out, ['The Matrix-1999', 'John Wick-2014']);
});

runOnAdapters('MATCH with parameter property', async engine => {
  const q = 'MATCH (n:Person {name:$name}) RETURN n';
  const out = [];
  for await (const row of engine.run(q, { name: 'Alice' })) out.push(row.n);
  assert.strictEqual(out.length, 1);
  assert.strictEqual(out[0].properties.name, 'Alice');
});

runOnAdapters('WHERE clause with parameter', async engine => {
  const q = 'MATCH (m:Movie) WHERE m.released > $year RETURN m';
  const out = [];
  for await (const row of engine.run(q, { year: 2000 })) out.push(row.m);
  assert.strictEqual(out.length, 1);
  assert.strictEqual(out[0].properties.title, 'John Wick');
});

runOnAdapters('SET property using parameter', async engine => {
  const q = 'MATCH (n:Person {name:"Bob"}) SET n.age = $age RETURN n';
  let node;
  for await (const row of engine.run(q, { age: 55 })) node = row.n;
  assert.strictEqual(node.properties.age, 55);
});

runOnAdapters('COUNT aggregation', async engine => {
  const out = [];
  for await (const row of engine.run('MATCH (m:Movie) RETURN COUNT(m)')) out.push(row.value);
  assert.strictEqual(out[0], 2);
});

runOnAdapters('SUM aggregation', async engine => {
  const out = [];
  for await (const row of engine.run('MATCH (m:Movie) RETURN SUM(m.released)')) out.push(row.value);
  assert.strictEqual(out[0], 1999 + 2014);
});

runOnAdapters('GROUP BY with COUNT', async engine => {
  const q = 'MATCH (m:Movie) RETURN m.released AS year, COUNT(m) AS cnt';
  const res = {};
  for await (const row of engine.run(q)) res[row.year] = row.cnt;
  assert.strictEqual(res[1999], 1);
  assert.strictEqual(res[2014], 1);
});

runOnAdapters('ORDER BY after aggregation', async engine => {
  for await (const _ of engine.run('CREATE (m:Movie {title:"Extra", released:2014})')) {}
  const q =
    'MATCH (m:Movie) RETURN m.released AS year, COUNT(m) AS cnt ORDER BY cnt DESC';
  const out = [];
  for await (const row of engine.run(q)) out.push(`${row.year}:${row.cnt}`);
  assert.deepStrictEqual(out, ['2014:2', '1999:1']);
});

runOnAdapters('UNION combines results', async engine => {
  const q =
    'MATCH (p:Person {name:"Alice"}) RETURN p.name AS name ' +
    'UNION ' +
    'MATCH (p:Person {name:"Bob"}) RETURN p.name AS name';
  const out = [];
  for await (const row of engine.run(q)) out.push(row.name);
  assert.deepStrictEqual(out.sort(), ['Alice', 'Bob']);
});

runOnAdapters('CALL subquery returns rows', async engine => {
  const q = 'CALL { MATCH (p:Person {name:"Alice"}) RETURN p } RETURN p';
  const out = [];
  for await (const row of engine.run(q)) out.push(row.p);
  assert.strictEqual(out.length, 1);
  assert.strictEqual(out[0].properties.name, 'Alice');
});

runOnAdapters('single hop match without rel variable', async engine => {
  const q = 'MATCH (p:Person)-[:ACTED_IN]->(m:Movie) RETURN p, m';
  const out = [];
  for await (const row of engine.run(q)) out.push(`${row.p.properties.name}-${row.m.properties.title}`);
  const expected = ['Alice-The Matrix', 'Alice-John Wick', 'Bob-John Wick'];
  assert.deepStrictEqual(out.sort(), expected.sort());
});

runOnAdapters('single hop incoming match without rel variable', async engine => {
  const q = 'MATCH (m:Movie)<-[:ACTED_IN]-(p:Person) RETURN p, m';
  const out = [];
  for await (const row of engine.run(q)) out.push(`${row.p.properties.name}-${row.m.properties.title}`);
  const expected = ['Alice-The Matrix', 'Alice-John Wick', 'Bob-John Wick'];
  assert.deepStrictEqual(out.sort(), expected.sort());
});

runOnAdapters('single hop match without labels', async engine => {
  const q = 'MATCH (a)-[:ACTED_IN]->(b) RETURN a, b';
  const out = [];
  for await (const row of engine.run(q)) out.push(`${row.a.properties.name}-${row.b.properties.title}`);
  const expected = ['Alice-The Matrix', 'Alice-John Wick', 'Bob-John Wick'];
  assert.deepStrictEqual(out.sort(), expected.sort());
});

runOnAdapters('single hop incoming match without labels', async engine => {
  const q = 'MATCH (b)<-[:ACTED_IN]-(a) RETURN a, b';
  const out = [];
  for await (const row of engine.run(q)) out.push(`${row.a.properties.name}-${row.b.properties.title}`);
  const expected = ['Alice-The Matrix', 'Alice-John Wick', 'Bob-John Wick'];
  assert.deepStrictEqual(out.sort(), expected.sort());
});

runOnAdapters('undirected single hop match', async engine => {
  const q = 'MATCH (p:Person)-[:ACTED_IN]-(m:Movie) RETURN p, m';
  const out = [];
  for await (const row of engine.run(q)) out.push(`${row.p.properties.name}-${row.m.properties.title}`);
  const expected = ['Alice-The Matrix', 'Alice-John Wick', 'Bob-John Wick'];
  assert.deepStrictEqual(out.sort(), expected.sort());
});

runOnAdapters('negative numeric literals parsed correctly', async engine => {
  let node;
  for await (const row of engine.run('CREATE (n:Neg {val:-5}) RETURN n')) node = row.n;
  assert.strictEqual(node.properties.val, -5);
  const out = [];
  for await (const row of engine.run('MATCH (n:Neg) WHERE n.val < -1 RETURN n')) out.push(row.n);
  assert.strictEqual(out.length, 1);
});
