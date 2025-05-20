const test = require('node:test');
const assert = require('node:assert');
const { SqlJsSchemaAdapter } = require('../packages/adapters/sqljs-schema-adapter/dist');

function makeAdapter(options) {
  return new SqlJsSchemaAdapter(options);
}

test('schema with table-defined labels', async () => {
  const schema = {
    nodes: [
      { table: 'people', id: 'id', labels: ['Person'], properties: ['name'] }
    ],
    relationships: [
      { table: 'knows', id: 'id', start: 'src', end: 'dst', type: 'KNOWS', properties: [] }
    ]
  };
  const adapter = makeAdapter({
    schema,
    setup(db) {
      db.run('CREATE TABLE people (id INTEGER, name TEXT);');
      db.run('CREATE TABLE knows (id INTEGER, src INTEGER, dst INTEGER);');
      db.run('INSERT INTO people VALUES (1, "Alice"), (2, "Bob");');
      db.run('INSERT INTO knows VALUES (3,1,2);');
    }
  });
  const nodes = [];
  for await (const n of adapter.scanNodes()) nodes.push(n);
  assert.strictEqual(nodes.length, 2);
  assert.deepStrictEqual(nodes[0].labels, ['Person']);
  const rels = [];
  for await (const r of adapter.scanRelationships()) rels.push(r);
  assert.strictEqual(rels.length, 1);
  assert.strictEqual(rels[0].type, 'KNOWS');
});

test('labels and types from columns', async () => {
  const schema = {
    nodes: [
      { table: 'nodes', id: 'id', labelColumn: 'label', properties: ['name'] }
    ],
    relationships: [
      { table: 'edges', id: 'id', start: 'start', end: 'end', typeColumn: 'type', properties: [] }
    ]
  };
  const adapter = makeAdapter({
    schema,
    setup(db) {
      db.run('CREATE TABLE nodes (id INTEGER, label TEXT, name TEXT);');
      db.run('CREATE TABLE edges (id INTEGER, type TEXT, start INTEGER, end INTEGER);');
      db.run('INSERT INTO nodes VALUES (1, "Person", "Alice"), (2, "Movie", "Matrix");');
      db.run('INSERT INTO edges VALUES (3, "ACTED_IN", 1, 2);');
    }
  });
  const people = [];
  for await (const n of adapter.scanNodes({ label: 'Person' })) people.push(n);
  assert.strictEqual(people.length, 1);
  assert.strictEqual(people[0].properties.name, 'Alice');
  const rel = await adapter.getRelationshipById(3);
  assert.ok(rel);
  assert.strictEqual(rel.type, 'ACTED_IN');
});

test('multiple node tables and implicit edge type', async () => {
  const schema = {
    nodes: [
      { table: 'people', id: 'pid', labels: ['Person'], properties: ['name'] },
      { table: 'movies', id: 'mid', labels: ['Movie'], properties: ['title'] }
    ],
    relationships: [
      { table: 'acted', id: 'id', start: 'person', end: 'movie', type: 'ACTED_IN', properties: [] }
    ]
  };
  const adapter = makeAdapter({
    schema,
    setup(db) {
      db.run('CREATE TABLE people (pid INTEGER, name TEXT);');
      db.run('CREATE TABLE movies (mid INTEGER, title TEXT);');
      db.run('CREATE TABLE acted (id INTEGER, person INTEGER, movie INTEGER);');
      db.run('INSERT INTO people VALUES (1, "Alice");');
      db.run('INSERT INTO movies VALUES (2, "Matrix");');
      db.run('INSERT INTO acted VALUES (3, 1, 2);');
    }
  });
  const persons = [];
  for await (const n of adapter.scanNodes({ label: 'Person' })) persons.push(n);
  assert.strictEqual(persons.length, 1);
  const movies = [];
  for await (const n of adapter.scanNodes({ label: 'Movie' })) movies.push(n);
  assert.strictEqual(movies.length, 1);
  const rels = [];
  for await (const r of adapter.scanRelationships()) rels.push(r);
  assert.strictEqual(rels.length, 1);
  assert.strictEqual(rels[0].startNode, 1);
});
