const test = require('node:test');
const assert = require('node:assert');
const { SqlJsAdapter } = require('../packages/adapters/sqljs-adapter/dist');

const data = { nodes: [], relationships: [] };

const adapter = new SqlJsAdapter({ dataset: data });

test('transpile simple match with label', () => {
  const result = adapter.transpile('MATCH (n:Person) RETURN n');
  assert.ok(result, 'should be transpilable');
  assert.strictEqual(result.sql, 'SELECT id, labels, properties FROM nodes WHERE labels LIKE ?');
  assert.deepStrictEqual(result.params, ['%"Person"%']);
});

test('transpile match with property', () => {
  const result = adapter.transpile('MATCH (n:Person {name:"Alice"}) RETURN n');
  assert.ok(result, 'should be transpilable');
  assert.strictEqual(result.sql, 'SELECT id, labels, properties FROM nodes WHERE labels LIKE ? AND json_extract(properties, \'$.name\') = ?');
  assert.deepStrictEqual(result.params, ['%"Person"%', 'Alice']);
});

test('unsupported query returns null', () => {
  const res = adapter.transpile('CREATE (n)');
  assert.strictEqual(res, null);
});

test('transpile with parameter', () => {
  const result = adapter.transpile('MATCH (n:Person {name:$name}) RETURN n', { name: 'Bob' });
  assert.ok(result);
  assert.strictEqual(result.sql, 'SELECT id, labels, properties FROM nodes WHERE labels LIKE ? AND json_extract(properties, \'$.name\') = ?');
  assert.deepStrictEqual(result.params, ['%"Person"%', 'Bob']);
});

test('transpile with WHERE comparison', () => {
  const q = 'MATCH (n:Person) WHERE n.age > 30 RETURN n';
  const result = adapter.transpile(q);
  assert.ok(result);
  assert.strictEqual(result.sql, 'SELECT id, labels, properties FROM nodes WHERE labels LIKE ? AND json_extract(properties, \'$.age\') > ?');
  assert.deepStrictEqual(result.params, ['%"Person"%', 30]);
});

test('transpile with WHERE IN list', () => {
  const q = 'MATCH (n:Person) WHERE n.name IN ["Alice","Bob"] RETURN n';
  const result = adapter.transpile(q);
  assert.ok(result);
  assert.strictEqual(result.sql, 'SELECT id, labels, properties FROM nodes WHERE labels LIKE ? AND json_extract(properties, \'$.name\') IN (?, ?)');
  assert.deepStrictEqual(result.params, ['%"Person"%', 'Alice', 'Bob']);
});
