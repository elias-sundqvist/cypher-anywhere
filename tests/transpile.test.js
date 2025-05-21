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
