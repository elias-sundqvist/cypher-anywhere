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

test('transpile with WHERE NOT', () => {
  const q = 'MATCH (n:Person) WHERE NOT n.name = "Alice" RETURN n';
  const result = adapter.transpile(q);
  assert.ok(result);
  assert.strictEqual(
    result.sql,
    'SELECT id, labels, properties FROM nodes WHERE labels LIKE ? AND NOT (json_extract(properties, \'$.name\') = ?)'
  );
  assert.deepStrictEqual(result.params, ['%"Person"%', 'Alice']);
});

test('transpile with WHERE OR', () => {
  const q = 'MATCH (n:Person) WHERE n.name = "Alice" OR n.name = "Bob" RETURN n';
  const result = adapter.transpile(q);
  assert.ok(result);
  assert.strictEqual(
    result.sql,
    'SELECT id, labels, properties FROM nodes WHERE labels LIKE ? AND (json_extract(properties, \'$.name\') = ? OR json_extract(properties, \'$.name\') = ?)'
  );
  assert.deepStrictEqual(result.params, ['%"Person"%', 'Alice', 'Bob']);
});

test('transpile with WHERE IS NULL', () => {
  const q = 'MATCH (n:Person) WHERE n.age IS NULL RETURN n';
  const result = adapter.transpile(q);
  assert.ok(result);
  assert.strictEqual(
    result.sql,
    'SELECT id, labels, properties FROM nodes WHERE labels LIKE ? AND json_extract(properties, \'$.age\') IS NULL'
  );
  assert.deepStrictEqual(result.params, ['%"Person"%']);
});

test('transpile with WHERE IS NOT NULL', () => {
  const q = 'MATCH (n:Person) WHERE n.age IS NOT NULL RETURN n';
  const result = adapter.transpile(q);
  assert.ok(result);
  assert.strictEqual(
    result.sql,
    'SELECT id, labels, properties FROM nodes WHERE labels LIKE ? AND json_extract(properties, \'$.age\') IS NOT NULL'
  );
  assert.deepStrictEqual(result.params, ['%"Person"%']);
});

test('transpile with WHERE STARTS WITH', () => {
  const q = 'MATCH (n:Person) WHERE n.name STARTS WITH "Al" RETURN n';
  const result = adapter.transpile(q);
  assert.ok(result);
  assert.strictEqual(
    result.sql,
    'SELECT id, labels, properties FROM nodes WHERE labels LIKE ? AND json_extract(properties, \'$.name\') LIKE ?'
  );
  assert.deepStrictEqual(result.params, ['%"Person"%', 'Al%']);
});

test('transpile with WHERE ENDS WITH', () => {
  const q = 'MATCH (n:Person) WHERE n.name ENDS WITH "ce" RETURN n';
  const result = adapter.transpile(q);
  assert.ok(result);
  assert.strictEqual(
    result.sql,
    'SELECT id, labels, properties FROM nodes WHERE labels LIKE ? AND json_extract(properties, \'$.name\') LIKE ?'
  );
  assert.deepStrictEqual(result.params, ['%"Person"%', '%ce']);
});

test('transpile with WHERE CONTAINS', () => {
  const q = 'MATCH (n:Person) WHERE n.name CONTAINS "li" RETURN n';
  const result = adapter.transpile(q);
  assert.ok(result);
  assert.strictEqual(
    result.sql,
    'SELECT id, labels, properties FROM nodes WHERE labels LIKE ? AND json_extract(properties, \'$.name\') LIKE ?'
  );
  assert.deepStrictEqual(result.params, ['%"Person"%', '%li%']);
});

test('transpile with WHERE not equals', () => {
  const q = 'MATCH (n:Person) WHERE n.name <> "Alice" RETURN n';
  const result = adapter.transpile(q);
  assert.ok(result);
  assert.strictEqual(
    result.sql,
    'SELECT id, labels, properties FROM nodes WHERE labels LIKE ? AND json_extract(properties, \'$.name\') <> ?'
  );
  assert.deepStrictEqual(result.params, ['%"Person"%', 'Alice']);
});

test('transpile IN empty list yields constant false', () => {
  const q = 'MATCH (n:Person) WHERE n.name IN [] RETURN n';
  const result = adapter.transpile(q);
  assert.ok(result);
  assert.strictEqual(
    result.sql,
    'SELECT id, labels, properties FROM nodes WHERE labels LIKE ? AND 0'
  );
  assert.deepStrictEqual(result.params, ['%"Person"%']);
});

test('transpile match with multiple labels', () => {
  const q = 'MATCH (n:Person:Actor) RETURN n';
  const result = adapter.transpile(q);
  assert.ok(result);
  assert.strictEqual(
    result.sql,
    'SELECT id, labels, properties FROM nodes WHERE labels LIKE ? AND labels LIKE ?'
  );
  assert.deepStrictEqual(result.params, ['%"Person"%', '%"Actor"%']);
});

test('transpile match with null property', () => {
  const q = 'MATCH (n:Person {nickname:null}) RETURN n';
  const result = adapter.transpile(q);
  assert.ok(result);
  assert.strictEqual(
    result.sql,
    'SELECT id, labels, properties FROM nodes WHERE labels LIKE ? AND json_extract(properties, \'$.nickname\') IS NULL'
  );
  assert.deepStrictEqual(result.params, ['%"Person"%']);
});

test('transpile WHERE equals null', () => {
  const q = 'MATCH (n:Person) WHERE n.nickname = null RETURN n';
  const result = adapter.transpile(q);
  assert.ok(result);
  assert.strictEqual(
    result.sql,
    'SELECT id, labels, properties FROM nodes WHERE labels LIKE ? AND json_extract(properties, \'$.nickname\') IS NULL'
  );
  assert.deepStrictEqual(result.params, ['%"Person"%']);
});

test('transpile with WHERE <= comparison', () => {
  const q = 'MATCH (n:Person) WHERE n.age <= 40 RETURN n';
  const result = adapter.transpile(q);
  assert.ok(result);
  assert.strictEqual(
    result.sql,
    'SELECT id, labels, properties FROM nodes WHERE labels LIKE ? AND json_extract(properties, \'$.age\') <= ?'
  );
  assert.deepStrictEqual(result.params, ['%"Person"%', 40]);
});

test('transpile with complex WHERE', () => {
  const q = 'MATCH (n:Person) WHERE n.name = "Alice" AND (n.age > 30 OR n.age IS NULL) RETURN n';
  const result = adapter.transpile(q);
  assert.ok(result);
  assert.strictEqual(
    result.sql,
    'SELECT id, labels, properties FROM nodes WHERE labels LIKE ? AND (json_extract(properties, \'$.name\') = ? AND (json_extract(properties, \'$.age\') > ? OR json_extract(properties, \'$.age\') IS NULL))'
  );
  assert.deepStrictEqual(result.params, ['%"Person"%', 'Alice', 30]);
});

test('transpile with WHERE >= comparison', () => {
  const q = 'MATCH (n:Person) WHERE n.age >= 20 RETURN n';
  const result = adapter.transpile(q);
  assert.ok(result);
  assert.strictEqual(
    result.sql,
    'SELECT id, labels, properties FROM nodes WHERE labels LIKE ? AND json_extract(properties, \'$.age\') >= ?'
  );
  assert.deepStrictEqual(result.params, ['%"Person"%', 20]);
});

test('transpile with WHERE < comparison', () => {
  const q = 'MATCH (n:Person) WHERE n.age < 50 RETURN n';
  const result = adapter.transpile(q);
  assert.ok(result);
  assert.strictEqual(
    result.sql,
    'SELECT id, labels, properties FROM nodes WHERE labels LIKE ? AND json_extract(properties, \'$.age\') < ?'
  );
  assert.deepStrictEqual(result.params, ['%"Person"%', 50]);
});

test('transpile with negative comparison', () => {
  const q = 'MATCH (n:Person) WHERE n.age < -5 RETURN n';
  const result = adapter.transpile(q);
  assert.ok(result);
  assert.strictEqual(
    result.sql,
    'SELECT id, labels, properties FROM nodes WHERE labels LIKE ? AND json_extract(properties, \'$.age\') < ?'
  );
  assert.deepStrictEqual(result.params, ['%"Person"%', -5]);
});

test('transpile returns null for ORDER BY', () => {
  const q = 'MATCH (n:Person) RETURN n ORDER BY n.name';
  const result = adapter.transpile(q);
  assert.strictEqual(result, null);
});

test('transpile returns null for LIMIT', () => {
  const q = 'MATCH (n:Person) RETURN n LIMIT 1';
  const result = adapter.transpile(q);
  assert.strictEqual(result, null);
});

test('transpile returns null for relationship match', () => {
  const q = 'MATCH ()-[r:REL]->() RETURN r';
  const result = adapter.transpile(q);
  assert.strictEqual(result, null);
});
