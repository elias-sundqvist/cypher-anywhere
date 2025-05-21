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

test('transpile match all nodes', () => {
  const result = adapter.transpile('MATCH (n) RETURN n');
  assert.ok(result);
  assert.strictEqual(result.sql, 'SELECT id, labels, properties FROM nodes');
  assert.deepStrictEqual(result.params, []);
});

test('transpile match by property only', () => {
  const result = adapter.transpile('MATCH (n {name:"Alice"}) RETURN n');
  assert.ok(result);
  assert.strictEqual(
    result.sql,
    'SELECT id, labels, properties FROM nodes WHERE json_extract(properties, \'$.name\') = ?'
  );
  assert.deepStrictEqual(result.params, ['Alice']);
});

test('transpile match with alias', () => {
  const result = adapter.transpile('MATCH (n:Person) RETURN n AS person');
  assert.ok(result);
  assert.strictEqual(result.sql, 'SELECT id, labels, properties FROM nodes WHERE labels LIKE ?');
  assert.deepStrictEqual(result.params, ['%"Person"%']);
});

test('transpile WHERE comparison with parameter', () => {
  const q = 'MATCH (n:Person) WHERE n.age >= $age RETURN n';
  const result = adapter.transpile(q, { age: 20 });
  assert.ok(result);
  assert.strictEqual(
    result.sql,
    'SELECT id, labels, properties FROM nodes WHERE labels LIKE ? AND json_extract(properties, \'$.age\') >= ?'
  );
  assert.deepStrictEqual(result.params, ['%"Person"%', 20]);
});

test('transpile parameterized IN list', () => {
  const q = 'MATCH (n:Person) WHERE n.name IN $names RETURN n';
  const result = adapter.transpile(q, { names: ['Alice', 'Bob'] });
  assert.ok(result);
  assert.strictEqual(
    result.sql,
    'SELECT id, labels, properties FROM nodes WHERE labels LIKE ? AND json_extract(properties, \'$.name\') IN (?, ?)'
  );
  assert.deepStrictEqual(result.params, ['%"Person"%', 'Alice', 'Bob']);
});

test('transpile with WHERE comparison', () => {
  const q = 'MATCH (n:Person) WHERE n.age > 30 RETURN n';
  const result = adapter.transpile(q);
  assert.ok(result);
  assert.strictEqual(result.sql, 'SELECT id, labels, properties FROM nodes WHERE labels LIKE ? AND json_extract(properties, \'$.age\') > ?');
  assert.deepStrictEqual(result.params, ['%"Person"%', 30]);
});

test('transpile with parameterized WHERE comparison', () => {
  const q = 'MATCH (n:Person) WHERE n.age > $age RETURN n';
  const result = adapter.transpile(q, { age: 30 });
  assert.ok(result);
  assert.strictEqual(
    result.sql,
    'SELECT id, labels, properties FROM nodes WHERE labels LIKE ? AND json_extract(properties, \'$.age\') > ?'
  );
  assert.deepStrictEqual(result.params, ['%"Person"%', 30]);
});

test('transpile with WHERE IN list', () => {
  const q = 'MATCH (n:Person) WHERE n.name IN ["Alice","Bob"] RETURN n';
  const result = adapter.transpile(q);
  assert.ok(result);
  assert.strictEqual(result.sql, 'SELECT id, labels, properties FROM nodes WHERE labels LIKE ? AND json_extract(properties, \'$.name\') IN (?, ?)');
  assert.deepStrictEqual(result.params, ['%"Person"%', 'Alice', 'Bob']);
});

test('transpile with parameterized WHERE IN', () => {
  const q = 'MATCH (n:Person) WHERE n.name IN $names RETURN n';
  const result = adapter.transpile(q, { names: ['Alice', 'Bob'] });
  assert.ok(result);
  assert.strictEqual(
    result.sql,
    'SELECT id, labels, properties FROM nodes WHERE labels LIKE ? AND json_extract(properties, \'$.name\') IN (?, ?)'
  );
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

test('transpile with WHERE NOT parameter', () => {
  const q = 'MATCH (n:Person) WHERE NOT n.name = $name RETURN n';
  const result = adapter.transpile(q, { name: 'Alice' });
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

test('transpile with parameterized WHERE STARTS WITH', () => {
  const q = 'MATCH (n:Person) WHERE n.name STARTS WITH $prefix RETURN n';
  const result = adapter.transpile(q, { prefix: 'Al' });
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

test('transpile with parameterized WHERE ENDS WITH', () => {
  const q = 'MATCH (n:Person) WHERE n.name ENDS WITH $suf RETURN n';
  const result = adapter.transpile(q, { suf: 'ce' });
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

test('transpile with parameterized WHERE CONTAINS', () => {
  const q = 'MATCH (n:Person) WHERE n.name CONTAINS $sub RETURN n';
  const result = adapter.transpile(q, { sub: 'li' });
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

test('transpile COUNT nodes', () => {
  const result = adapter.transpile('MATCH (n:Person) RETURN COUNT(n)');
  assert.ok(result);
  assert.strictEqual(
    result.sql,
    'SELECT COUNT(*) AS value FROM nodes WHERE labels LIKE ?'
  );
  assert.deepStrictEqual(result.params, ['%"Person"%']);
});

test('transpile COUNT with WHERE clause', () => {
  const q = 'MATCH (n:Person) WHERE n.age > 30 RETURN COUNT(n) AS cnt';
  const result = adapter.transpile(q);
  assert.ok(result);
  assert.strictEqual(
    result.sql,
    'SELECT COUNT(*) AS value FROM nodes WHERE labels LIKE ? AND json_extract(properties, \'$.age\') > ?'
  );
  assert.deepStrictEqual(result.params, ['%"Person"%', 30]);
});

test('transpile ORDER BY property', () => {
  const q = 'MATCH (n:Person) RETURN n ORDER BY n.name';
  const result = adapter.transpile(q);
  assert.ok(result);
  assert.strictEqual(
    result.sql,
    "SELECT id, labels, properties FROM nodes WHERE labels LIKE ? ORDER BY json_extract(properties, '$.name')"
  );
  assert.deepStrictEqual(result.params, ['%"Person"%']);
});

test('transpile LIMIT nodes', () => {
  const q = 'MATCH (n:Person) RETURN n LIMIT 1';
  const result = adapter.transpile(q);
  assert.ok(result);
  assert.strictEqual(
    result.sql,
    'SELECT id, labels, properties FROM nodes WHERE labels LIKE ? LIMIT 1'
  );
  assert.deepStrictEqual(result.params, ['%"Person"%']);
});

test('transpile returns null for relationship match', () => {
  const q = 'MATCH ()-[r:REL]->() RETURN r';
  const result = adapter.transpile(q);
  assert.strictEqual(result, null);
});

test('transpile WHERE id equality', () => {
  const q = 'MATCH (n:Person) WHERE id(n) = 1 RETURN n';
  const result = adapter.transpile(q);
  assert.ok(result);
  assert.strictEqual(
    result.sql,
    'SELECT id, labels, properties FROM nodes WHERE labels LIKE ? AND id = ?'
  );
  assert.deepStrictEqual(result.params, ['%"Person"%', 1]);
});

test('transpile WHERE id IN list', () => {
  const q = 'MATCH (n) WHERE id(n) IN [1,2] RETURN n';
  const result = adapter.transpile(q);
  assert.ok(result);
  assert.strictEqual(
    result.sql,
    'SELECT id, labels, properties FROM nodes WHERE id IN (?, ?)'
  );
  assert.deepStrictEqual(result.params, [1, 2]);
});

test('transpile WHERE id IN empty list', () => {
  const q = 'MATCH (n:Person) WHERE id(n) IN [] RETURN n';
  const result = adapter.transpile(q);
  assert.ok(result);
  assert.strictEqual(
    result.sql,
    'SELECT id, labels, properties FROM nodes WHERE labels LIKE ? AND 0'
  );
  assert.deepStrictEqual(result.params, ['%"Person"%']);
});

test('transpile WHERE id equality parameter', () => {
  const q = 'MATCH (n) WHERE id(n) = $id RETURN n';
  const result = adapter.transpile(q, { id: 3 });
  assert.ok(result);
  assert.strictEqual(
    result.sql,
    'SELECT id, labels, properties FROM nodes WHERE id = ?'
  );
  assert.deepStrictEqual(result.params, [3]);
});

test('transpile return property with ORDER BY', () => {
  const q = 'MATCH (n:Person) RETURN n.name AS name ORDER BY name DESC';
  const result = adapter.transpile(q);
  assert.ok(result);
  assert.strictEqual(
    result.sql,
    "SELECT json_extract(properties, '$.name') AS value FROM nodes WHERE labels LIKE ? ORDER BY json_extract(properties, '$.name') DESC"
  );
  assert.deepStrictEqual(result.params, ['%"Person"%']);
});

test('transpile ORDER BY with SKIP and LIMIT', () => {
  const q = 'MATCH (n:Person) RETURN n.name AS name ORDER BY name SKIP 1 LIMIT 2';
  const result = adapter.transpile(q);
  assert.ok(result);
  assert.strictEqual(
    result.sql,
    "SELECT json_extract(properties, '$.name') AS value FROM nodes WHERE labels LIKE ? ORDER BY json_extract(properties, '$.name') LIMIT 2 OFFSET 1"
  );
  assert.deepStrictEqual(result.params, ['%"Person"%']);
});

test('transpile RETURN node with LIMIT', () => {
  const q = 'MATCH (n:Person) RETURN n LIMIT 1';
  const result = adapter.transpile(q);
  assert.ok(result);
  assert.strictEqual(
    result.sql,
    'SELECT id, labels, properties FROM nodes WHERE labels LIKE ? LIMIT 1'
  );
  assert.deepStrictEqual(result.params, ['%"Person"%']);
});
