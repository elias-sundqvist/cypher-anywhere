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

test('transpile optional match simple return', () => {
  const result = adapter.transpile('OPTIONAL MATCH (n:Person {name:"Alice"}) RETURN n');
  assert.ok(result);
  assert.strictEqual(
    result.sql,
    'SELECT id, labels, properties FROM nodes WHERE labels LIKE ? AND json_extract(properties, \'$.name\') = ?'
  );
  assert.deepStrictEqual(result.params, ['%"Person"%', 'Alice']);
});

test('transpile optional match property return', () => {
  const q = 'OPTIONAL MATCH (n:Person {name:"Alice"}) RETURN n.name AS name';
  const result = adapter.transpile(q);
  assert.ok(result);
  assert.strictEqual(
    result.sql,
    'SELECT json_extract(properties, \'$.name\') AS name FROM nodes WHERE labels LIKE ? AND json_extract(properties, \'$.name\') = ?'
  );
  assert.deepStrictEqual(result.params, ['%"Person"%', 'Alice']);
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

test('transpile with parameterized WHERE not equals', () => {
  const q = 'MATCH (n:Person) WHERE n.name <> $name RETURN n';
  const result = adapter.transpile(q, { name: 'Alice' });
  assert.ok(result);
  assert.strictEqual(
    result.sql,
    "SELECT id, labels, properties FROM nodes WHERE labels LIKE ? AND json_extract(properties, '$.name') <> ?"
  );
  assert.deepStrictEqual(result.params, ['%"Person"%', 'Alice']);
});

test('transpile WHERE id greater than', () => {
  const q = 'MATCH (n) WHERE id(n) > 1 RETURN n';
  const result = adapter.transpile(q);
  assert.ok(result);
  assert.strictEqual(
    result.sql,
    'SELECT id, labels, properties FROM nodes WHERE id > ?'
  );
  assert.deepStrictEqual(result.params, [1]);
});

test('transpile WHERE id greater or equal parameter', () => {
  const q = 'MATCH (n:Person) WHERE id(n) >= $id RETURN n';
  const result = adapter.transpile(q, { id: 2 });
  assert.ok(result);
  assert.strictEqual(
    result.sql,
    'SELECT id, labels, properties FROM nodes WHERE labels LIKE ? AND id >= ?'
  );
  assert.deepStrictEqual(result.params, ['%"Person"%', 2]);
});

test('transpile WHERE boolean property', () => {
  const q = 'MATCH (n:Flagged) WHERE n.active = true RETURN n';
  const result = adapter.transpile(q);
  assert.ok(result);
  assert.strictEqual(
    result.sql,
    "SELECT id, labels, properties FROM nodes WHERE labels LIKE ? AND json_extract(properties, '$.active') = ?"
  );
  assert.deepStrictEqual(result.params, ['%"Flagged"%', true]);
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
    "SELECT json_extract(properties, '$.name') AS name FROM nodes WHERE labels LIKE ? ORDER BY json_extract(properties, '$.name') DESC"
  );
  assert.deepStrictEqual(result.params, ['%"Person"%']);
});

test('transpile ORDER BY with SKIP and LIMIT', () => {
  const q = 'MATCH (n:Person) RETURN n.name AS name ORDER BY name SKIP 1 LIMIT 2';
  const result = adapter.transpile(q);
  assert.ok(result);
  assert.strictEqual(
    result.sql,
    "SELECT json_extract(properties, '$.name') AS name FROM nodes WHERE labels LIKE ? ORDER BY json_extract(properties, '$.name') LIMIT 2 OFFSET 1"
  );
  assert.deepStrictEqual(result.params, ['%"Person"%']);
});

test('transpile ORDER BY id', () => {
  const q = 'MATCH (n:Person) RETURN n ORDER BY id(n) DESC';
  const result = adapter.transpile(q);
  assert.ok(result);
  assert.strictEqual(
    result.sql,
    'SELECT id, labels, properties FROM nodes WHERE labels LIKE ? ORDER BY id DESC'
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

test('transpile SKIP only', () => {
  const q = 'MATCH (n:Person) RETURN n SKIP 1';
  const result = adapter.transpile(q);
  assert.ok(result);
  assert.strictEqual(
    result.sql,
    'SELECT id, labels, properties FROM nodes WHERE labels LIKE ? LIMIT -1 OFFSET 1'
  );
  assert.deepStrictEqual(result.params, ['%"Person"%']);
});

test('transpile parameterized SKIP', () => {
  const q = 'MATCH (n:Person) RETURN n SKIP $s';
  const result = adapter.transpile(q, { s: 2 });
  assert.ok(result);
  assert.strictEqual(
    result.sql,
    'SELECT id, labels, properties FROM nodes WHERE labels LIKE ? LIMIT -1 OFFSET 2'
  );
  assert.deepStrictEqual(result.params, ['%"Person"%']);
});

test('transpile parameterized LIMIT', () => {
  const q = 'MATCH (n:Person) RETURN n LIMIT $lim';
  const result = adapter.transpile(q, { lim: 2 });
  assert.ok(result);
  assert.strictEqual(
    result.sql,
    'SELECT id, labels, properties FROM nodes WHERE labels LIKE ? LIMIT 2'
  );
  assert.deepStrictEqual(result.params, ['%"Person"%']);
});

test('transpile WHERE id IN parameter list', () => {
  const q = 'MATCH (n) WHERE id(n) IN $ids RETURN n';
  const result = adapter.transpile(q, { ids: [1, 2] });
  assert.ok(result);
  assert.strictEqual(
    result.sql,
    'SELECT id, labels, properties FROM nodes WHERE id IN (?, ?)'
  );
  assert.deepStrictEqual(result.params, [1, 2]);
});

test('transpile COUNT without variable', () => {
  const q = 'MATCH (:Person) RETURN COUNT(*) AS cnt';
  const result = adapter.transpile(q);
  assert.ok(result);
  assert.strictEqual(
    result.sql,
    'SELECT COUNT(*) AS value FROM nodes WHERE labels LIKE ?'
  );
  assert.deepStrictEqual(result.params, ['%"Person"%']);
});

test('transpile COUNT DISTINCT property', () => {
  const q = 'MATCH (p:Person) RETURN COUNT(DISTINCT p.name) AS cnt';
  const result = adapter.transpile(q);
  assert.ok(result);
  assert.strictEqual(
    result.sql,
    "SELECT COUNT(DISTINCT json_extract(properties, '$.name')) AS value FROM nodes WHERE labels LIKE ?"
  );
  assert.deepStrictEqual(result.params, ['%\"Person\"%']);
});

test('transpile COUNT DISTINCT star', () => {
  const q = 'MATCH (p:Person) RETURN COUNT(DISTINCT *) AS cnt';
  const result = adapter.transpile(q);
  assert.ok(result);
  assert.strictEqual(
    result.sql,
    'SELECT COUNT(DISTINCT id) AS value FROM nodes WHERE labels LIKE ?'
  );
  assert.deepStrictEqual(result.params, ['%"Person"%']);
});

test('transpile multiple return properties', () => {
  const q = 'MATCH (m:Movie) RETURN m.title AS title, m.released AS year ORDER BY year';
  const result = adapter.transpile(q);
  assert.ok(result);
  assert.strictEqual(
    result.sql,
    "SELECT json_extract(properties, '$.title') AS title, json_extract(properties, '$.released') AS year FROM nodes WHERE labels LIKE ? ORDER BY json_extract(properties, '$.released')"
  );
  assert.deepStrictEqual(result.params, ['%"Movie"%']);
});

test('transpile MIN aggregation', () => {
  const q = 'MATCH (m:Movie) RETURN MIN(m.released)';
  const result = adapter.transpile(q);
  assert.ok(result);
  assert.strictEqual(
    result.sql,
    "SELECT MIN(json_extract(properties, '$.released')) AS value FROM nodes WHERE labels LIKE ?"
  );
  assert.deepStrictEqual(result.params, ['%"Movie"%']);
});

test('transpile MAX aggregation with alias', () => {
  const q = 'MATCH (m:Movie) RETURN MAX(m.released) AS mx';
  const result = adapter.transpile(q);
  assert.ok(result);
  assert.strictEqual(
    result.sql,
    "SELECT MAX(json_extract(properties, '$.released')) AS mx FROM nodes WHERE labels LIKE ?"
  );
  assert.deepStrictEqual(result.params, ['%"Movie"%']);
});

test('transpile SUM aggregation', () => {
  const q = 'MATCH (m:Movie) RETURN SUM(m.released) AS total';
  const result = adapter.transpile(q);
  assert.ok(result);
  assert.strictEqual(
    result.sql,
    "SELECT SUM(json_extract(properties, '$.released')) AS total FROM nodes WHERE labels LIKE ?"
  );
  assert.deepStrictEqual(result.params, ['%"Movie"%']);
});

test('transpile RETURN id function', () => {
  const q = 'MATCH (n:Person {name:"Alice"}) RETURN id(n) AS id';
  const result = adapter.transpile(q);
  assert.ok(result);
  assert.strictEqual(
    result.sql,
    'SELECT id AS id FROM nodes WHERE labels LIKE ? AND json_extract(properties, \'$.name\') = ?'
  );
  assert.deepStrictEqual(result.params, ['%"Person"%', 'Alice']);
});

test('transpile RETURN labels function', () => {
  const q = 'MATCH (n:Person {name:"Alice"}) RETURN labels(n) AS labs';
  const result = adapter.transpile(q);
  assert.ok(result);
  assert.strictEqual(
    result.sql,
    'SELECT labels AS labs FROM nodes WHERE labels LIKE ? AND json_extract(properties, \'$.name\') = ?'
  );
  assert.deepStrictEqual(result.params, ['%"Person"%', 'Alice']);
});

test('transpile RETURN star single variable', () => {
  const q = 'MATCH (n:Person {name:"Alice"}) RETURN *';
  const result = adapter.transpile(q);
  assert.ok(result);
  assert.strictEqual(
    result.sql,
    'SELECT id, labels, properties FROM nodes WHERE labels LIKE ? AND json_extract(properties, \'$.name\') = ?'
  );
  assert.deepStrictEqual(result.params, ['%"Person"%', 'Alice']);
});
