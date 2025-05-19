const test = require('node:test');
const assert = require('node:assert');
const { parse } = require('../packages/core/dist/parser/CypherParser');

// Simple match
test('parse MATCH (n) RETURN n', () => {
  const ast = parse('MATCH (n) RETURN n');
  assert.strictEqual(ast.type, 'MatchReturn');
  assert.strictEqual(ast.variable, 'n');
  assert.strictEqual(ast.label, undefined);
});

// Match with label
test('parse MATCH (n:Person) RETURN n', () => {
  const ast = parse('MATCH (n:Person) RETURN n');
  assert.strictEqual(ast.label, 'Person');
});

// Create node
test('parse CREATE (n:Person {name:"Alice"}) RETURN n', () => {
  const ast = parse('CREATE (n:Person {name:"Alice"}) RETURN n');
  assert.strictEqual(ast.type, 'Create');
  assert.strictEqual(ast.label, 'Person');
  assert.strictEqual(ast.properties.name, 'Alice');
});

// Merge node without return
test('parse MERGE (n {name:"Bob"})', () => {
  const ast = parse('MERGE (n {name:"Bob"})');
  assert.strictEqual(ast.type, 'Merge');
  assert.strictEqual(ast.properties.name, 'Bob');
});

// Invalid query should throw
test('parse unsupported query throws', () => {
  assert.throws(() => parse('RETURN n'), /Parse error/);
});
