const test = require('node:test');
const assert = require('node:assert');
const { parse, parseMany } = require('../packages/core/dist/parser/CypherParser');

// Simple match
test('parse MATCH (n) RETURN n', () => {
  const ast = parse('MATCH (n) RETURN n');
  assert.strictEqual(ast.type, 'MatchReturn');
  assert.strictEqual(ast.variable, 'n');
  assert.deepStrictEqual(ast.labels, []);
  assert.deepStrictEqual(ast.returnItems, [
    { expression: { type: 'Variable', name: 'n' }, alias: undefined }
  ]);
});

// Match with label
test('parse MATCH (n:Person) RETURN n', () => {
  const ast = parse('MATCH (n:Person) RETURN n');
  assert.deepStrictEqual(ast.labels, ['Person']);
  assert.deepStrictEqual(ast.returnItems, [
    { expression: { type: 'Variable', name: 'n' }, alias: undefined }
  ]);
});

// Create node
test('parse CREATE (n:Person {name:"Alice"}) RETURN n', () => {
  const ast = parse('CREATE (n:Person {name:"Alice"}) RETURN n');
  assert.strictEqual(ast.type, 'Create');
  assert.deepStrictEqual(ast.labels, ['Person']);
  assert.strictEqual(ast.properties.name, 'Alice');
});

// Merge node without return
test('parse MERGE (n {name:"Bob"})', () => {
  const ast = parse('MERGE (n {name:"Bob"})');
  assert.strictEqual(ast.type, 'Merge');
  assert.strictEqual(ast.properties.name, 'Bob');
});

test('parse MERGE (a)-[r:REL]->(b) RETURN r', () => {
  const ast = parse('MERGE (a)-[r:REL]->(b) RETURN r');
  assert.strictEqual(ast.type, 'MergeRel');
  assert.strictEqual(ast.relType, 'REL');
  assert.strictEqual(ast.startVariable, 'a');
  assert.strictEqual(ast.endVariable, 'b');
});

test('parseMany splits semicolon separated statements', () => {
  const [q1, q2] = parseMany('CREATE (n) RETURN n; MATCH (n) RETURN n');
  assert.strictEqual(q1.type, 'Create');
  assert.strictEqual(q2.type, 'MatchReturn');
});

// Invalid query should throw
test('parse unsupported query throws', () => {
  assert.throws(() => parse('RETURN n'), /Parse error/);
});
