const test = require('node:test');
const assert = require('node:assert');
const { JsonAdapter } = require('../packages/adapters/json-adapter/dist');
const { CypherEngine } = require('../packages/core/dist');
const path = require('path');

const datasetPath = path.join(__dirname, 'data', 'sample.json');

test('JsonAdapter + CypherEngine returns all nodes for MATCH (n) RETURN n', async (t) => {
  const adapter = new JsonAdapter({ datasetPath });
  const engine = new CypherEngine({ adapter });
  const results = [];
  for await (const row of engine.run('MATCH (n) RETURN n')) {
    results.push(row.n);
  }
  assert.strictEqual(results.length, 2);
  assert.ok(results[0].labels.includes('Person'));
});

test('CypherEngine filters nodes by label', async (t) => {
  const adapter = new JsonAdapter({ datasetPath });
  const engine = new CypherEngine({ adapter });

  const people = [];
  for await (const row of engine.run('MATCH (p:Person) RETURN p')) {
    people.push(row.p);
  }
  assert.strictEqual(people.length, 1);
  assert.strictEqual(people[0].properties.name, 'Keanu Reeves');

  const movies = [];
  for await (const row of engine.run('MATCH (m:Movie) RETURN m')) {
    movies.push(row.m);
  }
  assert.strictEqual(movies.length, 1);
  assert.strictEqual(movies[0].properties.title, 'The Matrix');
});
