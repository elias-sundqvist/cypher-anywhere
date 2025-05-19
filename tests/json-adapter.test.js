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
