import { JsonAdapter } from '@cypher-anywhere/json-adapter';
import { CypherEngine } from '@cypher-anywhere/core';
import * as path from 'path';

const datasetPath = path.join(__dirname, 'data', 'sample.json');

describe('JsonAdapter + CypherEngine', () => {
  it('returns all nodes for MATCH (n) RETURN n', async () => {
    const adapter = new JsonAdapter({ datasetPath });
    const engine = new CypherEngine({ adapter });
    const results: any[] = [];
    for await (const row of engine.run('MATCH (n) RETURN n')) {
      results.push(row.n);
    }
    expect(results).toHaveLength(2);
    expect(results[0].labels).toContain('Person');
  });
});
