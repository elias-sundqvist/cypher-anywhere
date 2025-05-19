import { StorageAdapter, NodeRecord } from './storage/StorageAdapter';

export interface CypherEngineOptions {
  adapter: StorageAdapter;
}

export class CypherEngine {
  private adapter: StorageAdapter;

  constructor(options: CypherEngineOptions) {
    this.adapter = options.adapter;
  }

  async *run(query: string): AsyncIterable<Record<string, unknown>> {
    const trimmed = query.trim();
    if (trimmed === 'MATCH (n) RETURN n') {
      for await (const node of this.adapter.scanNodes()) {
        yield { n: node };
      }
    } else {
      throw new Error('Query not supported in this MVP');
    }
  }
}
