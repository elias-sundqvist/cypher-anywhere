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

    // Very small pattern matcher for either MATCH (n) RETURN n
    // or MATCH (n:Label) RETURN n
    const match = trimmed.match(/^MATCH\s+\((\w+)(?::(\w+))?\)\s+RETURN\s+\1$/);

    if (match) {
      const [, variable, label] = match;
      const scanSpec = label ? { label } : undefined;
      for await (const node of this.adapter.scanNodes(scanSpec)) {
        yield { [variable]: node };
      }
      return;
    }

    throw new Error('Query not supported in this MVP');
  }
}
