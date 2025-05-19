import {
  StorageAdapter,
  NodeRecord,
  RelRecord,
  TransactionCtx,
} from './storage/StorageAdapter';
import { parse, parseMany, CypherAST } from './parser/CypherParser';
import { astToLogical } from './logical/LogicalPlan';
import { logicalToPhysical } from './physical/PhysicalPlan';

export interface CypherEngineOptions {
  adapter: StorageAdapter;
}

export class CypherEngine {
  private adapter: StorageAdapter;

  constructor(options: CypherEngineOptions) {
    this.adapter = options.adapter;
  }

  async *run(
    query: string,
    params: Record<string, any> = {}
  ): AsyncIterable<Record<string, unknown>> {
    const statements = parseMany(query) as CypherAST[];
    const vars = new Map<string, any>();
    for (const ast of statements) {
      let tx: TransactionCtx | undefined;
      const isWrite =
        ast.type === 'Create' ||
        ast.type === 'Merge' ||
        ast.type === 'MatchDelete' ||
        ast.type === 'MatchSet' ||
        ast.type === 'CreateRel' ||
        ast.type === 'MergeRel' ||
        ast.type === 'Foreach';
      if (isWrite && this.adapter.beginTransaction) {
        tx = await this.adapter.beginTransaction();
      }
      try {
        const logical = astToLogical(ast);
        const physical = logicalToPhysical(logical, this.adapter);
        for await (const row of physical(vars, params)) {
          yield row;
        }
        if (tx && this.adapter.commit) {
          await this.adapter.commit(tx);
        }
      } catch (err) {
        if (tx && this.adapter.rollback) {
          await this.adapter.rollback(tx);
        }
        throw err;
      }
    }
  }
}

