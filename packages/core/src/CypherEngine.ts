import {
  StorageAdapter,
  NodeRecord,
  RelRecord,
  TransactionCtx,
} from './storage/StorageAdapter';
import { parseMany, CypherAST } from './parser/CypherParser';
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

  run(
    query: string,
    params: Record<string, any> = {}
  ): AsyncIterable<Record<string, unknown>> & { meta: { transpiled: boolean } } {
    const adapter: any = this.adapter;
    if (adapter.runTranspiled) {
      const iter = adapter.runTranspiled(query, params);
      if (iter) {
        (iter as any).meta = { transpiled: true };
        return iter as any;
      }
    }

    const self = this;
    async function* gen() {
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
        if (isWrite && self.adapter.beginTransaction) {
          tx = await self.adapter.beginTransaction();
        }
        try {
          const logical = astToLogical(ast);
          const physical = logicalToPhysical(logical, self.adapter);
          for await (const row of physical(vars, params)) {
            yield row;
          }
          if (tx && self.adapter.commit) {
            await self.adapter.commit(tx);
          }
        } catch (err) {
          if (tx && self.adapter.rollback) {
            await self.adapter.rollback(tx);
          }
          throw err;
        }
      }
    }
    const iter = gen();
    (iter as any).meta = { transpiled: false };
    return iter as any;
  }
}

