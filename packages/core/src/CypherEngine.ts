import { StorageAdapter, NodeRecord } from './storage/StorageAdapter';
import { parse, MatchReturnQuery, CreateQuery, MergeQuery, CypherAST } from './parser/CypherParser';

export interface CypherEngineOptions {
  adapter: StorageAdapter;
}

export class CypherEngine {
  private adapter: StorageAdapter;

  constructor(options: CypherEngineOptions) {
    this.adapter = options.adapter;
  }

  async *run(query: string): AsyncIterable<Record<string, unknown>> {
    const ast = parse(query) as CypherAST;
    switch (ast.type) {
      case 'MatchReturn': {
        const scan = this.adapter.scanNodes(ast.label ? { label: ast.label } : {});
        for await (const node of scan) {
          // filter by properties if provided
          if (ast.properties) {
            let ok = true;
            for (const [k, v] of Object.entries(ast.properties)) {
              if (node.properties[k] !== v) {
                ok = false;
                break;
              }
            }
            if (!ok) continue;
          }
          yield { [ast.variable]: node };
        }
        break;
      }
      case 'Create': {
        if (!this.adapter.createNode) {
          throw new Error('Adapter does not support CREATE');
        }
        const node = await this.adapter.createNode(ast.label ? [ast.label] : [], ast.properties ?? {});
        if (ast.returnVariable) {
          yield { [ast.variable]: node };
        }
        break;
      }
      case 'Merge': {
        if (!this.adapter.findNode || !this.adapter.createNode) {
          throw new Error('Adapter does not support MERGE');
        }
        let node = await this.adapter.findNode(ast.label ? [ast.label] : [], ast.properties ?? {});
        if (!node) {
          node = await this.adapter.createNode(ast.label ? [ast.label] : [], ast.properties ?? {});
        }
        if (ast.returnVariable) {
          yield { [ast.variable]: node };
        }
        break;
      }
      default:
        throw new Error('Query not supported in this MVP');
    }

    throw new Error('Query not supported in this MVP');
  }
}
