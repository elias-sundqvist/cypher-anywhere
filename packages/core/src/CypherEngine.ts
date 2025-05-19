import {
  StorageAdapter,
  NodeRecord,
  RelRecord,
  TransactionCtx,
} from './storage/StorageAdapter';
import {
  parse,
  parseMany,
  MatchReturnQuery,
  CreateQuery,
  MergeQuery,
  MatchDeleteQuery,
  MatchSetQuery,
  CreateRelQuery,
  MergeRelQuery,
  CypherAST,
} from './parser/CypherParser';

export interface CypherEngineOptions {
  adapter: StorageAdapter;
}

export class CypherEngine {
  private adapter: StorageAdapter;

  constructor(options: CypherEngineOptions) {
    this.adapter = options.adapter;
  }

  async *run(query: string): AsyncIterable<Record<string, unknown>> {
    const statements = parseMany(query) as CypherAST[];
    const vars = new Map<string, NodeRecord | RelRecord>();
    for (const ast of statements) {
      let tx: TransactionCtx | undefined;
      const isWrite =
        ast.type === 'Create' ||
        ast.type === 'Merge' ||
        ast.type === 'MatchDelete' ||
        ast.type === 'MatchSet' ||
        ast.type === 'CreateRel' ||
        ast.type === 'MergeRel';
      if (isWrite && this.adapter.beginTransaction) {
        tx = await this.adapter.beginTransaction();
      }
      try {
        switch (ast.type) {
      case 'MatchReturn': {
        const scan = this.adapter.scanNodes(ast.label ? { label: ast.label } : {});
        for await (const node of scan) {
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
          vars.set(ast.variable, node);
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
          vars.set(ast.variable, node);
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
          vars.set(ast.variable, node);
          yield { [ast.variable]: node };
        }
        break;
      }
      case 'MatchDelete': {
        if (ast.isRelationship) {
          if (!this.adapter.scanRelationships || !this.adapter.deleteRelationship) {
            throw new Error('Adapter does not support relationship delete');
          }
          for await (const rel of this.adapter.scanRelationships()) {
            if (ast.label && rel.type !== ast.label) continue;
            if (ast.properties) {
              let ok = true;
              for (const [k, v] of Object.entries(ast.properties)) {
                if (rel.properties[k] !== v) {
                  ok = false;
                  break;
                }
              }
              if (!ok) continue;
            }
            await this.adapter.deleteRelationship(rel.id);
            break;
          }
        } else {
          if (!this.adapter.scanNodes || !this.adapter.deleteNode) {
            throw new Error('Adapter does not support node delete');
          }
          for await (const node of this.adapter.scanNodes(ast.label ? { label: ast.label } : {})) {
            let ok = true;
            if (ast.properties) {
              for (const [k, v] of Object.entries(ast.properties)) {
                if (node.properties[k] !== v) {
                  ok = false;
                  break;
                }
              }
            }
            if (!ok) continue;
            await this.adapter.deleteNode(node.id);
            break;
          }
        }
        break;
      }
      case 'MatchSet': {
        if (ast.isRelationship) {
          if (!this.adapter.scanRelationships || !this.adapter.updateRelationshipProperties) {
            throw new Error('Adapter does not support relationship update');
          }
          for await (const rel of this.adapter.scanRelationships()) {
            if (ast.label && rel.type !== ast.label) continue;
            if (ast.properties) {
              let ok = true;
              for (const [k, v] of Object.entries(ast.properties)) {
                if (rel.properties[k] !== v) {
                  ok = false;
                  break;
                }
              }
              if (!ok) continue;
            }
            await this.adapter.updateRelationshipProperties(rel.id, { [ast.property]: ast.value });
            if (ast.returnVariable) {
              rel.properties[ast.property] = ast.value;
              vars.set(ast.variable, rel);
              yield { [ast.variable]: rel };
            }
            break;
          }
        } else {
          if (!this.adapter.scanNodes || !this.adapter.updateNodeProperties) {
            throw new Error('Adapter does not support node update');
          }
          for await (const node of this.adapter.scanNodes(ast.label ? { label: ast.label } : {})) {
            let ok = true;
            if (ast.properties) {
              for (const [k, v] of Object.entries(ast.properties)) {
                if (node.properties[k] !== v) {
                  ok = false;
                  break;
                }
              }
            }
            if (!ok) continue;
            await this.adapter.updateNodeProperties(node.id, { [ast.property]: ast.value });
            if (ast.returnVariable) {
              node.properties[ast.property] = ast.value;
              vars.set(ast.variable, node);
              yield { [ast.variable]: node };
            }
            break;
          }
        }
        break;
      }
      case 'CreateRel': {
        if (!this.adapter.createNode || !this.adapter.createRelationship) {
          throw new Error('Adapter does not support CREATE');
        }
        const start = await this.adapter.createNode(ast.start.label ? [ast.start.label] : [], ast.start.properties ?? {});
        const end = await this.adapter.createNode(ast.end.label ? [ast.end.label] : [], ast.end.properties ?? {});
        const rel = await this.adapter.createRelationship(ast.relType, start.id, end.id, ast.relProperties ?? {});
        if (ast.returnVariable) {
          vars.set(ast.relVariable, rel);
          yield { [ast.relVariable]: rel };
        }
        break;
      }
      case 'MergeRel': {
        if (!this.adapter.scanRelationships || !this.adapter.createRelationship) {
          throw new Error('Adapter does not support MERGE');
        }
        const startNode = vars.get(ast.startVariable) as NodeRecord | undefined;
        const endNode = vars.get(ast.endVariable) as NodeRecord | undefined;
        if (!startNode || !endNode) {
          throw new Error('MergeRel requires bound start and end variables');
        }
        let existing: RelRecord | null = null;
        for await (const rel of this.adapter.scanRelationships()) {
          if (
            rel.type === ast.relType &&
            rel.startNode === startNode.id &&
            rel.endNode === endNode.id
          ) {
            existing = rel;
            break;
          }
        }
        if (!existing) {
          existing = await this.adapter.createRelationship(
            ast.relType,
            startNode.id,
            endNode.id,
            ast.relProperties ?? {}
          );
        }
        if (ast.returnVariable) {
          vars.set(ast.relVariable, existing);
          yield { [ast.relVariable]: existing };
        }
        break;
      }
      default:
        throw new Error('Query not supported in this MVP');
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
