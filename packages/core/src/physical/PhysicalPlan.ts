import { LogicalPlan } from '../logical/LogicalPlan';
import {
  StorageAdapter,
  NodeRecord,
  RelRecord,
} from '../storage/StorageAdapter';
import { Expression, WhereClause } from '../parser/CypherParser';

function evalExpr(expr: Expression, vars: Map<string, any>): any {
  switch (expr.type) {
    case 'Literal':
      return expr.value;
    case 'Property': {
      const rec = vars.get(expr.variable) as NodeRecord | RelRecord | undefined;
      if (!rec) throw new Error(`Unbound variable ${expr.variable}`);
      return rec.properties[expr.property];
    }
    case 'Variable':
      return vars.get(expr.name);
    case 'Add':
      return String(evalExpr(expr.left, vars)) + String(evalExpr(expr.right, vars));
    default:
      throw new Error('Unknown expression');
  }
}

function evalWhere(where: WhereClause, vars: Map<string, any>): boolean {
  const l = evalExpr(where.left, vars);
  const r = evalExpr(where.right, vars);
  switch (where.operator) {
    case '=':
      return l === r;
    case '>':
      return (l as any) > (r as any);
    case '>=':
      return (l as any) >= (r as any);
    default:
      throw new Error('Unknown operator');
  }
}

export type PhysicalPlan = (
  vars: Map<string, any>
) => AsyncIterable<Record<string, unknown>>;

export function logicalToPhysical(
  plan: LogicalPlan,
  adapter: StorageAdapter
): PhysicalPlan {
  return async function* (vars: Map<string, any>) {
    switch (plan.type) {
      case 'MatchReturn': {
        let usedIndex = false;
        if (
          plan.labels && plan.labels.length > 0 &&
          plan.properties &&
          Object.keys(plan.properties).length === 1 &&
          adapter.indexLookup &&
          adapter.listIndexes
        ) {
          const [prop, value] = Object.entries(plan.properties)[0];
          const indexes = await adapter.listIndexes();
          const label = plan.labels[0];
          const found = indexes.find(
            i => i.label === label && i.properties.length === 1 && i.properties[0] === prop
          );
          if (found) {
            for await (const node of adapter.indexLookup(label, prop, value)) {
              vars.set(plan.variable, node);
              if (plan.where && !evalWhere(plan.where, vars)) continue;
              yield { [plan.variable]: node };
            }
            usedIndex = true;
          }
        }
        if (!usedIndex) {
          const scan = adapter.scanNodes(plan.labels ? { labels: plan.labels } : {});
          for await (const node of scan) {
            if (plan.properties) {
              let ok = true;
              for (const [k, v] of Object.entries(plan.properties)) {
                if (node.properties[k] !== v) {
                  ok = false;
                  break;
                }
              }
              if (!ok) continue;
            }
            vars.set(plan.variable, node);
            if (plan.where && !evalWhere(plan.where, vars)) continue;
            yield { [plan.variable]: node };
          }
        }
        break;
      }
      case 'Create': {
        if (!adapter.createNode) throw new Error('Adapter does not support CREATE');
        const node = await adapter.createNode(plan.labels ?? [], plan.properties ?? {});
        if (plan.returnVariable) {
          vars.set(plan.variable, node);
          yield { [plan.variable]: node };
        }
        break;
      }
      case 'Merge': {
        if (!adapter.findNode || !adapter.createNode)
          throw new Error('Adapter does not support MERGE');
        let node = await adapter.findNode(plan.labels ?? [], plan.properties ?? {});
        if (!node) {
          node = await adapter.createNode(plan.labels ?? [], plan.properties ?? {});
        }
        if (plan.returnVariable) {
          vars.set(plan.variable, node);
          yield { [plan.variable]: node };
        }
        break;
      }
      case 'MatchDelete': {
        if (plan.isRelationship) {
          if (!adapter.scanRelationships || !adapter.deleteRelationship)
            throw new Error('Adapter does not support relationship delete');
          for await (const rel of adapter.scanRelationships()) {
            const label = plan.labels && plan.labels.length > 0 ? plan.labels[0] : undefined;
            if (label && rel.type !== label) continue;
            if (plan.properties) {
              let ok = true;
              for (const [k, v] of Object.entries(plan.properties)) {
                if (rel.properties[k] !== v) {
                  ok = false;
                  break;
                }
              }
              if (!ok) continue;
            }
            vars.set(plan.variable, rel);
            if (plan.where && !evalWhere(plan.where, vars)) continue;
            await adapter.deleteRelationship(rel.id);
            break;
          }
        } else {
          if (!adapter.scanNodes || !adapter.deleteNode)
            throw new Error('Adapter does not support node delete');
          for await (const node of adapter.scanNodes(plan.labels ? { labels: plan.labels } : {})) {
            let ok = true;
            if (plan.properties) {
              for (const [k, v] of Object.entries(plan.properties)) {
                if (node.properties[k] !== v) {
                  ok = false;
                  break;
                }
              }
            }
            if (!ok) continue;
            vars.set(plan.variable, node);
            if (plan.where && !evalWhere(plan.where, vars)) continue;
            await adapter.deleteNode(node.id);
            break;
          }
        }
        break;
      }
      case 'MatchSet': {
        if (plan.isRelationship) {
          if (!adapter.scanRelationships || !adapter.updateRelationshipProperties)
            throw new Error('Adapter does not support relationship update');
          for await (const rel of adapter.scanRelationships()) {
            const label = plan.labels && plan.labels.length > 0 ? plan.labels[0] : undefined;
            if (label && rel.type !== label) continue;
            if (plan.properties) {
              let ok = true;
              for (const [k, v] of Object.entries(plan.properties)) {
                if (rel.properties[k] !== v) {
                  ok = false;
                  break;
                }
              }
              if (!ok) continue;
            }
            vars.set(plan.variable, rel);
            if (plan.where && !evalWhere(plan.where, vars)) continue;
            const val = evalExpr(plan.value, vars);
            await adapter.updateRelationshipProperties(rel.id, { [plan.property]: val });
            rel.properties[plan.property] = val;
            if (plan.returnVariable) {
              yield { [plan.variable]: rel };
            }
          }
        } else {
          if (!adapter.scanNodes || !adapter.updateNodeProperties)
            throw new Error('Adapter does not support node update');
          for await (const node of adapter.scanNodes(plan.labels ? { labels: plan.labels } : {})) {
            let ok = true;
            if (plan.properties) {
              for (const [k, v] of Object.entries(plan.properties)) {
                if (node.properties[k] !== v) {
                  ok = false;
                  break;
                }
              }
            }
            if (!ok) continue;
            vars.set(plan.variable, node);
            if (plan.where && !evalWhere(plan.where, vars)) continue;
            const val = evalExpr(plan.value, vars);
            await adapter.updateNodeProperties(node.id, { [plan.property]: val });
            node.properties[plan.property] = val;
            if (plan.returnVariable) {
              yield { [plan.variable]: node };
            }
          }
        }
        break;
      }
      case 'CreateRel': {
        if (!adapter.createNode || !adapter.createRelationship)
          throw new Error('Adapter does not support CREATE');
        const start = await adapter.createNode(plan.start.labels ?? [], plan.start.properties ?? {});
        const end = await adapter.createNode(plan.end.labels ?? [], plan.end.properties ?? {});
        const rel = await adapter.createRelationship(plan.relType, start.id, end.id, plan.relProperties ?? {});
        if (plan.returnVariable) {
          vars.set(plan.relVariable, rel);
          yield { [plan.relVariable]: rel };
        }
        break;
      }
      case 'MergeRel': {
        if (!adapter.scanRelationships || !adapter.createRelationship)
          throw new Error('Adapter does not support MERGE');
        const startNode = vars.get(plan.startVariable) as NodeRecord | undefined;
        const endNode = vars.get(plan.endVariable) as NodeRecord | undefined;
        if (!startNode || !endNode)
          throw new Error('MergeRel requires bound start and end variables');
        let existing: RelRecord | null = null;
        for await (const rel of adapter.scanRelationships()) {
          if (rel.type === plan.relType && rel.startNode === startNode.id && rel.endNode === endNode.id) {
            existing = rel;
            break;
          }
        }
        if (!existing) {
          existing = await adapter.createRelationship(plan.relType, startNode.id, endNode.id, plan.relProperties ?? {});
        }
        if (plan.returnVariable) {
          vars.set(plan.relVariable, existing);
          yield { [plan.relVariable]: existing };
        }
        break;
      }
      case 'Foreach': {
        const innerPlan = logicalToPhysical(plan.statement, adapter);
        for (const item of plan.list) {
          vars.set(plan.variable, item);
          for await (const row of innerPlan(vars)) {
            yield row;
          }
        }
        break;
      }
      default:
        throw new Error('Query not supported in this MVP');
    }
  };
}
