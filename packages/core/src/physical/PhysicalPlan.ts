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
      const l = evalExpr(expr.left, vars);
      const r = evalExpr(expr.right, vars);
      if (typeof l === 'number' && typeof r === 'number') {
        return l + r;
      }
      return String(l) + String(r);
    case 'Nodes':
      return vars.get(expr.variable);
    default:
      throw new Error('Unknown expression');
  }
}

async function findPath(
  adapter: StorageAdapter,
  startId: number | string,
  endId: number | string
): Promise<NodeRecord[] | null> {
  if (!adapter.scanRelationships || !adapter.getNodeById) {
    throw new Error('Adapter does not support path finding');
  }
  const rels: RelRecord[] = [];
  for await (const r of adapter.scanRelationships()) rels.push(r);
  const queue: (Array<number | string>)[] = [[startId]];
  const visited = new Set<number | string>([startId]);
  while (queue.length > 0) {
    const path = queue.shift()!;
    const last = path[path.length - 1];
    if (last === endId) {
      const nodes: NodeRecord[] = [];
      for (const id of path) {
        const n = await adapter.getNodeById(id);
        if (!n) return null;
        nodes.push(n);
      }
      return nodes;
    }
    for (const rel of rels) {
      if (rel.startNode === last && !visited.has(rel.endNode)) {
        visited.add(rel.endNode);
        queue.push([...path, rel.endNode]);
      }
    }
  }
  return null;
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
        const rows: { row: Record<string, unknown>; order?: any; node: NodeRecord }[] = [];
        const collect = async (node: NodeRecord) => {
          vars.set(plan.variable, node);
          if (plan.where && !evalWhere(plan.where, vars)) return;
          const row: Record<string, unknown> = {};
          plan.returnItems.forEach((item, idx) => {
            const val = evalExpr(item.expression, vars);
            let key: string;
            if (item.alias) {
              key = item.alias;
            } else if (item.expression.type === 'Variable') {
              key = item.expression.name;
            } else if (plan.returnItems.length === 1) {
              key = 'value';
            } else {
              key = `value${idx}`;
            }
            row[key] = val;
          });
          const order = plan.orderBy ? evalExpr(plan.orderBy, vars) : undefined;
          rows.push({ row, order, node });
        };

        let usedIndex = false;
        if (
          plan.labels &&
          plan.labels.length > 0 &&
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
              await collect(node);
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
            await collect(node);
          }
        }

        if (plan.orderBy) {
          rows.sort((a, b) => {
            if (a.order === b.order) return 0;
            if (a.order === undefined) return 1;
            if (b.order === undefined) return -1;
            return a.order > b.order ? 1 : -1;
          });
        }

        const start = plan.skip ?? 0;
        let end = rows.length;
        if (plan.limit !== undefined) end = Math.min(end, start + plan.limit);
        for (let i = start; i < end; i++) {
          vars.set(plan.variable, rows[i].node);
          yield rows[i].row;
        }
        break;
      }
      case 'Create': {
        if (!adapter.createNode) throw new Error('Adapter does not support CREATE');
        const node = await adapter.createNode(plan.labels ?? [], plan.properties ?? {});
        vars.set(plan.variable, node);
        if (plan.setProperties && adapter.updateNodeProperties) {
          for (const [k, expr] of Object.entries(plan.setProperties)) {
            const val = evalExpr(expr, vars);
            await adapter.updateNodeProperties(node.id, { [k]: val });
            node.properties[k] = val;
          }
        }
        if (plan.returnVariable) {
          yield { [plan.variable]: node };
        }
        break;
      }
      case 'Merge': {
        if (!adapter.findNode || !adapter.createNode)
          throw new Error('Adapter does not support MERGE');
        let node = await adapter.findNode(plan.labels ?? [], plan.properties ?? {});
        let created = false;
        if (!node) {
          node = await adapter.createNode(plan.labels ?? [], plan.properties ?? {});
          created = true;
        }
        vars.set(plan.variable, node);
        if (created && plan.onCreateSet && adapter.updateNodeProperties) {
          for (const [k, expr] of Object.entries(plan.onCreateSet)) {
            const val = evalExpr(expr, vars);
            await adapter.updateNodeProperties(node.id, { [k]: val });
            node.properties[k] = val;
          }
        }
        if (plan.returnVariable) {
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
          const bound = vars.get(plan.variable) as NodeRecord | undefined;
          if (bound && (!plan.labels || plan.labels.length === 0) && !plan.properties) {
            const node = bound;
            if (!plan.where || evalWhere(plan.where, vars)) {
              const val = evalExpr(plan.value, vars);
              await adapter.updateNodeProperties(node.id, { [plan.property]: val });
              node.properties[plan.property] = val;
              if (plan.returnVariable) {
                yield { [plan.variable]: node };
              }
            }
          } else {
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
        let created = false;
        if (!existing) {
          existing = await adapter.createRelationship(plan.relType, startNode.id, endNode.id, plan.relProperties ?? {});
          created = true;
        }
        vars.set(plan.relVariable, existing);
        if (created && plan.onCreateSet && adapter.updateRelationshipProperties) {
          for (const [k, expr] of Object.entries(plan.onCreateSet)) {
            const val = evalExpr(expr, vars);
            await adapter.updateRelationshipProperties(existing.id, { [k]: val });
            existing.properties[k] = val;
          }
        }
        if (plan.returnVariable) {
          yield { [plan.relVariable]: existing };
        }
        break;
      }
      case 'MatchPath': {
        if (!adapter.scanNodes)
          throw new Error('Adapter does not support MATCH');
        const starts: NodeRecord[] = [];
        for await (const node of adapter.scanNodes(plan.start.labels ? { labels: plan.start.labels } : {})) {
          let ok = true;
          if (plan.start.properties) {
            for (const [k, v] of Object.entries(plan.start.properties)) {
              if (node.properties[k] !== v) {
                ok = false;
                break;
              }
            }
          }
          if (ok) starts.push(node);
        }
        const ends: NodeRecord[] = [];
        for await (const node of adapter.scanNodes(plan.end.labels ? { labels: plan.end.labels } : {})) {
          let ok = true;
          if (plan.end.properties) {
            for (const [k, v] of Object.entries(plan.end.properties)) {
              if (node.properties[k] !== v) {
                ok = false;
                break;
              }
            }
          }
          if (ok) ends.push(node);
        }
        for (const s of starts) {
          for (const e of ends) {
            const path = await findPath(adapter, s.id, e.id);
            if (path) {
              vars.set(plan.pathVariable, path);
              if (plan.returnVariable) {
                yield { [plan.pathVariable]: path };
              }
              return;
            }
          }
        }
        break;
      }
      case 'MatchChain': {
        if (!adapter.scanNodes || !adapter.scanRelationships || !adapter.getNodeById)
          throw new Error('Adapter does not support MATCH');
        const scanRels = adapter.scanRelationships!.bind(adapter);
        const getNode = adapter.getNodeById!.bind(adapter);
        const startNodes: NodeRecord[] = [];
        for await (const node of adapter.scanNodes(
          plan.start.labels ? { labels: plan.start.labels } : {}
        )) {
          let ok = true;
          if (plan.start.properties) {
            for (const [k, v] of Object.entries(plan.start.properties)) {
              if (node.properties[k] !== v) {
                ok = false;
                break;
              }
            }
          }
          if (ok) startNodes.push(node);
        }
        const traverse = async function* (
          node: NodeRecord,
          hop: number,
          varsLocal: Map<string, any>
        ): AsyncIterable<Record<string, unknown>> {
          if (hop >= plan.hops.length) {
            const ret = varsLocal.get(plan.returnVariable);
            if (ret) yield { [plan.returnVariable]: ret };
            return;
          }
          const step = plan.hops[hop];
          for await (const rel of scanRels()) {
            if (step.rel.type && rel.type !== step.rel.type) continue;
            if (step.rel.direction === 'out') {
              if (rel.startNode !== node.id) continue;
            } else {
              if (rel.endNode !== node.id) continue;
            }
            if (step.rel.properties) {
              let ok = true;
              for (const [k, v] of Object.entries(step.rel.properties)) {
                if (rel.properties[k] !== v) {
                  ok = false;
                  break;
                }
              }
              if (!ok) continue;
            }
            const nextId =
              step.rel.direction === 'out' ? rel.endNode : rel.startNode;
            const nextNode = await getNode(nextId);
            if (!nextNode) continue;
            if (step.node.labels && !step.node.labels.every(l => nextNode.labels.includes(l)))
              continue;
            if (step.node.properties) {
              let ok = true;
              for (const [k, v] of Object.entries(step.node.properties)) {
                if (nextNode.properties[k] !== v) {
                  ok = false;
                  break;
                }
              }
              if (!ok) continue;
            }
            const varsNext = new Map(varsLocal);
            varsNext.set(step.rel.variable, rel);
            varsNext.set(step.node.variable, nextNode);
            for await (const row of traverse(nextNode, hop + 1, varsNext)) {
              yield row;
            }
          }
        };
        for (const s of startNodes) {
          const varsStart = new Map(vars);
          varsStart.set(plan.start.variable, s);
          for await (const row of traverse(s, 0, varsStart)) {
            yield row;
          }
        }
        break;
      }
      case 'Foreach': {
        const innerPlan = logicalToPhysical(plan.statement, adapter);
        let items: unknown[];
        if (Array.isArray(plan.list)) {
          items = plan.list;
        } else {
          const v = evalExpr(plan.list, vars);
          items = Array.isArray(v) ? v : [];
        }
        for (const item of items) {
          vars.set(plan.variable, item);
          for await (const row of innerPlan(vars)) {
            yield row;
          }
        }
        break;
      }
      case 'Unwind': {
        let items: unknown[];
        if (Array.isArray(plan.list)) {
          items = plan.list;
        } else {
          const v = evalExpr(plan.list, vars);
          items = Array.isArray(v) ? v : [];
        }
        for (const item of items) {
          vars.set(plan.variable, item);
          const val = evalExpr(plan.returnExpression, vars);
          if (
            plan.returnExpression.type === 'Variable' &&
            plan.returnExpression.name === plan.variable
          ) {
            yield { [plan.variable]: val };
          } else {
            yield { value: val };
          }
        }
        break;
      }
      default:
        throw new Error('Query not supported in this MVP');
    }
  };
}
