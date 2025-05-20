import { LogicalPlan, astToLogical } from '../logical/LogicalPlan';
import {
  StorageAdapter,
  NodeRecord,
  RelRecord,
} from '../storage/StorageAdapter';
import { Expression, WhereClause } from '../parser/CypherParser';

function evalExpr(
  expr: Expression,
  vars: Map<string, any>,
  params: Record<string, any>
): any {
  switch (expr.type) {
    case 'Literal':
      return expr.value;
    case 'Property': {
      const rec = vars.get(expr.variable) as NodeRecord | RelRecord | undefined;
      if (!rec) return undefined;
      return rec.properties[expr.property];
    }
    case 'Variable':
      return vars.get(expr.name);
    case 'Parameter':
      return params[expr.name];
    case 'Add':
      const l = evalExpr(expr.left, vars, params);
      const r = evalExpr(expr.right, vars, params);
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

function evalWhere(
  where: WhereClause,
  vars: Map<string, any>,
  params: Record<string, any>
): boolean {
  switch (where.type) {
    case 'Condition': {
      const l = evalExpr(where.left, vars, params);
      const r = evalExpr(where.right, vars, params);
      switch (where.operator) {
        case '=':
          return l === r;
        case '>':
          return (l as any) > (r as any);
        case '>=':
          return (l as any) >= (r as any);
        case '<':
          return (l as any) < (r as any);
        case '<=':
          return (l as any) <= (r as any);
        case '<>':
          return l !== r;
        default:
          throw new Error('Unknown operator');
      }
    }
    case 'And':
      return evalWhere(where.left, vars, params) && evalWhere(where.right, vars, params);
    case 'Or':
      return evalWhere(where.left, vars, params) || evalWhere(where.right, vars, params);
    case 'Not':
      return !evalWhere(where.clause, vars, params);
    default:
      throw new Error('Unknown where clause');
  }
}

function evalPropValue(
  val: any,
  vars: Map<string, any>,
  params: Record<string, any>
): any {
  if (val && typeof val === 'object' && '__param' in val) {
    return params[(val as any).__param];
  }
  return val;
}

function evalProps(
  props: Record<string, unknown> | undefined,
  vars: Map<string, any>,
  params: Record<string, any>
): Record<string, any> {
  const out: Record<string, any> = {};
  if (!props) return out;
  for (const [k, v] of Object.entries(props)) {
    out[k] = evalPropValue(v, vars, params);
  }
  return out;
}

export type PhysicalPlan = (
  vars: Map<string, any>,
  params: Record<string, any>
) => AsyncIterable<Record<string, unknown>>;

export function logicalToPhysical(
  plan: LogicalPlan,
  adapter: StorageAdapter
): PhysicalPlan {
  return async function* (vars: Map<string, any>, params: Record<string, any>) {
    switch (plan.type) {
      case 'MatchReturn': {
        const isAgg = (e: Expression): boolean =>
          ['Count', 'Sum', 'Min', 'Max', 'Avg'].includes(e.type);
        const hasAgg = plan.returnItems.some(r => isAgg(r.expression));
        const rows: { row: Record<string, unknown>; order?: any; record?: NodeRecord | RelRecord }[] = [];
        const aliasFor = (item: typeof plan.returnItems[number], idx: number): string => {
          if (item.alias) return item.alias;
          if (item.expression.type === 'Variable') return item.expression.name;
          return plan.returnItems.length === 1 ? 'value' : `value${idx}`;
        };

        const groups = new Map<string, { row: Record<string, unknown>; aggs: any[]; record?: NodeRecord | RelRecord }>();

        const collectAgg = async (rec: NodeRecord | RelRecord) => {
          vars.set(plan.variable, rec);
          if (plan.where && !evalWhere(plan.where, vars, params)) return;
          const keyParts: unknown[] = [];
          for (const item of plan.returnItems) {
            if (!isAgg(item.expression)) keyParts.push(evalExpr(item.expression, vars, params));
          }
          const key = JSON.stringify(keyParts);
          let group = groups.get(key);
          if (!group) {
            const row: Record<string, unknown> = {};
            let i = 0;
            plan.returnItems.forEach((item, idx) => {
              if (!isAgg(item.expression)) {
                row[aliasFor(item, idx)] = keyParts[i++];
              }
            });
            group = { row, aggs: [], record: rec };
            groups.set(key, group);
          }
          plan.returnItems.forEach((item, idx) => {
            if (!isAgg(item.expression)) return;
            const expr = item.expression;
            const agg = (group!.aggs[idx] = group!.aggs[idx] ?? { count: 0, sum: 0 });
            switch (expr.type) {
              case 'Count':
                agg.count++;
                break;
              case 'Sum': {
                const v = evalExpr(expr.expression, vars, params);
                if (typeof v === 'number') agg.sum += v;
                break;
              }
              case 'Min': {
                const v = evalExpr(expr.expression, vars, params);
                if (agg.min === undefined || v < agg.min) agg.min = v;
                break;
              }
              case 'Max': {
                const v = evalExpr(expr.expression, vars, params);
                if (agg.max === undefined || v > agg.max) agg.max = v;
                break;
              }
              case 'Avg': {
                const v = evalExpr(expr.expression, vars, params);
                if (typeof v === 'number') {
                  agg.sum += v;
                  agg.count++;
                }
                break;
              }
            }
          });
        };

        const collectSimple = async (rec: NodeRecord | RelRecord) => {
          vars.set(plan.variable, rec);
          if (plan.where && !evalWhere(plan.where, vars, params)) return;
          const row: Record<string, unknown> = {};
          const aliasVars = new Map(vars);
          plan.returnItems.forEach((item, idx) => {
            const val = evalExpr(item.expression, vars, params);
            row[aliasFor(item, idx)] = val;
            if (item.alias) aliasVars.set(item.alias, val);
          });
          const order = plan.orderBy ? evalExpr(plan.orderBy, aliasVars, params) : undefined;
          rows.push({ row, order, record: rec });
        };

        const collect = hasAgg ? collectAgg : collectSimple;

        if (plan.isRelationship) {
          if (!adapter.scanRelationships)
            throw new Error('Adapter does not support MATCH');
          for await (const rel of adapter.scanRelationships()) {
            const label = plan.labels && plan.labels.length > 0 ? plan.labels[0] : undefined;
            if (label && rel.type !== label) continue;
            if (plan.properties) {
              let ok = true;
              for (const [k, v] of Object.entries(plan.properties)) {
                if (rel.properties[k] !== evalPropValue(v, vars, params)) {
                  ok = false;
                  break;
                }
              }
              if (!ok) continue;
            }
            await collect(rel);
          }
        } else {
          let usedIndex = false;
          if (
            plan.labels &&
            plan.labels.length > 0 &&
            plan.properties &&
            Object.keys(plan.properties).length === 1 &&
            adapter.indexLookup &&
            adapter.listIndexes
          ) {
            const [prop, valueExpr] = Object.entries(plan.properties)[0];
            const value = evalPropValue(valueExpr, vars, params);
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
                  if (node.properties[k] !== evalPropValue(v, vars, params)) {
                    ok = false;
                    break;
                  }
                }
                if (!ok) continue;
              }
              await collect(node);
            }
          }
        }

        if (hasAgg) {
          for (const group of groups.values()) {
            plan.returnItems.forEach((item, idx) => {
              if (!isAgg(item.expression)) return;
              const agg = group.aggs[idx];
              let val: any;
              switch (item.expression.type) {
                case 'Count':
                  val = agg.count;
                  break;
                case 'Sum':
                  val = agg.sum;
                  break;
                case 'Min':
                  val = agg.min;
                  break;
                case 'Max':
                  val = agg.max;
                  break;
                case 'Avg':
                  val = agg.count ? agg.sum / agg.count : null;
                  break;
              }
              group.row[aliasFor(item, idx)] = val;
            });
            let order: any;
            if (plan.orderBy) {
              const aliasVars = new Map(vars);
              plan.returnItems.forEach((it, i) => {
                const alias = aliasFor(it, i);
                aliasVars.set(alias, group.row[alias]);
                if (it.alias) aliasVars.set(it.alias, group.row[alias]);
              });
              order = evalExpr(plan.orderBy, aliasVars, params);
            }
            rows.push({ row: group.row, order, record: group.record });
          }
        }

        if (plan.orderBy) {
          rows.sort((a, b) => {
            if (a.order === b.order) return 0;
            if (a.order === undefined) return 1;
            if (b.order === undefined) return -1;
            const cmp = a.order > b.order ? 1 : -1;
            return plan.orderDirection === 'DESC' ? -cmp : cmp;
          });
        }

        const start = plan.skip ?? 0;
        let end = rows.length;
        if (plan.limit !== undefined) end = Math.min(end, start + plan.limit);
        if (rows.length === 0 && plan.optional) {
          vars.delete(plan.variable);
          const row: Record<string, unknown> = {};
          plan.returnItems.forEach((item, idx) => {
            const val = evalExpr(item.expression, vars, params);
            row[aliasFor(item, idx)] = val;
          });
          yield row;
        } else {
          for (let i = start; i < end; i++) {
            vars.set(plan.variable, rows[i].record as any);
            yield rows[i].row;
          }
        }
        break;
      }
      case 'Create': {
        if (!adapter.createNode) throw new Error('Adapter does not support CREATE');
        const node = await adapter.createNode(
          plan.labels ?? [],
          evalProps(plan.properties ?? {}, vars, params)
        );
        vars.set(plan.variable, node);
        if (plan.setProperties && adapter.updateNodeProperties) {
          for (const [k, expr] of Object.entries(plan.setProperties)) {
            const val = evalExpr(expr, vars, params);
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
        let node = await adapter.findNode(
          plan.labels ?? [],
          evalProps(plan.properties ?? {}, vars, params)
        );
        let created = false;
        if (!node) {
          node = await adapter.createNode(
            plan.labels ?? [],
            evalProps(plan.properties ?? {}, vars, params)
          );
          created = true;
        }
        vars.set(plan.variable, node);
        if (created && plan.onCreateSet && adapter.updateNodeProperties) {
          for (const [k, expr] of Object.entries(plan.onCreateSet)) {
            const val = evalExpr(expr, vars, params);
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
                if (rel.properties[k] !== evalPropValue(v, vars, params)) {
                  ok = false;
                  break;
                }
              }
              if (!ok) continue;
            }
            vars.set(plan.variable, rel);
            if (plan.where && !evalWhere(plan.where, vars, params)) continue;
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
                if (node.properties[k] !== evalPropValue(v, vars, params)) {
                  ok = false;
                  break;
                }
              }
            }
            if (!ok) continue;
            vars.set(plan.variable, node);
            if (plan.where && !evalWhere(plan.where, vars, params)) continue;
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
            if (plan.where && !evalWhere(plan.where, vars, params)) continue;
            const val = evalExpr(plan.value, vars, params);
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
            if (!plan.where || evalWhere(plan.where, vars, params)) {
              const val = evalExpr(plan.value, vars, params);
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
                  if (node.properties[k] !== evalPropValue(v, vars, params)) {
                    ok = false;
                    break;
                  }
                }
              }
              if (!ok) continue;
              vars.set(plan.variable, node);
              if (plan.where && !evalWhere(plan.where, vars, params)) continue;
              const val = evalExpr(plan.value, vars, params);
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
        const start = await adapter.createNode(
          plan.start.labels ?? [],
          evalProps(plan.start.properties ?? {}, vars, params)
        );
        const end = await adapter.createNode(
          plan.end.labels ?? [],
          evalProps(plan.end.properties ?? {}, vars, params)
        );
        const rel = await adapter.createRelationship(
          plan.relType,
          start.id,
          end.id,
          evalProps(plan.relProperties ?? {}, vars, params)
        );
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
          existing = await adapter.createRelationship(
            plan.relType,
            startNode.id,
            endNode.id,
            evalProps(plan.relProperties ?? {}, vars, params)
          );
          created = true;
        }
        vars.set(plan.relVariable, existing);
        if (created && plan.onCreateSet && adapter.updateRelationshipProperties) {
          for (const [k, expr] of Object.entries(plan.onCreateSet)) {
            const val = evalExpr(expr, vars, params);
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
              if (node.properties[k] !== evalPropValue(v, vars, params)) {
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
              if (node.properties[k] !== evalPropValue(v, vars, params)) {
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
        const aliasFor = (item: typeof plan.returnItems[number], idx: number): string => {
          if (item.alias) return item.alias;
          if (item.expression.type === 'Variable') return item.expression.name;
          return plan.returnItems.length === 1 ? 'value' : `value${idx}`;
        };
        const rows: { row: Record<string, unknown>; order?: any }[] = [];
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
        const traverse = async (
          node: NodeRecord,
          hop: number,
          varsLocal: Map<string, any>
        ): Promise<void> => {
          if (hop >= plan.hops.length) {
            const row: Record<string, unknown> = {};
            const aliasVars = new Map(varsLocal);
            plan.returnItems.forEach((item, idx) => {
              const val = evalExpr(item.expression, varsLocal, params);
              row[aliasFor(item, idx)] = val;
              if (item.alias) aliasVars.set(item.alias, val);
            });
            const order = plan.orderBy ? evalExpr(plan.orderBy, aliasVars, params) : undefined;
            rows.push({ row, order });
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
                if (rel.properties[k] !== evalPropValue(v, varsLocal, params)) {
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
                if (nextNode.properties[k] !== evalPropValue(v, varsLocal, params)) {
                  ok = false;
                  break;
                }
              }
              if (!ok) continue;
            }
            const varsNext = new Map(varsLocal);
            if (step.rel.variable) varsNext.set(step.rel.variable, rel);
            varsNext.set(step.node.variable, nextNode);
            await traverse(nextNode, hop + 1, varsNext);
          }
        };
        for (const s of startNodes) {
          const varsStart = new Map(vars);
          varsStart.set(plan.start.variable, s);
          await traverse(s, 0, varsStart);
        }
        if (plan.orderBy) {
          rows.sort((a, b) => {
            if (a.order === b.order) return 0;
            if (a.order === undefined) return 1;
            if (b.order === undefined) return -1;
            const cmp = a.order > b.order ? 1 : -1;
            return plan.orderDirection === 'DESC' ? -cmp : cmp;
          });
        }
        const startIdx = plan.skip ?? 0;
        let end = rows.length;
        if (plan.limit !== undefined) end = Math.min(end, startIdx + plan.limit);
        for (let i = startIdx; i < end; i++) {
          yield rows[i].row;
        }
        break;
      }
      case 'Foreach': {
        const innerPlan = logicalToPhysical(plan.statement, adapter);
        let items: unknown[];
        if (Array.isArray(plan.list)) {
          items = plan.list;
        } else {
          const v = evalExpr(plan.list, vars, params);
          items = Array.isArray(v) ? v : [];
        }
        for (const item of items) {
          vars.set(plan.variable, item);
          for await (const row of innerPlan(vars, params)) {
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
          const v = evalExpr(plan.list, vars, params);
          items = Array.isArray(v) ? v : [];
        }
        for (const item of items) {
          vars.set(plan.variable, item);
          const val = evalExpr(plan.returnExpression, vars, params);
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
      case 'Union': {
        const left = logicalToPhysical(plan.left, adapter);
        for await (const row of left(new Map(vars), params)) {
          yield row;
        }
        const right = logicalToPhysical(plan.right, adapter);
        for await (const row of right(new Map(vars), params)) {
          yield row;
        }
        break;
      }
      case 'Call': {
        const innerPlans = plan.subquery.map(q =>
          logicalToPhysical(astToLogical(q), adapter)
        );
        const local = new Map(vars);
        for (let i = 0; i < innerPlans.length; i++) {
          const p = innerPlans[i];
          let idx = 0;
          for await (const _row of p(local, params)) {
            idx++;
            if (i === innerPlans.length - 1) {
              const out: Record<string, unknown> = {};
              plan.returnItems.forEach((item, ridx) => {
                const alias =
                  item.alias ||
                  (item.expression.type === 'Variable'
                    ? item.expression.name
                    : plan.returnItems.length === 1
                    ? 'value'
                    : `value${ridx}`);
                out[alias] = evalExpr(item.expression, local, params);
              });
              yield out;
            }
          }
        }
        break;
      }
      default:
        throw new Error('Query not supported in this MVP');
    }
  };
}
