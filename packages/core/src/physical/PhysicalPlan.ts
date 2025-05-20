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
      {
        const val = vars.get(expr.name);
        if (val && typeof val === 'object' && 'nodes' in val && 'relationships' in val)
          return (val as any).nodes;
        return val;
      }
    case 'Parameter':
      return params[expr.name];
    case 'Add':
      const l = evalExpr(expr.left, vars, params);
      const r = evalExpr(expr.right, vars, params);
      if (typeof l === 'number' && typeof r === 'number') {
        return l + r;
      }
      return String(l) + String(r);
    case 'Sub': {
      const l = evalExpr(expr.left, vars, params);
      const r = evalExpr(expr.right, vars, params);
      if (typeof l === 'number' && typeof r === 'number') {
        return l - r;
      }
      return NaN;
    }
    case 'Mul': {
      const l = evalExpr(expr.left, vars, params);
      const r = evalExpr(expr.right, vars, params);
      if (typeof l === 'number' && typeof r === 'number') {
        return l * r;
      }
      return NaN;
    }
    case 'Div': {
      const l = evalExpr(expr.left, vars, params);
      const r = evalExpr(expr.right, vars, params);
      if (typeof l === 'number' && typeof r === 'number') {
        return l / r;
      }
      return NaN;
    }
    case 'Nodes':
      {
        const val = vars.get(expr.variable);
        if (val && typeof val === 'object' && 'nodes' in val) return (val as any).nodes;
        return val;
      }
    case 'Relationships':
      {
        const val = vars.get(expr.variable);
        if (val && typeof val === 'object' && 'relationships' in val)
          return (val as any).relationships;
        return undefined;
      }
    case 'Length': {
      let val: any;
      if (expr.expression.type === 'Variable') {
        val = vars.get(expr.expression.name);
      } else {
        val = evalExpr(expr.expression, vars, params);
      }
      if (val && typeof val === 'object' && 'relationships' in val) {
        return (val as any).relationships.length;
      }
      if (Array.isArray(val) || typeof val === 'string') {
        return (val as any).length;
      }
      return undefined;
    }
    case 'Labels': {
      const rec = vars.get(expr.variable) as NodeRecord | undefined;
      return rec ? rec.labels : undefined;
    }
    case 'Type': {
      const rec = vars.get(expr.variable) as RelRecord | undefined;
      return rec ? rec.type : undefined;
    }
    case 'Id': {
      const rec = vars.get(expr.variable) as NodeRecord | RelRecord | undefined;
      return rec ? rec.id : undefined;
    }
    case 'All':
      return Object.fromEntries(vars.entries());
    default:
      throw new Error('Unknown expression');
  }
}

async function* findPaths(
  adapter: StorageAdapter,
  startId: number | string,
  endId: number | string,
  minHops = 1,
  maxHops = Infinity,
  direction: 'out' | 'in' | 'none' = 'out',
  relType?: string
): AsyncIterable<{ nodes: NodeRecord[]; relationships: RelRecord[] }> {
  if (!adapter.scanRelationships || !adapter.getNodeById) {
    throw new Error('Adapter does not support path finding');
  }
  const rels: RelRecord[] = [];
  for await (const r of adapter.scanRelationships()) {
    if (relType && r.type !== relType) continue;
    rels.push(r);
  }
  const queue: { nodes: Array<number | string>; relationships: RelRecord[] }[] = [
    { nodes: [startId], relationships: [] },
  ];
  while (queue.length > 0) {
    const { nodes: path, relationships: pathRels } = queue.shift()!;
    const last = path[path.length - 1];
    const hops = path.length - 1;
    if (last === endId && hops >= minHops && hops <= maxHops) {
      const nodes: NodeRecord[] = [];
      for (const id of path) {
        const n = await adapter.getNodeById(id);
        if (!n) return;
        nodes.push(n);
      }
      yield { nodes, relationships: pathRels };
    }
    if (hops >= maxHops) continue;
    for (const rel of rels) {
      if (direction === 'out' || direction === 'none') {
        if (rel.startNode === last) {
          if (!path.includes(rel.endNode)) {
            queue.push({ nodes: [...path, rel.endNode], relationships: [...pathRels, rel] });
          }
        }
      }
      if (direction === 'in' || direction === 'none') {
        if (rel.endNode === last) {
          if (!path.includes(rel.startNode)) {
            queue.push({ nodes: [...path, rel.startNode], relationships: [...pathRels, rel] });
          }
        }
      }
    }
  }
}

function evalWhere(
  where: WhereClause,
  vars: Map<string, any>,
  params: Record<string, any>
): boolean {
  switch (where.type) {
    case 'Condition': {
      const l = evalExpr(where.left, vars, params);
      const r = where.right !== undefined ? evalExpr(where.right, vars, params) : undefined;
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
        case 'IN':
          return Array.isArray(r) && r.includes(l);
        case 'IS NULL':
          return l === null || l === undefined;
        case 'IS NOT NULL':
          return l !== null && l !== undefined;
        case 'STARTS WITH':
          return typeof l === 'string' && typeof r === 'string' && l.startsWith(r);
        case 'ENDS WITH':
          return typeof l === 'string' && typeof r === 'string' && l.endsWith(r);
        case 'CONTAINS':
          return typeof l === 'string' && typeof r === 'string' && l.includes(r);
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
  if (val && typeof val === 'object') {
    if ('__param' in val) {
      return params[(val as any).__param];
    }
    if ('type' in val) {
      return evalExpr(val as Expression, vars, params);
    }
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

function serializeVars(vars: Map<string, any>): string {
  const obj: Record<string, unknown> = {};
  const entries = Array.from(vars.entries()).sort(([a], [b]) =>
    a.localeCompare(b)
  );
  for (const [k, v] of entries) {
    if (v && typeof v === 'object' && 'id' in v) {
      obj[k] = (v as any).id;
    } else {
      obj[k] = v;
    }
  }
  return JSON.stringify(obj);
}

function hasAgg(expr: Expression): boolean {
  switch (expr.type) {
    case 'Count':
    case 'Sum':
    case 'Min':
    case 'Max':
    case 'Avg':
    case 'Collect':
      return true;
    case 'Add':
    case 'Sub':
    case 'Mul':
    case 'Div':
      return hasAgg(expr.left) || hasAgg(expr.right);
    default:
      return false;
  }
}

type AggState =
  | {
      type: 'Count';
      distinct?: boolean;
      expr: Expression | null;
      count: number;
      values: Set<string>;
    }
  | {
      type: 'Sum' | 'Min' | 'Max';
      distinct?: boolean;
      expr: Expression;
      sum?: number;
      min?: any;
      max?: any;
      values: Set<string>;
    }
  | {
      type: 'Avg';
      distinct?: boolean;
      expr: Expression;
      sum: number;
      count: number;
      values: Set<string>;
    }
  | {
      type: 'Collect';
      distinct?: boolean;
      expr: Expression;
      values: unknown[];
      seen: Set<string>;
    }
  | { type: 'Add' | 'Sub' | 'Mul' | 'Div'; left: AggState | null; right: AggState | null };

function initAggState(expr: Expression): AggState | null {
  switch (expr.type) {
    case 'Count':
      return { type: 'Count', distinct: expr.distinct, expr: expr.expression, count: 0, values: new Set() };
    case 'Sum':
    case 'Min':
    case 'Max':
      return { type: expr.type, distinct: expr.distinct, expr: expr.expression, sum: 0, min: undefined, max: undefined, values: new Set() };
    case 'Avg':
      return { type: 'Avg', distinct: expr.distinct, expr: expr.expression, sum: 0, count: 0, values: new Set() };
    case 'Collect':
      return { type: 'Collect', distinct: expr.distinct, expr: expr.expression, values: [], seen: new Set() };
    case 'Add':
    case 'Sub': {
      const left = initAggState(expr.left);
      const right = initAggState(expr.right);
      return left || right ? { type: expr.type, left, right } : null;
    }
    default:
      return null;
  }
}

function updateAggState(
  expr: Expression,
  state: AggState | null,
  vars: Map<string, any>,
  params: Record<string, any>
) {
  if (!state) return;
  switch (state.type) {
    case 'Count': {
      const val = state.expr ? evalExpr(state.expr, vars, params) : null;
      if (state.distinct) {
        const key =
          state.expr === null
            ? serializeVars(vars)
            : JSON.stringify(val);
        if (!state.values.has(key)) {
          state.values.add(key);
          state.count++;
        }
      } else {
        state.count++;
      }
      break;
    }
    case 'Sum': {
      const v = evalExpr(state.expr, vars, params);
      let include = true;
      if (state.distinct) {
        const key = JSON.stringify(v);
        if (state.values.has(key)) include = false;
        else state.values.add(key);
      }
      if (include && typeof v === 'number') state.sum = (state.sum || 0) + v;
      break;
    }
    case 'Min': {
      const v = evalExpr(state.expr, vars, params);
      let include = true;
      if (state.distinct) {
        const key = JSON.stringify(v);
        if (state.values.has(key)) include = false;
        else state.values.add(key);
      }
      if (include && (state.min === undefined || v < state.min)) state.min = v;
      break;
    }
    case 'Max': {
      const v = evalExpr(state.expr, vars, params);
      let include = true;
      if (state.distinct) {
        const key = JSON.stringify(v);
        if (state.values.has(key)) include = false;
        else state.values.add(key);
      }
      if (include && (state.max === undefined || v > state.max)) state.max = v;
      break;
    }
    case 'Avg': {
      const v = evalExpr(state.expr, vars, params);
      let include = true;
      if (state.distinct) {
        const key = JSON.stringify(v);
        if (state.values.has(key)) include = false;
        else state.values.add(key);
      }
      if (include && typeof v === 'number') {
        state.sum += v;
        state.count++;
      }
      break;
    }
    case 'Collect': {
      const v = evalExpr(state.expr, vars, params);
      if (state.distinct) {
        const key = JSON.stringify(v);
        if (state.seen.has(key)) break;
        state.seen.add(key);
      }
      state.values.push(v);
      break;
    }
    case 'Add':
    case 'Sub':
    case 'Mul':
    case 'Div':
      updateAggState((expr as any).left, state.left, vars, params);
      updateAggState((expr as any).right, state.right, vars, params);
      break;
  }
}

function finalizeAgg(
  expr: Expression,
  state: AggState | null,
  vars: Map<string, any>,
  params: Record<string, any>
): any {
  switch (expr.type) {
    case 'Add':
      return (
        finalizeAgg((expr as any).left, state ? (state as any).left : null, vars, params) +
        finalizeAgg((expr as any).right, state ? (state as any).right : null, vars, params)
      );
    case 'Sub':
      return (
        finalizeAgg((expr as any).left, state ? (state as any).left : null, vars, params) -
        finalizeAgg((expr as any).right, state ? (state as any).right : null, vars, params)
      );
    case 'Mul':
      return (
        finalizeAgg((expr as any).left, state ? (state as any).left : null, vars, params) *
        finalizeAgg((expr as any).right, state ? (state as any).right : null, vars, params)
      );
    case 'Div':
      return (
        finalizeAgg((expr as any).left, state ? (state as any).left : null, vars, params) /
        finalizeAgg((expr as any).right, state ? (state as any).right : null, vars, params)
      );
    case 'Count':
      return state ? (state as any).count : 0;
    case 'Sum':
      return state ? (state as any).sum ?? 0 : 0;
    case 'Min':
      return state ? (state as any).min ?? null : null;
    case 'Max':
      return state ? (state as any).max ?? null : null;
    case 'Avg':
      return state ? ((state as any).count ? (state as any).sum / (state as any).count : null) : null;
    case 'Collect':
      return state ? (state as any).values : [];
    default:
      return evalExpr(expr, vars, params);
  }
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
        const hasAggItem: boolean[] = plan.returnItems.map(r => hasAgg(r.expression));
        const hasAggFlag: boolean = hasAggItem.some((v: boolean) => v);
        const rows: { row: Record<string, unknown>; order?: any; record?: NodeRecord | RelRecord }[] = [];
        const aliasFor = (item: typeof plan.returnItems[number], idx: number): string => {
          if (item.alias) return item.alias;
          if (item.expression.type === 'Variable') return item.expression.name;
          return plan.returnItems.length === 1 ? 'value' : `value${idx}`;
        };

        const groups = new Map<string, { row: Record<string, unknown>; aggs: (AggState | null)[]; record?: NodeRecord | RelRecord }>();

        const collectAgg = async (rec: NodeRecord | RelRecord) => {
          vars.set(plan.variable, rec);
          if (plan.where && !evalWhere(plan.where, vars, params)) return;
          const keyParts: unknown[] = [];
          for (let i = 0; i < plan.returnItems.length; i++) {
            if (!hasAggItem[i]) keyParts.push(evalExpr(plan.returnItems[i].expression, vars, params));
          }
          const key = JSON.stringify(keyParts);
          let group = groups.get(key);
          if (!group) {
            const row: Record<string, unknown> = {};
            let i = 0;
            plan.returnItems.forEach((item, idx) => {
              if (!hasAggItem[idx]) {
                row[aliasFor(item, idx)] = keyParts[i++];
              }
            });
            group = { row, aggs: [], record: rec };
            groups.set(key, group);
          }
          plan.returnItems.forEach((item, idx) => {
            if (!hasAggItem[idx]) return;
            const expr = item.expression;
            const current = (group!.aggs[idx] = group!.aggs[idx] ?? initAggState(expr));
            updateAggState(expr, current, vars, params);
          });
        };

        const collectSimple = async (rec: NodeRecord | RelRecord) => {
          vars.set(plan.variable, rec);
          if (plan.where && !evalWhere(plan.where, vars, params)) return;
          const row: Record<string, unknown> = {};
          const aliasVars = new Map(vars);
          plan.returnItems.forEach((item, idx) => {
            if (item.expression.type === 'All') {
              for (const [k, v] of vars.entries()) {
                row[k] = v;
                aliasVars.set(k, v);
              }
            } else {
              const val = evalExpr(item.expression, vars, params);
              row[aliasFor(item, idx)] = val;
              if (item.alias) aliasVars.set(item.alias, val);
            }
          });
          const order = plan.orderBy
            ? plan.orderBy.map(o => evalExpr(o.expression, aliasVars, params))
            : undefined;
          rows.push({ row, order, record: rec });
        };

        const collect = hasAggFlag ? collectAgg : collectSimple;

        if (plan.isRelationship) {
          if (!adapter.scanRelationships)
            throw new Error('Adapter does not support MATCH');
          const boundRel = vars.get(plan.variable) as RelRecord | undefined;
          if (boundRel) {
            let ok = true;
            if (plan.labels && plan.labels.length > 0 && boundRel.type !== plan.labels[0]) {
              ok = false;
            }
            if (ok && plan.properties) {
              for (const [k, v] of Object.entries(plan.properties)) {
                if (boundRel.properties[k] !== evalPropValue(v, vars, params)) {
                  ok = false;
                  break;
                }
              }
            }
            if (ok) await collect(boundRel);
          } else {
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
          }
        } else {
          let usedIndex = false;
          const boundNode = vars.get(plan.variable) as NodeRecord | undefined;
          if (boundNode) {
            let ok = true;
            if (plan.labels && !plan.labels.every(l => boundNode.labels.includes(l))) {
              ok = false;
            }
            if (ok && plan.properties) {
              for (const [k, v] of Object.entries(plan.properties)) {
                if (boundNode.properties[k] !== evalPropValue(v, vars, params)) {
                  ok = false;
                  break;
                }
              }
            }
            if (ok) {
              await collect(boundNode);
              usedIndex = true;
            }
          }
          if (
            !boundNode &&
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
          if (
            !usedIndex &&
            !boundNode &&
            plan.labels &&
            plan.labels.length > 0 &&
            !plan.properties &&
            adapter.indexLookup &&
            adapter.listIndexes &&
            plan.where &&
            plan.where.type === 'Condition' &&
            plan.where.operator === '=' &&
            plan.where.left.type === 'Property' &&
            plan.where.left.variable === plan.variable &&
            plan.where.right
          ) {
            const prop = plan.where.left.property;
            const value = evalExpr(plan.where.right, vars, params);
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

        if (hasAggFlag) {
          if (
            groups.size === 0 &&
            hasAggItem.every((v: boolean) => v)
          ) {
            groups.set('__empty__', { row: {}, aggs: [] });
          }
          for (const group of groups.values()) {
            const localVars = new Map(vars);
            if (group.record) localVars.set(plan.variable, group.record);
            plan.returnItems.forEach((item, idx) => {
              if (!hasAggItem[idx]) return;
              group.row[aliasFor(item, idx)] = finalizeAgg(
                item.expression,
                group.aggs[idx],
                localVars,
                params
              );
            });
            let order: any[] | undefined;
            if (plan.orderBy) {
              const aliasVars = new Map(vars);
              plan.returnItems.forEach((it, i) => {
                const alias = aliasFor(it, i);
                aliasVars.set(alias, group.row[alias]);
                if (it.alias) aliasVars.set(it.alias, group.row[alias]);
              });
              order = plan.orderBy.map(o =>
                evalExpr(o.expression, aliasVars, params)
              );
            }
            rows.push({ row: group.row, order, record: group.record });
          }
        }

        if (plan.distinct) {
          const seen = new Set<string>();
          const unique: typeof rows = [];
          for (const r of rows) {
            const key = JSON.stringify(r.row);
            if (!seen.has(key)) {
              seen.add(key);
              unique.push(r);
            }
          }
          rows.splice(0, rows.length, ...unique);
        }

        if (plan.orderBy) {
          rows.sort((a, b) => {
            for (let i = 0; i < plan.orderBy!.length; i++) {
              const av = a.order ? a.order[i] : undefined;
              const bv = b.order ? b.order[i] : undefined;
              if (av === bv) continue;
              if (av === undefined) return 1;
              if (bv === undefined) return -1;
              let cmp = av > bv ? 1 : -1;
              if (plan.orderBy![i].direction === 'DESC') cmp = -cmp;
              return cmp;
            }
            return 0;
          });
        }

        const start = plan.skip ? Number(evalExpr(plan.skip, vars, params)) : 0;
        let end = rows.length;
        if (plan.limit !== undefined)
          end = Math.min(end, start + Number(evalExpr(plan.limit, vars, params)));
        if (rows.length === 0 && plan.optional) {
          vars.delete(plan.variable);
          const row: Record<string, unknown> = {};
          plan.returnItems.forEach((item, idx) => {
            if (hasAggItem[idx]) {
              row[aliasFor(item, idx)] = finalizeAgg(
                item.expression,
                initAggState(item.expression),
                vars,
                params
              );
            } else {
              const val = evalExpr(item.expression, vars, params);
              row[aliasFor(item, idx)] = val;
            }
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
      case 'MatchMultiReturn': {
        const hasAggItem: boolean[] = plan.returnItems.map(r => hasAgg(r.expression));
        const hasAggFlag: boolean = hasAggItem.some(v => v);
        const rows: { row: Record<string, unknown>; order?: any }[] = [];
        const aliasFor = (item: typeof plan.returnItems[number], idx: number): string => {
          if (item.alias) return item.alias;
          if (item.expression.type === 'Variable') return item.expression.name;
          return plan.returnItems.length === 1 ? 'value' : `value${idx}`;
        };
        const groups = new Map<string, { row: Record<string, unknown>; aggs: (AggState | null)[] }>();

        const traverse = async (idx: number, varsLocal: Map<string, any>): Promise<void> => {
          if (idx >= plan.patterns.length) {
            if (plan.where && !evalWhere(plan.where, varsLocal, params)) return;
            const aliasVars = new Map(varsLocal);
            if (hasAggFlag) {
              const keyParts: unknown[] = [];
              for (let i = 0; i < plan.returnItems.length; i++) {
                if (!hasAggItem[i]) keyParts.push(evalExpr(plan.returnItems[i].expression, aliasVars, params));
              }
              const key = JSON.stringify(keyParts);
              let group = groups.get(key);
              if (!group) {
                const row: Record<string, unknown> = {};
                let j = 0;
                plan.returnItems.forEach((item, idx2) => {
                  if (!hasAggItem[idx2]) {
                    row[aliasFor(item, idx2)] = keyParts[j++];
                  }
                });
                group = { row, aggs: [] };
                groups.set(key, group);
              }
              plan.returnItems.forEach((item, idx2) => {
                if (!hasAggItem[idx2]) return;
                const current = (group!.aggs[idx2] = group!.aggs[idx2] ?? initAggState(item.expression));
                updateAggState(item.expression, current, aliasVars, params);
              });
            } else {
              const row: Record<string, unknown> = {};
              plan.returnItems.forEach((item, idx2) => {
                if (item.expression.type === 'All') {
                  for (const [k, v] of aliasVars.entries()) row[k] = v;
                } else {
                  const val = evalExpr(item.expression, aliasVars, params);
                  row[aliasFor(item, idx2)] = val;
                  if (item.alias) aliasVars.set(item.alias, val);
                }
              });
              const order = plan.orderBy ? plan.orderBy.map(o => evalExpr(o.expression, aliasVars, params)) : undefined;
              rows.push({ row, order });
            }
            return;
          }

          const pat = plan.patterns[idx];
          let usedIndex = false;
          let matched = false;
          if (
            pat.labels &&
            pat.labels.length > 0 &&
            pat.properties &&
            Object.keys(pat.properties).length === 1 &&
            adapter.indexLookup &&
            adapter.listIndexes
          ) {
            const [prop, valueExpr] = Object.entries(pat.properties)[0];
            const value = evalPropValue(valueExpr, varsLocal, params);
            const indexes = await adapter.listIndexes();
            const label = pat.labels[0];
            const found = indexes.find(i => i.label === label && i.properties.length === 1 && i.properties[0] === prop);
            if (found) {
              for await (const node of adapter.indexLookup(label, prop, value)) {
                const nextVars = new Map(varsLocal);
                nextVars.set(pat.variable, node);
                matched = true;
                await traverse(idx + 1, nextVars);
              }
              usedIndex = true;
            }
          }
          if (!usedIndex) {
            const scan = adapter.scanNodes!(pat.labels ? { labels: pat.labels } : {});
            for await (const node of scan) {
              if (pat.properties) {
                let ok = true;
                for (const [k, v] of Object.entries(pat.properties)) {
                  if (node.properties[k] !== evalPropValue(v, varsLocal, params)) {
                    ok = false;
                    break;
                  }
                }
                if (!ok) continue;
              }
              const nextVars = new Map(varsLocal);
              nextVars.set(pat.variable, node);
              matched = true;
              await traverse(idx + 1, nextVars);
            }
          }
          if (!matched && plan.optional) {
            const nextVars = new Map(varsLocal);
            nextVars.set(pat.variable, undefined);
            await traverse(idx + 1, nextVars);
          }
        };

        await traverse(0, new Map(vars));

        if (hasAggFlag) {
          if (groups.size === 0 && hasAggItem.every(v => v)) {
            groups.set('__empty__', { row: {}, aggs: [] });
          }
          for (const [key, group] of groups) {
            const aliasVars = new Map(vars);
            plan.returnItems.forEach((item, idx2) => {
              const alias = aliasFor(item, idx2);
              aliasVars.set(alias, group.row[alias]);
              if (item.alias) aliasVars.set(item.alias, group.row[alias]);
            });
            plan.returnItems.forEach((item, idx2) => {
              if (!hasAggItem[idx2]) return;
              group.row[aliasFor(item, idx2)] = finalizeAgg(
                item.expression,
                group.aggs[idx2],
                aliasVars,
                params
              );
            });
            let order: any[] | undefined;
            if (plan.orderBy) {
              order = plan.orderBy.map(o => evalExpr(o.expression, aliasVars, params));
            }
            rows.push({ row: group.row, order });
          }
        }

        if (plan.distinct) {
          const seen = new Set<string>();
          const unique: typeof rows = [];
          for (const r of rows) {
            const key = JSON.stringify(r.row);
            if (!seen.has(key)) {
              seen.add(key);
              unique.push(r);
            }
          }
          rows.splice(0, rows.length, ...unique);
        }

        if (plan.orderBy) {
          rows.sort((a, b) => {
            for (let i = 0; i < plan.orderBy!.length; i++) {
              const av = a.order ? a.order[i] : undefined;
              const bv = b.order ? b.order[i] : undefined;
              if (av === bv) continue;
              if (av === undefined) return 1;
              if (bv === undefined) return -1;
              let cmp = av > bv ? 1 : -1;
              if (plan.orderBy![i].direction === 'DESC') cmp = -cmp;
              return cmp;
            }
            return 0;
          });
        }

        const startIdx = plan.skip ? Number(evalExpr(plan.skip, vars, params)) : 0;
        let endIdx = rows.length;
        if (plan.limit !== undefined) endIdx = Math.min(endIdx, startIdx + Number(evalExpr(plan.limit, vars, params)));
        for (let i = startIdx; i < endIdx; i++) {
          yield rows[i].row;
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
        if (!created && plan.onMatchSet && adapter.updateNodeProperties) {
          for (const [k, expr] of Object.entries(plan.onMatchSet)) {
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
                if (rel.properties[k] !== evalPropValue(v, vars, params)) {
                  ok = false;
                  break;
                }
              }
              if (!ok) continue;
            }
            vars.set(plan.variable, rel);
            if (plan.where && !evalWhere(plan.where, vars, params)) continue;
            const updates: Record<string, any> = {};
            for (const [prop, expr] of Object.entries(plan.updates)) {
              const val = evalExpr(expr, vars, params);
              updates[prop] = val;
              rel.properties[prop] = val;
            }
            await adapter.updateRelationshipProperties(rel.id, updates);
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
              const updates: Record<string, any> = {};
              for (const [prop, expr] of Object.entries(plan.updates)) {
                const val = evalExpr(expr, vars, params);
                updates[prop] = val;
                node.properties[prop] = val;
              }
              await adapter.updateNodeProperties(node.id, updates);
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
              const updates: Record<string, any> = {};
              for (const [prop, expr] of Object.entries(plan.updates)) {
                const val = evalExpr(expr, vars, params);
                updates[prop] = val;
                node.properties[prop] = val;
              }
              await adapter.updateNodeProperties(node.id, updates);
              if (plan.returnVariable) {
                yield { [plan.variable]: node };
              }
            }
          }
        }
        break;
      }
      case 'CreateRel': {
        if (!adapter.createRelationship)
          throw new Error('Adapter does not support CREATE');
        let startNode = (plan.start.variable
          ? (vars.get(plan.start.variable) as NodeRecord | undefined)
          : undefined);
        if (!startNode) {
          if (!adapter.createNode)
            throw new Error('Adapter does not support CREATE');
          startNode = await adapter.createNode(
            plan.start.labels ?? [],
            evalProps(plan.start.properties ?? {}, vars, params)
          );
          vars.set(plan.start.variable, startNode);
        }
        let endNode = (plan.end.variable
          ? (vars.get(plan.end.variable) as NodeRecord | undefined)
          : undefined);
        if (!endNode) {
          if (!adapter.createNode)
            throw new Error('Adapter does not support CREATE');
          endNode = await adapter.createNode(
            plan.end.labels ?? [],
            evalProps(plan.end.properties ?? {}, vars, params)
          );
          vars.set(plan.end.variable, endNode);
        }
        const rel = await adapter.createRelationship(
          plan.relType,
          startNode.id,
          endNode.id,
          evalProps(plan.relProperties ?? {}, vars, params)
        );
        if (plan.returnVariable && plan.relVariable) {
          vars.set(plan.relVariable, rel);
          yield { [plan.relVariable]: rel };
        }
        break;
      }
      case 'MergeRel': {
        if (
          !adapter.scanRelationships ||
          !adapter.createRelationship ||
          !adapter.findNode ||
          !adapter.createNode
        )
          throw new Error('Adapter does not support MERGE');

        let startNode: NodeRecord | null | undefined =
          plan.start.variable
            ? (vars.get(plan.start.variable) as NodeRecord | undefined)
            : undefined;
        if (!startNode) {
          startNode = await adapter.findNode(
            plan.start.labels ?? [],
            evalProps(plan.start.properties ?? {}, vars, params)
          );
          if (!startNode) {
            startNode = await adapter.createNode(
              plan.start.labels ?? [],
              evalProps(plan.start.properties ?? {}, vars, params)
            );
          }
          if (plan.start.variable) vars.set(plan.start.variable, startNode);
        }

        let endNode: NodeRecord | null | undefined =
          plan.end.variable
            ? (vars.get(plan.end.variable) as NodeRecord | undefined)
            : undefined;
        if (!endNode) {
          endNode = await adapter.findNode(
            plan.end.labels ?? [],
            evalProps(plan.end.properties ?? {}, vars, params)
          );
          if (!endNode) {
            endNode = await adapter.createNode(
              plan.end.labels ?? [],
              evalProps(plan.end.properties ?? {}, vars, params)
            );
          }
          if (plan.end.variable) vars.set(plan.end.variable, endNode);
        }
        const sNode = startNode as NodeRecord;
        const eNode = endNode as NodeRecord;
        let existing: RelRecord | null = null;
        for await (const rel of adapter.scanRelationships()) {
          if (
            rel.type === plan.relType &&
            rel.startNode === sNode.id &&
            rel.endNode === eNode.id
          ) {
            let ok = true;
            if (plan.relProperties) {
              for (const [k, v] of Object.entries(plan.relProperties)) {
                if (rel.properties[k] !== evalPropValue(v, vars, params)) {
                  ok = false;
                  break;
                }
              }
            }
            if (ok) {
              existing = rel;
              break;
            }
          }
        }
        let created = false;
        if (!existing) {
          existing = await adapter.createRelationship(
            plan.relType,
            sNode.id,
            eNode.id,
            evalProps(plan.relProperties ?? {}, vars, params)
          );
          created = true;
        }
        if (plan.relVariable) vars.set(plan.relVariable, existing);
        if (created && plan.onCreateSet && adapter.updateRelationshipProperties) {
          for (const [k, expr] of Object.entries(plan.onCreateSet)) {
            const val = evalExpr(expr, vars, params);
            await adapter.updateRelationshipProperties(existing.id, { [k]: val });
            existing.properties[k] = val;
          }
        }
        if (!created && plan.onMatchSet && adapter.updateRelationshipProperties) {
          for (const [k, expr] of Object.entries(plan.onMatchSet)) {
            const val = evalExpr(expr, vars, params);
            await adapter.updateRelationshipProperties(existing.id, { [k]: val });
            existing.properties[k] = val;
          }
        }
        if (plan.returnVariable && plan.relVariable) {
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
        const rows: { row: Record<string, unknown>; order?: any }[] = [];
        for (const s of starts) {
          for (const e of ends) {
            for await (const path of findPaths(
              adapter,
              s.id,
              e.id,
              plan.minHops ?? 1,
              plan.maxHops ?? Infinity,
              plan.direction ?? 'out',
              plan.relType
            )) {
              vars.set(plan.pathVariable, path);
              const local = new Map(vars);
              if (plan.start.variable) local.set(plan.start.variable, s);
              if (plan.end.variable) local.set(plan.end.variable, e);
              if (!plan.returnItems) {
                return;
              }
              const row: Record<string, unknown> = {};
              const aliasVars = new Map(local);
              plan.returnItems.forEach((item, idx) => {
                const alias =
                  item.alias ||
                  (item.expression.type === 'Variable'
                    ? item.expression.name
                    : plan.returnItems!.length === 1
                    ? 'value'
                    : `value${idx}`);
                const val = evalExpr(item.expression, aliasVars, params);
                row[alias] = val;
                if (item.alias) aliasVars.set(item.alias, val);
              });
              const order = plan.orderBy
                ? plan.orderBy.map(o => evalExpr(o.expression, aliasVars, params))
                : undefined;
              rows.push({ row, order });
            }
          }
        }
        if (plan.returnItems) {
          if (plan.distinct) {
            const seen = new Set<string>();
            const uniq: typeof rows = [];
            for (const r of rows) {
              const key = JSON.stringify(r.row);
              if (!seen.has(key)) {
                seen.add(key);
                uniq.push(r);
              }
            }
            rows.splice(0, rows.length, ...uniq);
          }
          if (plan.orderBy) {
            rows.sort((a, b) => {
              for (let i = 0; i < plan.orderBy!.length; i++) {
                const av = a.order ? a.order[i] : undefined;
                const bv = b.order ? b.order[i] : undefined;
                if (av === bv) continue;
                if (av === undefined) return 1;
                if (bv === undefined) return -1;
                let cmp = av > bv ? 1 : -1;
                if (plan.orderBy![i].direction === 'DESC') cmp = -cmp;
                return cmp;
              }
              return 0;
            });
          }
          const startIdx = plan.skip ? Number(evalExpr(plan.skip, vars, params)) : 0;
          let endIdx = rows.length;
          if (plan.limit !== undefined) endIdx = Math.min(endIdx, startIdx + Number(evalExpr(plan.limit, vars, params)));
          for (let i = startIdx; i < endIdx; i++) {
            yield rows[i].row;
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
        const hasAggItem: boolean[] = plan.returnItems.map(r => hasAgg(r.expression));
        const hasAggFlag: boolean = hasAggItem.some(v => v);
        const rows: { row: Record<string, unknown>; order?: any }[] = [];
        const groups = new Map<string, { row: Record<string, unknown>; aggs: (AggState | null)[] }>();
        const startNodes: NodeRecord[] = [];
        const boundStart = vars.get(plan.start.variable) as NodeRecord | undefined;
        if (boundStart) {
          startNodes.push(boundStart);
        } else {
          for await (const node of adapter.scanNodes(
            plan.start.labels ? { labels: plan.start.labels } : {}
          )) {
            let ok = true;
            if (plan.start.properties) {
              for (const [k, v] of Object.entries(plan.start.properties)) {
                if (node.properties[k] !== evalPropValue(v, vars, params)) {
                  ok = false;
                  break;
                }
              }
            }
            if (ok) startNodes.push(node);
          }
        }
        const traverse = async (
          node: NodeRecord,
          hop: number,
          varsLocal: Map<string, any>,
          path: { nodes: NodeRecord[]; relationships: RelRecord[] }
        ): Promise<void> => {
          if (hop >= plan.hops.length) {
            const aliasVars = new Map(varsLocal);
            if (plan.pathVariable) aliasVars.set(plan.pathVariable, path);
            if (plan.where && !evalWhere(plan.where, aliasVars, params)) {
              return;
            }
            if (hasAggFlag) {
              const keyParts: unknown[] = [];
              let group = null;
              let i = 0;
              for (let idx = 0; idx < plan.returnItems.length; idx++) {
                const item = plan.returnItems[idx];
                if (!hasAggItem[idx]) {
                  const val = evalExpr(item.expression, aliasVars, params);
                  keyParts.push(val);
                }
              }
              const key = JSON.stringify(keyParts);
              group = groups.get(key);
              if (!group) {
                const row: Record<string, unknown> = {};
                let j = 0;
                plan.returnItems.forEach((item, idx) => {
                  if (!hasAggItem[idx]) {
                    const val = keyParts[j++];
                    row[aliasFor(item, idx)] = val;
                  }
                });
                group = { row, aggs: [] };
                groups.set(key, group);
              }
              plan.returnItems.forEach((item, idx) => {
                if (!hasAggItem[idx]) return;
                const current = (group!.aggs[idx] = group!.aggs[idx] ?? initAggState(item.expression));
                updateAggState(item.expression, current, aliasVars, params);
              });
            } else {
              const row: Record<string, unknown> = {};
              plan.returnItems.forEach((item, idx) => {
                if (item.expression.type === 'All') {
                  for (const [k, v] of aliasVars.entries()) {
                    row[k] = v;
                  }
                } else {
                  const val = evalExpr(item.expression, aliasVars, params);
                  row[aliasFor(item, idx)] = val;
                  if (item.alias) aliasVars.set(item.alias, val);
                }
              });
              const order = plan.orderBy
                ? plan.orderBy.map(o => evalExpr(o.expression, aliasVars, params))
                : undefined;
              rows.push({ row, order });
            }
            return;
          }
          const step = plan.hops[hop];
          for await (const rel of scanRels()) {
            if (step.rel.type && rel.type !== step.rel.type) continue;
            const nextIds: Array<number | string> = [];
            if (step.rel.direction === 'out') {
              if (rel.startNode !== node.id) continue;
              nextIds.push(rel.endNode);
            } else if (step.rel.direction === 'in') {
              if (rel.endNode !== node.id) continue;
              nextIds.push(rel.startNode);
            } else {
              if (rel.startNode === node.id) nextIds.push(rel.endNode);
              if (rel.endNode === node.id && rel.endNode !== rel.startNode)
                nextIds.push(rel.startNode);
              if (nextIds.length === 0) continue;
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
            for (const nextId of nextIds) {
              const nextNode = await getNode(nextId);
              if (!nextNode) continue;
              if (
                step.node.labels &&
                !step.node.labels.every(l => nextNode.labels.includes(l))
              )
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
              await traverse(nextNode, hop + 1, varsNext, {
                nodes: [...path.nodes, nextNode],
                relationships: [...path.relationships, rel],
              });
            }
          }
        };
        for (const s of startNodes) {
          const varsStart = new Map(vars);
          varsStart.set(plan.start.variable, s);
          await traverse(s, 0, varsStart, { nodes: [s], relationships: [] });
        }

        if (hasAggFlag) {
          if (groups.size === 0 && hasAggItem.every(v => v)) {
            groups.set('__empty__', { row: {}, aggs: [] });
          }
          for (const group of groups.values()) {
            const aliasVars = new Map(vars);
            plan.returnItems.forEach((item, idx) => {
              const alias = aliasFor(item, idx);
              if (hasAggItem[idx]) {
                group.row[alias] = finalizeAgg(
                  item.expression,
                  group.aggs[idx],
                  aliasVars,
                  params
                );
              }
              aliasVars.set(alias, group.row[alias]);
              if (item.alias) aliasVars.set(item.alias, group.row[alias]);
            });
            const order = plan.orderBy
              ? plan.orderBy.map(o => evalExpr(o.expression, aliasVars, params))
              : undefined;
            rows.push({ row: group.row, order });
          }
        }

        if (plan.distinct) {
          const seen = new Set<string>();
          const unique: typeof rows = [];
          for (const r of rows) {
            const key = JSON.stringify(r.row);
            if (!seen.has(key)) {
              seen.add(key);
              unique.push(r);
            }
          }
          rows.splice(0, rows.length, ...unique);
        }

        if (plan.orderBy) {
          rows.sort((a, b) => {
            for (let i = 0; i < plan.orderBy!.length; i++) {
              const av = a.order ? a.order[i] : undefined;
              const bv = b.order ? b.order[i] : undefined;
              if (av === bv) continue;
              if (av === undefined) return 1;
              if (bv === undefined) return -1;
              let cmp = av > bv ? 1 : -1;
              if (plan.orderBy![i].direction === 'DESC') cmp = -cmp;
              return cmp;
            }
            return 0;
          });
        }
        const startIdx = plan.skip ? Number(evalExpr(plan.skip, vars, params)) : 0;
        let end = rows.length;
        if (plan.limit !== undefined)
          end = Math.min(end, startIdx + Number(evalExpr(plan.limit, vars, params)));
        if (rows.length === 0 && plan.optional) {
          const local = new Map(vars);
          local.delete(plan.start.variable);
          for (const h of plan.hops) {
            if (h.rel.variable) local.delete(h.rel.variable);
            local.delete(h.node.variable);
          }
          if (plan.pathVariable) local.delete(plan.pathVariable);
          const row: Record<string, unknown> = {};
          plan.returnItems.forEach((item, idx) => {
            if (hasAggItem[idx]) {
              row[aliasFor(item, idx)] = finalizeAgg(
                item.expression,
                initAggState(item.expression),
                local,
                params
              );
            } else {
              row[aliasFor(item, idx)] = evalExpr(item.expression, local, params);
            }
          });
          yield row;
        } else {
          for (let i = startIdx; i < end; i++) {
            yield rows[i].row;
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
          const v = evalExpr(plan.list, vars, params);
          items = Array.isArray(v) ? v : [];
        }
        for (const item of items) {
          vars.set(plan.variable, item);
          for await (const row of innerPlan(vars, params)) {
            yield row;
          }
        }
        vars.delete(plan.variable);
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
          const alias =
            plan.returnAlias ??
            (plan.returnExpression.type === 'Variable' &&
            plan.returnExpression.name === plan.variable
              ? plan.variable
              : 'value');
          yield { [alias]: val };
        }
        break;
      }
      case 'With': {
        const left = logicalToPhysical(plan.source, adapter);
        const right = logicalToPhysical(plan.next, adapter);
        const aliasFor = (item: typeof plan.source.returnItems[number], idx: number): string => {
          if (item.alias) return item.alias;
          if (item.expression.type === 'Variable') return item.expression.name;
          return plan.source.returnItems.length === 1 ? 'value' : `value${idx}`;
        };
        for await (const row of left(new Map(vars), params)) {
          const local = new Map(vars);
          plan.source.returnItems.forEach((item, idx) => {
            if (item.expression.type === 'All') {
              for (const [k, v] of Object.entries(row as any)) {
                local.set(k, v);
              }
            } else {
              const alias = aliasFor(item, idx);
              local.set(alias, (row as any)[alias]);
            }
          });
          if (plan.where && !evalWhere(plan.where, local, params)) {
            continue;
          }
          for await (const out of right(local, params)) {
            yield out;
          }
        }
        break;
      }
      case 'Union': {
        const left = logicalToPhysical(plan.left, adapter);
        const right = logicalToPhysical(plan.right, adapter);
        const seen = new Set<string>();
        for await (const row of left(new Map(vars), params)) {
          if (plan.all) {
            yield row;
          } else {
            const key = JSON.stringify(row);
            if (!seen.has(key)) {
              seen.add(key);
              yield row;
            }
          }
        }
        for await (const row of right(new Map(vars), params)) {
          if (plan.all) {
            yield row;
          } else {
            const key = JSON.stringify(row);
            if (!seen.has(key)) {
              seen.add(key);
              yield row;
            }
          }
        }
        break;
      }
      case 'Return': {
        const aliasFor = (item: typeof plan.returnItems[number], idx: number): string => {
          if (item.alias) return item.alias;
          if (item.expression.type === 'Variable') return item.expression.name;
          return plan.returnItems.length === 1 ? 'value' : `value${idx}`;
        };
        const row: Record<string, unknown> = {};
        const aliasVars = new Map(vars);
        plan.returnItems.forEach((item, idx) => {
          if (item.expression.type === 'All') {
            for (const [k, v] of vars.entries()) {
              row[k] = v;
              aliasVars.set(k, v);
            }
          } else {
            const val = evalExpr(item.expression, vars, params);
            const alias = aliasFor(item, idx);
            row[alias] = val;
            if (item.alias) aliasVars.set(item.alias, val);
          }
        });
        let include = true;
        const skip = plan.skip ? Number(evalExpr(plan.skip, vars, params)) : 0;
        let limit = plan.limit ? Number(evalExpr(plan.limit, vars, params)) : 1;
        if (skip > 0) include = false;
        if (include && limit > 0) {
          yield row;
          limit--;
        }
        break;
      }
      case 'Call': {
        const innerPlans = plan.subquery.map(q =>
          logicalToPhysical(astToLogical(q), adapter)
        );
        const local = new Map(vars);
        const outRows: Record<string, unknown>[] = [];
        for (let i = 0; i < innerPlans.length; i++) {
          const p = innerPlans[i];
          for await (const row of p(local, params)) {
            if (i === innerPlans.length - 1) {
              const varsWithRow = new Map(local);
              for (const [k, v] of Object.entries(row)) varsWithRow.set(k, v);
              const out: Record<string, unknown> = {};
              plan.returnItems.forEach((item, ridx) => {
                const alias =
                  item.alias ||
                  (item.expression.type === 'Variable'
                    ? item.expression.name
                    : plan.returnItems.length === 1
                    ? 'value'
                    : `value${ridx}`);
                out[alias] = evalExpr(item.expression, varsWithRow, params);
              });
              outRows.push(out);
            }
          }
        }
        let rowsToYield = outRows;
        if (plan.distinct) {
          const seen = new Set<string>();
          rowsToYield = [];
          for (const r of outRows) {
            const key = JSON.stringify(r);
            if (!seen.has(key)) {
              seen.add(key);
              rowsToYield.push(r);
            }
          }
        }

        if (plan.orderBy) {
          rowsToYield.sort((a, b) => {
            for (let i = 0; i < plan.orderBy!.length; i++) {
              const av = evalExpr(plan.orderBy![i].expression, new Map(Object.entries(a)), params);
              const bv = evalExpr(plan.orderBy![i].expression, new Map(Object.entries(b)), params);
              if (av === bv) continue;
              if (av === undefined) return 1;
              if (bv === undefined) return -1;
              let cmp = av > bv ? 1 : -1;
              if (plan.orderBy![i].direction === 'DESC') cmp = -cmp;
              return cmp;
            }
            return 0;
          });
        }

        let start = 0;
        if (plan.skip) start = Number(evalExpr(plan.skip, vars, params));
        let end = rowsToYield.length;
        if (plan.limit !== undefined) {
          end = Math.min(end, start + Number(evalExpr(plan.limit, vars, params)));
        }
        for (let i = start; i < end; i++) {
          yield rowsToYield[i];
        }
        break;
      }
      default:
        throw new Error('Query not supported in this MVP');
    }
  };
}
