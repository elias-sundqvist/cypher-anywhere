import {
  StorageAdapter,
  NodeRecord,
  RelRecord,
  NodeScanSpec,
  TransactionCtx,
  IndexMetadata,
  parseMany,
  MatchReturnQuery,
  MatchMultiReturnQuery,
  MatchChainQuery,
  ReturnQuery,
  UnwindQuery,
  Expression,
  CypherAST,
  UnionQuery,
  CallQuery
} from '@cypher-anywhere/core';
import type * as fsType from 'fs';
import initSqlJs, { Database } from 'sql.js';

// Declare Node's require for TypeScript without pulling in Node types
declare var require: (module: string) => unknown;

export interface Dataset {
  nodes: NodeRecord[];
  relationships: RelRecord[];
}

export interface SqlJsAdapterOptions {
  datasetPath?: string;
  dataset?: Dataset;
  indexes?: IndexMetadata[];
}

export class SqlJsAdapter implements StorageAdapter {
  private db!: Database;
  private ready: Promise<void>;
  private indexes: IndexMetadata[];

  constructor(options: SqlJsAdapterOptions) {
    this.indexes = options.indexes ?? [];
    this.ready = this.init(options);
  }

  private async init(options: SqlJsAdapterOptions): Promise<void> {
    const SQL = await initSqlJs();
    this.db = new SQL.Database();
    this.db.run(
      'CREATE TABLE nodes (id INTEGER PRIMARY KEY, labels TEXT, properties TEXT)'
    );
    this.db.run(
      'CREATE TABLE edges (id INTEGER PRIMARY KEY, type TEXT, startNode INTEGER, endNode INTEGER, properties TEXT)'
    );
    let data: Dataset | undefined = options.dataset;
    if (!data && options.datasetPath) {
      const fs = require('fs') as typeof fsType;
      const text = fs.readFileSync(options.datasetPath, 'utf8');
      data = JSON.parse(text);
    }
    if (!data) throw new Error('dataset or datasetPath must be provided');
    const insertNode = this.db.prepare(
      'INSERT INTO nodes VALUES (?, ?, ?)'
    );
    for (const n of data.nodes) {
      insertNode.run([n.id, JSON.stringify(n.labels), JSON.stringify(n.properties)]);
    }
    insertNode.free();
    const insertRel = this.db.prepare(
      'INSERT INTO edges VALUES (?, ?, ?, ?, ?)'
    );
    for (const r of data.relationships) {
      insertRel.run([
        r.id,
        r.type,
        r.startNode,
        r.endNode,
        JSON.stringify(r.properties),
      ]);
    }
    insertRel.free();
  }

  private async ensureReady(): Promise<void> {
    await this.ready;
  }

  private rowToNode(row: any): NodeRecord {
    return {
      id: row.id,
      labels: JSON.parse(row.labels),
      properties: JSON.parse(row.properties || '{}'),
    };
  }

  private rowToRel(row: any): RelRecord {
    return {
      id: row.id,
      type: row.type,
      startNode: row.startNode,
      endNode: row.endNode,
      properties: JSON.parse(row.properties || '{}'),
    };
  }

  async getNodeById(id: number | string): Promise<NodeRecord | null> {
    await this.ensureReady();
    const stmt = this.db.prepare(
      'SELECT id, labels, properties FROM nodes WHERE id = ?'
    );
    stmt.bind([id]);
    const row = stmt.step() ? stmt.getAsObject() : null;
    stmt.free();
    return row ? this.rowToNode(row) : null;
  }

  async *scanNodes(spec: NodeScanSpec = {}): AsyncIterable<NodeRecord> {
    await this.ensureReady();
    const stmt = this.db.prepare(
      'SELECT id, labels, properties FROM nodes'
    );
    while (stmt.step()) {
      const row = this.rowToNode(stmt.getAsObject());
      const { label, labels } = spec;
      const labelMatch = label ? row.labels.includes(label) : true;
      const labelsMatch = labels ? labels.every(l => row.labels.includes(l)) : true;
      if (labelMatch && labelsMatch) {
        yield row;
      }
    }
    stmt.free();
  }

  async createNode(labels: string[], properties: Record<string, unknown>): Promise<NodeRecord> {
    await this.ensureReady();
    const stmt = this.db.prepare('SELECT MAX(id) as id FROM nodes');
    const row = stmt.step() ? stmt.getAsObject() : { id: 0 };
    stmt.free();
    const id = Number(row.id) + 1;
    const node: NodeRecord = { id, labels, properties };
    this.db.run('INSERT INTO nodes VALUES (?, ?, ?)', [id, JSON.stringify(labels), JSON.stringify(properties)]);
    return node;
  }

  async deleteNode(id: number | string): Promise<void> {
    await this.ensureReady();
    this.db.run('DELETE FROM edges WHERE startNode = ? OR endNode = ?', [id, id]);
    this.db.run('DELETE FROM nodes WHERE id = ?', [id]);
  }

  async updateNodeProperties(id: number | string, properties: Record<string, unknown>): Promise<void> {
    await this.ensureReady();
    const node = await this.getNodeById(id);
    if (!node) throw new Error('node not found');
    Object.assign(node.properties, properties);
    this.db.run('UPDATE nodes SET properties = ? WHERE id = ?', [JSON.stringify(node.properties), id]);
  }

  async findNode(labels: string[], properties: Record<string, unknown>): Promise<NodeRecord | null> {
    await this.ensureReady();
    const stmt = this.db.prepare('SELECT id, labels, properties FROM nodes');
    while (stmt.step()) {
      const row = this.rowToNode(stmt.getAsObject());
      if (labels.length && !labels.every(l => row.labels.includes(l))) continue;
      let ok = true;
      for (const [k, v] of Object.entries(properties)) {
        if (row.properties[k] !== v) {
          ok = false;
          break;
        }
      }
      if (ok) {
        stmt.free();
        return row;
      }
    }
    stmt.free();
    return null;
  }

  async *indexLookup(label: string | undefined, property: string, value: unknown): AsyncIterable<NodeRecord> {
    await this.ensureReady();
    const stmt = this.db.prepare('SELECT id, labels, properties FROM nodes');
    while (stmt.step()) {
      const row = this.rowToNode(stmt.getAsObject());
      if (label && !row.labels.includes(label)) continue;
      if (row.properties[property] === value) {
        yield row;
      }
    }
    stmt.free();
  }

  async listIndexes(): Promise<IndexMetadata[]> {
    await this.ensureReady();
    return this.indexes;
  }

  async getRelationshipById(id: number | string): Promise<RelRecord | null> {
    await this.ensureReady();
    const stmt = this.db.prepare(
      'SELECT id, type, startNode, endNode, properties FROM edges WHERE id = ?'
    );
    stmt.bind([id]);
    const row = stmt.step() ? stmt.getAsObject() : null;
    stmt.free();
    return row ? this.rowToRel(row) : null;
  }

  async *scanRelationships(): AsyncIterable<RelRecord> {
    await this.ensureReady();
    const stmt = this.db.prepare(
      'SELECT id, type, startNode, endNode, properties FROM edges'
    );
    while (stmt.step()) {
      yield this.rowToRel(stmt.getAsObject());
    }
    stmt.free();
  }

  async createRelationship(type: string, startNode: number | string, endNode: number | string, properties: Record<string, unknown>): Promise<RelRecord> {
    await this.ensureReady();
    const stmt = this.db.prepare('SELECT MAX(id) as id FROM edges');
    const row = stmt.step() ? stmt.getAsObject() : { id: 0 };
    stmt.free();
    const id = Number(row.id) + 1;
    const rel: RelRecord = { id, type, startNode, endNode, properties };
    this.db.run('INSERT INTO edges VALUES (?, ?, ?, ?, ?)', [id, type, startNode, endNode, JSON.stringify(properties)]);
    return rel;
  }

  async deleteRelationship(id: number | string): Promise<void> {
    await this.ensureReady();
    this.db.run('DELETE FROM edges WHERE id = ?', [id]);
  }

  async updateRelationshipProperties(id: number | string, properties: Record<string, unknown>): Promise<void> {
    await this.ensureReady();
    const rel = await this.getRelationshipById(id);
    if (!rel) throw new Error('relationship not found');
    Object.assign(rel.properties, properties);
    this.db.run('UPDATE edges SET properties = ? WHERE id = ?', [JSON.stringify(rel.properties), id]);
  }

  async beginTransaction(): Promise<TransactionCtx> {
    await this.ensureReady();
    this.db.run('BEGIN TRANSACTION');
    return {};
  }

  async commit(_: TransactionCtx): Promise<void> {
    await this.ensureReady();
    this.db.run('COMMIT');
  }

  async rollback(_: TransactionCtx): Promise<void> {
    await this.ensureReady();
    this.db.run('ROLLBACK');
  }

  // Helper functions for evaluating expressions and aggregations similar to
  // the core engine's PhysicalPlan implementation. These are purposely scoped
  // to the features needed by transpiled queries.

  private evalExpr(
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
      case 'Variable': {
        const val = vars.get(expr.name);
        if (val && typeof val === 'object' && 'nodes' in val && 'relationships' in val)
          return (val as any).nodes;
        return val;
      }
      case 'Parameter':
        return params[expr.name];
      case 'Add':
        const la = this.evalExpr(expr.left, vars, params);
        const ra = this.evalExpr(expr.right, vars, params);
        if (la == null || ra == null) return null;
        if (typeof la === 'number' && typeof ra === 'number') return la + ra;
        return String(la) + String(ra);
      case 'Sub': {
        const l = this.evalExpr(expr.left, vars, params);
        const r = this.evalExpr(expr.right, vars, params);
        if (l == null || r == null) return null;
        if (typeof l === 'number' && typeof r === 'number') return l - r;
        return NaN;
      }
      case 'Mul': {
        const l = this.evalExpr(expr.left, vars, params);
        const r = this.evalExpr(expr.right, vars, params);
        if (l == null || r == null) return null;
        if (typeof l === 'number' && typeof r === 'number') return l * r;
        return NaN;
      }
      case 'Div': {
        const l = this.evalExpr(expr.left, vars, params);
        const r = this.evalExpr(expr.right, vars, params);
        if (l == null || r == null) return null;
        if (typeof l === 'number' && typeof r === 'number') return l / r;
        return NaN;
      }
      case 'Neg': {
        const v = this.evalExpr(expr.expression, vars, params);
        if (v == null) return null;
        return typeof v === 'number' ? -v : NaN;
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
      case 'Length': {
        let v: any;
        if (expr.expression.type === 'Variable') {
          v = vars.get(expr.expression.name);
        } else {
          v = this.evalExpr(expr.expression, vars, params);
        }
        if (v && typeof v === 'object' && 'relationships' in v) {
          return (v as any).relationships.length;
        }
        if (Array.isArray(v) || typeof v === 'string') return v.length;
        return undefined;
      }
      case 'All':
        return Object.fromEntries(vars.entries());
      default:
        throw new Error('Unknown expression');
    }
  }

  private hasAgg(expr: Expression): boolean {
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
        return this.hasAgg(expr.left) || this.hasAgg(expr.right);
      case 'Neg':
        return this.hasAgg(expr.expression);
      default:
        return false;
    }
  }

  private serializeVars(vars: Map<string, any>): string {
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

  private initAggState(expr: Expression): any | null {
    switch (expr.type) {
      case 'Count':
        return { type: 'Count', distinct: expr.distinct, expr: expr.expression, count: 0, values: new Set<string>() };
      case 'Sum':
      case 'Min':
      case 'Max':
        return { type: expr.type, distinct: expr.distinct, expr: expr.expression, sum: 0, min: undefined, max: undefined, values: new Set<string>() };
      case 'Avg':
        return { type: 'Avg', distinct: expr.distinct, expr: expr.expression, sum: 0, count: 0, values: new Set<string>() };
      case 'Collect':
        return { type: 'Collect', distinct: expr.distinct, expr: expr.expression, values: [] as unknown[], seen: new Set<string>() };
      case 'Add':
      case 'Sub':
      case 'Mul':
      case 'Div':
        const left = this.initAggState(expr.left);
        const right = this.initAggState(expr.right);
        return left || right ? { type: expr.type, left, right } : null;
      case 'Neg':
        const inner = this.initAggState(expr.expression);
        return inner ? { type: 'Neg', inner } : null;
      default:
        return null;
    }
  }

  private updateAggState(
    expr: Expression,
    state: any | null,
    vars: Map<string, any>,
    params: Record<string, any>
  ) {
    if (!state) return;
    switch (state.type) {
      case 'Count': {
        const val = state.expr ? this.evalExpr(state.expr, vars, params) : null;
        if (state.distinct) {
          const key = state.expr === null ? this.serializeVars(vars) : JSON.stringify(val);
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
        const v = this.evalExpr(state.expr, vars, params);
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
        const v = this.evalExpr(state.expr, vars, params);
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
        const v = this.evalExpr(state.expr, vars, params);
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
        const v = this.evalExpr(state.expr, vars, params);
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
        const v = this.evalExpr(state.expr, vars, params);
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
        this.updateAggState((expr as any).left, state.left, vars, params);
        this.updateAggState((expr as any).right, state.right, vars, params);
        break;
      case 'Neg':
        this.updateAggState((expr as any).expression, state.inner, vars, params);
        break;
    }
  }

  private finalizeAgg(
    expr: Expression,
    state: any | null,
    vars: Map<string, any>,
    params: Record<string, any>
  ): any {
    switch (expr.type) {
      case 'Add':
        return (
          this.finalizeAgg((expr as any).left, state ? state.left : null, vars, params) +
          this.finalizeAgg((expr as any).right, state ? state.right : null, vars, params)
        );
      case 'Sub':
        return (
          this.finalizeAgg((expr as any).left, state ? state.left : null, vars, params) -
          this.finalizeAgg((expr as any).right, state ? state.right : null, vars, params)
        );
      case 'Mul':
        return (
          this.finalizeAgg((expr as any).left, state ? state.left : null, vars, params) *
          this.finalizeAgg((expr as any).right, state ? state.right : null, vars, params)
        );
      case 'Div':
        return (
          this.finalizeAgg((expr as any).left, state ? state.left : null, vars, params) /
          this.finalizeAgg((expr as any).right, state ? state.right : null, vars, params)
        );
      case 'Neg':
        return -this.finalizeAgg((expr as any).expression, state ? state.inner : null, vars, params);
      case 'Count':
        return state ? state.count : 0;
      case 'Sum':
        return state ? state.sum ?? 0 : 0;
      case 'Min':
        return state ? state.min ?? null : null;
      case 'Max':
        return state ? state.max ?? null : null;
      case 'Avg':
        return state ? (state.count ? state.sum / state.count : null) : null;
      case 'Collect':
        return state ? state.values : [];
      default:
        return this.evalExpr(expr, vars, params);
    }
  }

  supportsTranspilation = true;

  transpile(
    cypher: string,
    params: Record<string, any> = {}
  ): { sql: string; params: any[] } | null {
    const asts = parseMany(cypher);
    if (asts.length !== 1) return null;
    return this.transpileAST(asts[0], params);
  }

  private transpileAST(
    ast: CypherAST,
    params: Record<string, any>
  ): { sql: string; params: any[] } | null {
    if (ast.type === 'MatchReturn') {
      return this.transpileMatchReturn(ast as MatchReturnQuery, params);
    }
    return null;
  }

  private transpileMatchReturn(
    matchAst: MatchReturnQuery,
    params: Record<string, any>
  ): { sql: string; params: any[] } | null {
    if (matchAst.isRelationship || matchAst.distinct) return null;

    const retExprs = matchAst.returnItems.map(i => i.expression);
    if (retExprs.some(e => e.type === 'Count') && retExprs.length > 1) return null;

    const allowMulti =
      retExprs.every(
        e => e.type === 'Property' && e.variable === matchAst.variable
      ) && retExprs.length > 1;

    if (!allowMulti && matchAst.returnItems.length !== 1) return null;

    const retExpr = matchAst.returnItems[0].expression;
    const isCount = retExpr.type === 'Count';
    if (isCount && (retExpr as any).distinct) return null;
    if (
      !isCount &&
      (matchAst.optional ||
        !(
          (retExpr.type === 'Variable' && retExpr.name === matchAst.variable) ||
          (retExpr.type === 'Property' && retExpr.variable === matchAst.variable) ||
          ((retExpr as any).type === 'Id' && (retExpr as any).variable === matchAst.variable)
        ))
    )
      return null;

    let sql = 'SELECT ';
    if (isCount) {
      sql += 'COUNT(*) AS value';
    } else if (allowMulti) {
      const parts: string[] = [];
      for (const item of matchAst.returnItems) {
        const alias = item.alias || (item.expression as any).property;
        parts.push(
          `json_extract(properties, '$.${(item.expression as any).property}') AS ${alias}`
        );
      }
      sql += parts.join(', ');
    } else if (retExpr.type === 'Variable') {
      sql += 'id, labels, properties';
    } else if ((retExpr as any).type === 'Id') {
      const alias = matchAst.returnItems[0].alias || 'value';
      sql += `id AS ${alias}`;
    } else {
      const alias = matchAst.returnItems[0].alias || 'value';
      sql += `json_extract(properties, '$.${(retExpr as any).property}') AS ${alias}`;
    }
    sql += ' FROM nodes';
    const paramsArr: any[] = [];
    const conds: string[] = [];
    if (matchAst.labels && matchAst.labels.length > 0) {
      for (const lbl of matchAst.labels) {
        conds.push('labels LIKE ?');
        paramsArr.push(`%"${lbl}"%`);
      }
    }
    if (matchAst.properties) {
      for (const [k, v] of Object.entries(matchAst.properties)) {
        const val =
          v && typeof v === 'object' && 'type' in v
            ? v.type === 'Literal'
              ? (v as any).value
              : v.type === 'Parameter'
              ? params[(v as any).name]
              : undefined
            : v;
        if (val === null) {
          conds.push(`json_extract(properties, '$.${k}') IS NULL`);
        } else {
          conds.push(`json_extract(properties, '$.${k}') = ?`);
          paramsArr.push(val);
        }
      }
    }

    function toValue(expr: any): any {
      if (!expr) return undefined;
      if (typeof expr === 'object' && 'type' in expr) {
        if (expr.type === 'Literal') return expr.value;
        if (expr.type === 'Parameter') return params[expr.name];
      }
      return expr;
    }

    function convertWhere(where: any): [string, any[]] | null {
      if (!where) return ['1', []];
      if (where.type === 'Condition') {
        let col: string | null = null;
        const left = where.left;
        let isId = false;
        if (left.type === 'Property' && left.variable === matchAst.variable) {
          col = `json_extract(properties, '$.${left.property}')`;
        } else if (left.type === 'Id' && left.variable === matchAst.variable) {
          col = 'id';
          isId = true;
        } else {
          return null;
        }
        switch (where.operator) {
          case '=':
          case '<>': {
            const val = toValue(where.right);
            if (val === undefined) return null;
            if (val === null) {
              return [
                `${col} ${where.operator === '=' ? 'IS' : 'IS NOT'} NULL`,
                [],
              ];
            }
            return [`${col} ${where.operator} ?`, [val]];
          }
          case '>':
          case '>=':
          case '<':
          case '<=': {
            const val = toValue(where.right);
            if (val === undefined) return null;
            return [`${col} ${where.operator} ?`, [val]];
          }
          case 'IN': {
            const val = toValue(where.right);
            if (!Array.isArray(val)) return null;
            if (val.length === 0) return ['0', []];
            const placeholders = val.map(() => '?').join(', ');
            return [`${col} IN (${placeholders})`, val];
          }
          case 'IS NULL':
            if (isId) return null;
            return [`${col} IS NULL`, []];
          case 'IS NOT NULL':
            if (isId) return null;
            return [`${col} IS NOT NULL`, []];
          case 'STARTS WITH': {
            if (isId) return null;
            const val = toValue(where.right);
            if (typeof val !== 'string') return null;
            return [`${col} LIKE ?`, [val + '%']];
          }
          case 'ENDS WITH': {
            if (isId) return null;
            const val = toValue(where.right);
            if (typeof val !== 'string') return null;
            return [`${col} LIKE ?`, ['%' + val]];
          }
          case 'CONTAINS': {
            if (isId) return null;
            const val = toValue(where.right);
            if (typeof val !== 'string') return null;
            return [`${col} LIKE ?`, ['%' + val + '%']];
          }
          default:
            return null;
        }
      }
      if (where.type === 'And' || where.type === 'Or') {
        const l = convertWhere(where.left);
        const r = convertWhere(where.right);
        if (!l || !r) return null;
        const op = where.type === 'And' ? 'AND' : 'OR';
        return [`(${l[0]} ${op} ${r[0]})`, [...l[1], ...r[1]]];
      }
      if (where.type === 'Not') {
        const inner = convertWhere(where.clause);
        if (!inner) return null;
        return [`NOT (${inner[0]})`, inner[1]];
      }
      return null;
    }

    const whereSql = convertWhere(matchAst.where);
    if (!whereSql) return null;
    if (whereSql[0] !== '1') {
      conds.push(whereSql[0]);
      paramsArr.push(...whereSql[1]);
    }
    if (conds.length > 0) sql += ' WHERE ' + conds.join(' AND ');

    if (matchAst.orderBy && matchAst.orderBy.length > 0) {
      const aliasMap = new Map<string, string>();
      if (allowMulti) {
        for (const item of matchAst.returnItems) {
          const a = item.alias || (item.expression as any).property;
          aliasMap.set(a, (item.expression as any).property);
        }
      } else if (retExpr.type === 'Property') {
        aliasMap.set(matchAst.returnItems[0].alias || 'value', retExpr.property);
      } else if ((retExpr as any).type === 'Id') {
        aliasMap.set(matchAst.returnItems[0].alias || 'value', 'id');
      }
      const orderParts: string[] = [];
      for (const ob of matchAst.orderBy) {
        let exprSql: string | null = null;
        if (ob.expression.type === 'Variable') {
          if (ob.expression.name === matchAst.variable) {
            exprSql = 'id';
          } else if (aliasMap.has(ob.expression.name)) {
            const prop = aliasMap.get(ob.expression.name)!;
            exprSql = prop === 'id' ? 'id' : `json_extract(properties, '$.${prop}')`;
          }
        } else if (
          ob.expression.type === 'Property' &&
          ob.expression.variable === matchAst.variable
        ) {
          exprSql = `json_extract(properties, '$.${ob.expression.property}')`;
        } else if (
          ob.expression.type === 'Id' &&
          ob.expression.variable === matchAst.variable
        ) {
          exprSql = 'id';
        }
        if (!exprSql) return null;
        const dir = ob.direction ? ' ' + ob.direction : '';
        orderParts.push(`${exprSql}${dir}`);
      }
      sql += ' ORDER BY ' + orderParts.join(', ');
    }

    if (matchAst.limit) {
      const lim = toValue(matchAst.limit);
      if (typeof lim !== 'number') return null;
      sql += ' LIMIT ' + lim;
    }
    if (matchAst.skip) {
      const off = toValue(matchAst.skip);
      if (typeof off !== 'number') return null;
      if (!matchAst.limit) sql += ' LIMIT -1';
      sql += ' OFFSET ' + off;
    }

    return { sql, params: paramsArr };
  }

  runTranspiled(
    cypher: string,
    params: Record<string, any>
  ): AsyncIterable<Record<string, unknown>> | null {
    const asts = parseMany(cypher);
    if (asts.length !== 1) return null;
    const ast = asts[0];
    const t = this.transpileAST(ast, params);
    if (!t) return null;
    const transpiled = t;
    if (ast.type !== 'MatchReturn') return null;
    const matchAst = ast as MatchReturnQuery;
    const retExprs = matchAst.returnItems.map(i => i.expression);
    const allowMulti =
      retExprs.every(e => e.type === 'Property' && e.variable === matchAst.variable) &&
      retExprs.length > 1;
    const self = this;
    async function* gen() {
      await self.ensureReady();
      const stmt = self.db.prepare(transpiled.sql);
      stmt.bind(transpiled.params);
      while (stmt.step()) {
        const row = stmt.getAsObject();
        const out: Record<string, unknown> = {};
        for (const item of matchAst.returnItems) {
          const alias =
            item.alias ||
            (allowMulti
              ? (item.expression as any).property
              : item.expression.type === 'Variable'
              ? item.expression.name
              : 'value');
          const col = alias;
          if (item.expression.type === 'Variable') {
            out[alias] = self.rowToNode(row);
          } else if (item.expression.type === 'Property') {
            out[alias] = row[col];
          } else if (item.expression.type === 'Id') {
            out[alias] = row[col];
          } else if (item.expression.type === 'Count') {
            out[alias] = row.value;
          }
        }
        yield out;
      }
      stmt.free();
    }
    return gen();
  }
}
