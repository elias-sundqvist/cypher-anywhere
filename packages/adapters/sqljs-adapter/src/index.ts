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
    if (
      matchAst.isRelationship ||
      matchAst.orderBy ||
      matchAst.skip ||
      matchAst.limit ||
      matchAst.distinct ||
      matchAst.returnItems.length !== 1 ||
      matchAst.returnItems[0].expression.type !== 'Variable' ||
      matchAst.returnItems[0].expression.name !== matchAst.variable
    )
      return null;

    let sql = 'SELECT id, labels, properties FROM nodes';
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
        if (where.left.type !== 'Property' || where.left.variable !== matchAst.variable)
          return null;
        const col = `json_extract(properties, '$.${where.left.property}')`;
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
            return [`${col} IS NULL`, []];
          case 'IS NOT NULL':
            return [`${col} IS NOT NULL`, []];
          case 'STARTS WITH': {
            const val = toValue(where.right);
            if (typeof val !== 'string') return null;
            return [`${col} LIKE ?`, [val + '%']];
          }
          case 'ENDS WITH': {
            const val = toValue(where.right);
            if (typeof val !== 'string') return null;
            return [`${col} LIKE ?`, ['%' + val]];
          }
          case 'CONTAINS': {
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
    return { sql, params: paramsArr };
  }

  private runTranspiledAST(
    ast: CypherAST,
    params: Record<string, any>
  ): AsyncIterable<Record<string, unknown>> | null {
    if (ast.type === 'MatchReturn') {
      const matchAst = ast as MatchReturnQuery;
    // allow multi-label node matches by requiring each label to be present
    if (matchAst.isRelationship) {
      if (matchAst.orderBy || matchAst.skip || matchAst.limit || matchAst.distinct)
        return null;
      function checkRelExpr(expr: Expression): boolean {
        switch (expr.type) {
          case 'Variable':
            return expr.name === matchAst.variable;
          case 'Property':
          case 'Id':
          case 'Labels':
            return expr.variable === matchAst.variable;
          case 'Type':
            return expr.variable === matchAst.variable;
          case 'Literal':
          case 'Parameter':
            return true;
          default:
            return false;
        }
      }
      for (const ri of matchAst.returnItems) if (!checkRelExpr(ri.expression)) return null;
      const self = this;
      async function* genRel() {
        await self.ensureReady();
        let sql = 'SELECT id, type, startNode, endNode, properties FROM edges';
        const paramsArr: any[] = [];
        const conds: string[] = [];
        if (matchAst.labels && matchAst.labels.length === 1) {
          conds.push('type = ?');
          paramsArr.push(matchAst.labels[0]);
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

        function toVal(expr: any): any {
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
            if (where.left.type !== 'Property' || where.left.variable !== matchAst.variable)
              return null;
            const col = `json_extract(properties, '$.${where.left.property}')`;
            switch (where.operator) {
              case '=':
              case '<>': {
                const val = toVal(where.right);
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
                const val = toVal(where.right);
                if (val === undefined) return null;
                return [`${col} ${where.operator} ?`, [val]];
              }
              case 'IN': {
                const val = toVal(where.right);
                if (!Array.isArray(val)) return null;
                if (val.length === 0) return ['0', []];
                const placeholders = val.map(() => '?').join(', ');
                return [`${col} IN (${placeholders})`, val];
              }
              case 'IS NULL':
                return [`${col} IS NULL`, []];
              case 'IS NOT NULL':
                return [`${col} IS NOT NULL`, []];
              case 'STARTS WITH': {
                const val = toVal(where.right);
                if (typeof val !== 'string') return null;
                return [`${col} LIKE ?`, [val + '%']];
              }
              case 'ENDS WITH': {
                const val = toVal(where.right);
                if (typeof val !== 'string') return null;
                return [`${col} LIKE ?`, ['%' + val]];
              }
              case 'CONTAINS': {
                const val = toVal(where.right);
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
        const stmt = self.db.prepare(sql);
        stmt.bind(paramsArr);
        while (stmt.step()) {
          const rel = self.rowToRel(stmt.getAsObject());
          const vars = new Map<string, any>();
          vars.set(matchAst.variable, rel);
          const row: Record<string, any> = {};
          matchAst.returnItems.forEach((item, idx) => {
            const alias = item.alias || (item.expression.type === 'Variable' ? item.expression.name : `value${idx}`);
            row[alias] = self.evalExpr(item.expression, vars, params);
          });
          yield row;
        }
        stmt.free();
      }
      return genRel();
    }
    const isOptional = matchAst.optional;
    function checkExpr(expr: Expression): boolean {
      switch (expr.type) {
        case 'Variable':
          return expr.name === matchAst.variable;
        case 'Property':
          return expr.variable === matchAst.variable;
        case 'Id':
        case 'Labels':
          return expr.variable === matchAst.variable;
        case 'Literal':
        case 'Parameter':
          return true;
        case 'Add':
        case 'Sub':
        case 'Mul':
        case 'Div':
          return checkExpr(expr.left) && checkExpr(expr.right);
        case 'Neg':
          return checkExpr(expr.expression);
        case 'Count':
        case 'Sum':
        case 'Min':
        case 'Max':
        case 'Avg':
        case 'Collect':
          return expr.expression ? checkExpr(expr.expression) : true;
        case 'Length':
          return checkExpr(expr.expression);
        case 'All':
          return matchAst.returnItems.length === 1;
        default:
          return false;
      }
    }

    for (const ri of matchAst.returnItems) {
      if (!checkExpr(ri.expression)) return null;
    }
    const aliasMap = new Map<string, any>();
    if (
      matchAst.returnItems.length === 1 &&
      matchAst.returnItems[0].expression.type === 'All'
    ) {
      const name = matchAst.returnItems[0].alias || matchAst.variable;
      aliasMap.set(name, { type: 'Variable', name: matchAst.variable });
    } else {
      for (const ri of matchAst.returnItems) {
        const expr = ri.expression as any;
        const name =
          ri.alias ||
          (expr.type === 'Variable'
            ? expr.name
            : expr.type === 'Property'
            ? expr.property
            : expr.type === 'Id'
            ? 'id'
            : expr.type === 'Labels'
            ? 'labels'
            : 'val');
        aliasMap.set(name, ri.expression);
      }
    }

    let sql = 'SELECT id, labels, properties FROM nodes';
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
        if (where.left.type !== 'Property' || where.left.variable !== matchAst.variable)
          return null;
        const col = `json_extract(properties, '$.${where.left.property}')`;
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
            return [`${col} IS NULL`, []];
          case 'IS NOT NULL':
            return [`${col} IS NOT NULL`, []];
          case 'STARTS WITH': {
            const val = toValue(where.right);
            if (typeof val !== 'string') return null;
            return [`${col} LIKE ?`, [val + '%']];
          }
          case 'ENDS WITH': {
            const val = toValue(where.right);
            if (typeof val !== 'string') return null;
            return [`${col} LIKE ?`, ['%' + val]];
          }
          case 'CONTAINS': {
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
    const self = this;
    async function* gen() {
      await self.ensureReady();
      const stmt = self.db.prepare(sql);
      stmt.bind(paramsArr);
      const results: NodeRecord[] = [];
      while (stmt.step()) {
        const node = self.rowToNode(stmt.getAsObject());
        results.push(node);
      }
      stmt.free();

      const hasAggItem = matchAst.returnItems.map(r => self.hasAgg(r.expression));
      const hasAgg = hasAggItem.some(v => v);
      const aliasFor = (ri: typeof matchAst.returnItems[number], idx: number): string => {
        if (ri.alias) return ri.alias;
        if (ri.expression.type === 'Variable') return ri.expression.name;
        return matchAst.returnItems.length === 1 ? 'value' : `value${idx}`;
      };

      let rows: { row: Record<string, any>; order?: any[] }[] = [];
      if (hasAgg) {
        const groups = new Map<string, { row: Record<string, any>; aggs: any[]; record?: NodeRecord }>();
        const vars = new Map<string, any>();
        for (const node of results) {
          vars.set(matchAst.variable, node);
          const keyParts: any[] = [];
          for (let i = 0; i < matchAst.returnItems.length; i++) {
            if (!hasAggItem[i]) keyParts.push(self.evalExpr(matchAst.returnItems[i].expression, vars, params));
          }
          const key = JSON.stringify(keyParts);
          let group = groups.get(key);
          if (!group) {
            const row: Record<string, any> = {};
            let j = 0;
            matchAst.returnItems.forEach((item, idx) => {
              if (!hasAggItem[idx]) {
                row[aliasFor(item, idx)] = keyParts[j++];
              }
            });
            group = { row, aggs: [], record: node };
            groups.set(key, group);
          }
          matchAst.returnItems.forEach((item, idx) => {
            if (!hasAggItem[idx]) return;
            const cur = group!.aggs[idx] = group!.aggs[idx] ?? self.initAggState(item.expression);
            self.updateAggState(item.expression, cur, vars, params);
          });
        }
        if (groups.size === 0 && hasAggItem.every(v => v)) {
          groups.set('__empty__', { row: {}, aggs: [] });
        }
        for (const group of groups.values()) {
          const localVars = new Map<string, any>();
          if (group.record) localVars.set(matchAst.variable, group.record);
          matchAst.returnItems.forEach((item, idx) => {
            if (!hasAggItem[idx]) return;
            group.row[aliasFor(item, idx)] = self.finalizeAgg(
              item.expression,
              group.aggs[idx],
              localVars,
              params
            );
          });
          let order: any[] | undefined;
          if (matchAst.orderBy) {
            const aliasVars = new Map(localVars);
            matchAst.returnItems.forEach((it, i) => {
              const alias = aliasFor(it, i);
              aliasVars.set(alias, group.row[alias]);
              if (it.alias) aliasVars.set(it.alias, group.row[alias]);
            });
            order = matchAst.orderBy.map(o => self.evalExpr(o.expression, aliasVars, params));
          }
          rows.push({ row: group.row, order });
        }
      } else {
        for (const node of results) {
          const vars = new Map<string, any>();
          vars.set(matchAst.variable, node);
          const row: Record<string, any> = {};
          const aliasVars = new Map(vars);
          matchAst.returnItems.forEach((item, idx) => {
            const alias = aliasFor(item, idx);
            if (item.expression.type === 'All') {
              for (const [k, v] of vars.entries()) {
                row[k] = v;
                aliasVars.set(k, v);
              }
            } else {
              const val = self.evalExpr(item.expression, vars, params);
              row[alias] = val;
              if (item.alias) aliasVars.set(item.alias, val);
            }
          });
          const order = matchAst.orderBy
            ? matchAst.orderBy.map(o => self.evalExpr(o.expression, aliasVars, params))
            : undefined;
          rows.push({ row, order });
        }
      }

      if (rows.length === 0 && isOptional && !hasAgg) {
        const empty: Record<string, any> = {};
        matchAst.returnItems.forEach((item, idx) => {
          const alias = aliasFor(item, idx);
          if (item.expression.type === 'All') {
            empty[matchAst.variable] = undefined;
          } else {
            empty[alias] = undefined;
          }
        });
        rows.push({ row: empty });
      }

      if (matchAst.distinct) {
        const seen = new Set<string>();
        const uniq: typeof rows = [];
        for (const r of rows) {
          const key = JSON.stringify(r.row);
          if (!seen.has(key)) {
            seen.add(key);
            uniq.push(r);
          }
        }
        rows = uniq;
      }

      if (matchAst.orderBy && matchAst.orderBy.length > 0) {
        rows.sort((a, b) => {
          for (let i = 0; i < matchAst.orderBy!.length; i++) {
            const av = a.order ? a.order[i] : undefined;
            const bv = b.order ? b.order[i] : undefined;
            if (av === bv) continue;
            if (av === undefined) return 1;
            if (bv === undefined) return -1;
            let cmp = av > bv ? 1 : -1;
            if (matchAst.orderBy![i].direction === 'DESC') cmp = -cmp;
            return cmp;
          }
          return 0;
        });
      }

      let start = 0;
      if (matchAst.skip) {
        const v = toValue(matchAst.skip);
        start = typeof v === 'number' ? v : 0;
      }
      let end: number | undefined = undefined;
      if (matchAst.limit) {
        const v = toValue(matchAst.limit);
        if (typeof v === 'number') {
          end = start + v;
        }
      }
      const slice = rows.slice(start, end);
      for (const r of slice) {
        yield r.row;
      }
    }
      return gen();
    }

    if (ast.type === 'MatchMultiReturn') {
      const multi = ast as MatchMultiReturnQuery;
      const isOptional = multi.optional;
      // allow multi-label patterns in MATCH by checking all labels
      const vars = multi.patterns.map(p => p.variable);
      function checkExpr(expr: Expression): boolean {
        switch (expr.type) {
          case 'Variable':
            return vars.includes(expr.name);
          case 'Property':
          case 'Id':
          case 'Labels':
          case 'Type':
            return vars.includes(expr.variable);
          case 'Literal':
          case 'Parameter':
            return true;
          case 'Add':
          case 'Sub':
          case 'Mul':
          case 'Div':
            return checkExpr(expr.left) && checkExpr(expr.right);
          case 'Neg':
            return checkExpr(expr.expression);
          case 'Count':
          case 'Sum':
          case 'Min':
          case 'Max':
          case 'Avg':
        case 'Collect':
          return expr.expression ? checkExpr(expr.expression) : true;
        case 'Length':
          return checkExpr(expr.expression);
        case 'All':
          return multi.returnItems.length === 1;
        default:
          return false;
      }
      }

      for (const ri of multi.returnItems) {
        if (!checkExpr(ri.expression)) return null;
      }

      const self = this;
      async function fetch(p: typeof multi.patterns[number]): Promise<NodeRecord[]> {
        let sql = 'SELECT id, labels, properties FROM nodes';
        const paramsArr: any[] = [];
        const conds: string[] = [];
        if (p.labels && p.labels.length > 0) {
          for (const lbl of p.labels) {
            conds.push('labels LIKE ?');
            paramsArr.push(`%"${lbl}"%`);
          }
        }
        if (p.properties) {
          for (const [k, v] of Object.entries(p.properties)) {
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
        if (conds.length > 0) sql += ' WHERE ' + conds.join(' AND ');
        await self.ensureReady();
        const stmt = self.db.prepare(sql);
        stmt.bind(paramsArr);
        const res: NodeRecord[] = [];
        while (stmt.step()) res.push(self.rowToNode(stmt.getAsObject()));
        stmt.free();
        return res;
      }

      function evalWhere(where: any, map: Map<string, any>): boolean {
        if (!where) return true;
        if (where.type === 'Condition') {
          const left = self.evalExpr(where.left, map, params);
          const right = where.right ? self.evalExpr(where.right, map, params) : undefined;
          switch (where.operator) {
            case '=':
              return left === right;
            case '<>':
              return left !== right;
            case '>':
              return left > right;
            case '>=':
              return left >= right;
            case '<':
              return left < right;
            case '<=':
              return left <= right;
            case 'IN':
              return Array.isArray(right) && right.includes(left);
            case 'IS NULL':
              return left === null || left === undefined;
            case 'IS NOT NULL':
              return left !== null && left !== undefined;
            case 'STARTS WITH':
              return typeof left === 'string' && typeof right === 'string' && left.startsWith(right);
            case 'ENDS WITH':
              return typeof left === 'string' && typeof right === 'string' && left.endsWith(right);
            case 'CONTAINS':
              return typeof left === 'string' && typeof right === 'string' && left.includes(right);
            default:
              return false;
          }
        }
        if (where.type === 'And') return evalWhere(where.left, map) && evalWhere(where.right, map);
        if (where.type === 'Or') return evalWhere(where.left, map) || evalWhere(where.right, map);
        if (where.type === 'Not') return !evalWhere(where.clause, map);
        return false;
      }

      async function* genMulti() {
        const sets = await Promise.all(
          multi.patterns.map(async p => {
            const res = await fetch(p);
            return res.length === 0 && isOptional ? [undefined] : res;
          })
        );

        const hasAggItem = multi.returnItems.map(r => self.hasAgg(r.expression));
        const hasAgg = hasAggItem.some(v => v);
        const aliasFor = (ri: typeof multi.returnItems[number], idx: number): string => {
          if (ri.alias) return ri.alias;
          if (ri.expression.type === 'Variable') return ri.expression.name;
          return multi.returnItems.length === 1 ? 'value' : `value${idx}`;
        };

        const rows: { row: Record<string, any>; order?: any[] }[] = [];

        const traverse = (idx: number, varsMap: Map<string, any>) => {
          if (idx >= sets.length) {
            if (!evalWhere(multi.where, varsMap)) return;
            if (hasAgg) {
              // treat like single pattern but with synthetic group key
              const keyParts: any[] = [];
              for (let i = 0; i < multi.returnItems.length; i++) {
                if (!hasAggItem[i]) keyParts.push(self.evalExpr(multi.returnItems[i].expression, varsMap, params));
              }
              const key = JSON.stringify(keyParts);
              let group = (groupMap as any).get(key);
              if (!group) {
                const row: Record<string, any> = {};
                let j = 0;
                multi.returnItems.forEach((item, idx2) => {
                  if (!hasAggItem[idx2]) {
                    row[aliasFor(item, idx2)] = keyParts[j++];
                  }
                });
                group = { row, aggs: [], vars: new Map<string, any>(varsMap) };
                (groupMap as any).set(key, group);
              }
              multi.returnItems.forEach((item, idx2) => {
                if (!hasAggItem[idx2]) return;
                const cur = (group.aggs[idx2] = group.aggs[idx2] ?? self.initAggState(item.expression));
                self.updateAggState(item.expression, cur, varsMap, params);
              });
            } else {
              const row: Record<string, any> = {};
              const aliasVars = new Map<string, any>(varsMap);
              multi.returnItems.forEach((item, idx2) => {
                const alias = aliasFor(item, idx2);
                if (item.expression.type === 'All') {
                  for (const [k, v] of varsMap.entries()) {
                    row[k] = v;
                    aliasVars.set(k, v);
                  }
                } else {
                  const val = self.evalExpr(item.expression, varsMap, params);
                  row[alias] = val;
                  if (item.alias) aliasVars.set(item.alias, val);
                }
              });
              const order = multi.orderBy ? multi.orderBy.map(o => self.evalExpr(o.expression, aliasVars, params)) : undefined;
              rows.push({ row, order });
            }
            return;
          }
          for (const node of sets[idx]) {
            varsMap.set(multi.patterns[idx].variable, node);
            traverse(idx + 1, varsMap);
          }
        };

        const groupMap = new Map<string, any>();
        traverse(0, new Map<string, any>());

        if (hasAgg) {
          if (groupMap.size === 0 && hasAggItem.every(v => v)) {
            groupMap.set('__empty__', { row: {}, aggs: [], vars: new Map<string, any>() });
          }
          for (const group of groupMap.values()) {
            const aliasVars = new Map<string, any>(group.vars);
            multi.returnItems.forEach((it, i) => {
              const alias = aliasFor(it, i);
              aliasVars.set(alias, group.row[alias]);
              if (it.alias) aliasVars.set(it.alias, group.row[alias]);
            });
            multi.returnItems.forEach((item, idx2) => {
              if (!hasAggItem[idx2]) return;
              group.row[aliasFor(item, idx2)] = self.finalizeAgg(item.expression, group.aggs[idx2], aliasVars, params);
            });
            let order: any[] | undefined;
            if (multi.orderBy) {
              order = multi.orderBy.map(o => self.evalExpr(o.expression, aliasVars, params));
            }
            rows.push({ row: group.row, order });
          }
        }

        if (rows.length === 0 && isOptional && !hasAgg) {
          const empty: Record<string, any> = {};
          multi.returnItems.forEach((item, idx) => {
            const alias = aliasFor(item, idx);
            if (item.expression.type === 'All') {
              vars.forEach(v => {
                empty[v] = undefined;
              });
            } else {
              empty[alias] = undefined;
            }
          });
          rows.push({ row: empty });
        }

        if (multi.distinct) {
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

        if (multi.orderBy && multi.orderBy.length > 0) {
          rows.sort((a, b) => {
            for (let i = 0; i < multi.orderBy!.length; i++) {
              const av = a.order ? a.order[i] : undefined;
              const bv = b.order ? b.order[i] : undefined;
              if (av === bv) continue;
              if (av === undefined) return 1;
              if (bv === undefined) return -1;
              let cmp = av > bv ? 1 : -1;
              if (multi.orderBy![i].direction === 'DESC') cmp = -cmp;
              return cmp;
            }
            return 0;
          });
        }

        let startIdx = 0;
        if (multi.skip) {
          const v = self.evalExpr(multi.skip, new Map(), params);
          if (typeof v === 'number') startIdx = v;
        }
        let endIdx = rows.length;
        if (multi.limit !== undefined) {
          const v = self.evalExpr(multi.limit, new Map(), params);
          if (typeof v === 'number') endIdx = Math.min(endIdx, startIdx + v);
        }
        for (let i = startIdx; i < endIdx; i++) {
          yield rows[i].row;
        }
      }

      return genMulti();
    }

    if (ast.type === 'MatchChain') {
      const chain = ast as MatchChainQuery;
      if (
        chain.hops.length !== 1 ||
        chain.orderBy ||
        chain.skip ||
        chain.limit ||
        chain.distinct ||
        chain.optional
      )
        return null;
      const hop = chain.hops[0];
      if (hop.rel.direction === 'none') return null;
      const vars = [chain.start.variable, hop.node.variable];
      if (hop.rel.variable) vars.push(hop.rel.variable);
      if (chain.pathVariable) vars.push(chain.pathVariable);
      function checkExpr(expr: Expression): boolean {
        switch (expr.type) {
          case 'Variable':
            return vars.includes(expr.name);
          case 'Property':
          case 'Id':
          case 'Labels':
          case 'Type':
            return vars.includes(expr.variable);
          case 'Literal':
          case 'Parameter':
            return true;
          case 'Add':
          case 'Sub':
          case 'Mul':
          case 'Div':
            return checkExpr(expr.left) && checkExpr(expr.right);
          case 'Neg':
            return checkExpr(expr.expression);
          case 'Count':
          case 'Sum':
          case 'Min':
          case 'Max':
          case 'Avg':
          case 'Collect':
            return expr.expression ? checkExpr(expr.expression) : true;
          case 'Length':
            return checkExpr(expr.expression);
          case 'All':
            return chain.returnItems.length === 1;
          default:
            return false;
        }
      }
      for (const ri of chain.returnItems) if (!checkExpr(ri.expression)) return null;
      const self = this;

      function evalWhere(where: any, map: Map<string, any>): boolean {
        if (!where) return true;
        if (where.type === 'Condition') {
          const left = self.evalExpr(where.left, map, params);
          const right = where.right ? self.evalExpr(where.right, map, params) : undefined;
          switch (where.operator) {
            case '=':
              return left === right;
            case '<>':
              return left !== right;
            case '>':
              return left > right;
            case '>=':
              return left >= right;
            case '<':
              return left < right;
            case '<=':
              return left <= right;
            case 'IN':
              return Array.isArray(right) && right.includes(left);
            case 'IS NULL':
              return left === null || left === undefined;
            case 'IS NOT NULL':
              return left !== null && left !== undefined;
            case 'STARTS WITH':
              return typeof left === 'string' && typeof right === 'string' && left.startsWith(right);
            case 'ENDS WITH':
              return typeof left === 'string' && typeof right === 'string' && left.endsWith(right);
            case 'CONTAINS':
              return typeof left === 'string' && typeof right === 'string' && left.includes(right);
            default:
              return false;
          }
        }
        if (where.type === 'And') return evalWhere(where.left, map) && evalWhere(where.right, map);
        if (where.type === 'Or') return evalWhere(where.left, map) || evalWhere(where.right, map);
        if (where.type === 'Not') return !evalWhere(where.clause, map);
        return false;
      }

      async function* genChain() {
        await self.ensureReady();
        let sql =
          'SELECT n1.id as n1id, n1.labels as n1labels, n1.properties as n1props,' +
          ' e.id as eid, e.type as etype, e.startNode as estart, e.endNode as eend, e.properties as eprops,' +
          ' n2.id as n2id, n2.labels as n2labels, n2.properties as n2props FROM nodes n1 JOIN edges e ';
        if (hop.rel.direction === 'out') {
          sql += 'ON e.startNode = n1.id JOIN nodes n2 ON e.endNode = n2.id';
        } else {
          sql += 'ON e.endNode = n1.id JOIN nodes n2 ON e.startNode = n2.id';
        }
        const conds: string[] = [];
        const paramsArr: any[] = [];
        if (hop.rel.type) {
          conds.push('e.type = ?');
          paramsArr.push(hop.rel.type);
        }
        if (hop.rel.properties) {
          for (const [k, v] of Object.entries(hop.rel.properties)) {
            const val =
              v && typeof v === 'object' && 'type' in v
                ? v.type === 'Literal'
                  ? (v as any).value
                  : v.type === 'Parameter'
                  ? params[(v as any).name]
                  : undefined
                : v;
            if (val === null) {
              conds.push(`json_extract(e.properties, '$.${k}') IS NULL`);
            } else {
              conds.push(`json_extract(e.properties, '$.${k}') = ?`);
              paramsArr.push(val);
            }
          }
        }
        if (chain.start.labels) {
          for (const lbl of chain.start.labels) {
            conds.push('n1.labels LIKE ?');
            paramsArr.push(`%"${lbl}"%`);
          }
        }
        if (chain.start.properties) {
          for (const [k, v] of Object.entries(chain.start.properties)) {
            const val =
              v && typeof v === 'object' && 'type' in v
                ? v.type === 'Literal'
                  ? (v as any).value
                  : v.type === 'Parameter'
                  ? params[(v as any).name]
                  : undefined
                : v;
            if (val === null) {
              conds.push(`json_extract(n1.properties, '$.${k}') IS NULL`);
            } else {
              conds.push(`json_extract(n1.properties, '$.${k}') = ?`);
              paramsArr.push(val);
            }
          }
        }
        if (hop.node.labels) {
          for (const lbl of hop.node.labels) {
            conds.push('n2.labels LIKE ?');
            paramsArr.push(`%"${lbl}"%`);
          }
        }
        if (hop.node.properties) {
          for (const [k, v] of Object.entries(hop.node.properties)) {
            const val =
              v && typeof v === 'object' && 'type' in v
                ? v.type === 'Literal'
                  ? (v as any).value
                  : v.type === 'Parameter'
                  ? params[(v as any).name]
                  : undefined
                : v;
            if (val === null) {
              conds.push(`json_extract(n2.properties, '$.${k}') IS NULL`);
            } else {
              conds.push(`json_extract(n2.properties, '$.${k}') = ?`);
              paramsArr.push(val);
            }
          }
        }
        if (conds.length) sql += ' WHERE ' + conds.join(' AND ');
        const stmt = self.db.prepare(sql);
        stmt.bind(paramsArr);
        while (stmt.step()) {
          const rowObj: any = stmt.getAsObject();
          const n1 = self.rowToNode({
            id: rowObj.n1id,
            labels: rowObj.n1labels,
            properties: rowObj.n1props,
          });
          const n2 = self.rowToNode({
            id: rowObj.n2id,
            labels: rowObj.n2labels,
            properties: rowObj.n2props,
          });
          const rel = self.rowToRel({
            id: rowObj.eid,
            type: rowObj.etype,
            startNode: rowObj.estart,
            endNode: rowObj.eend,
            properties: rowObj.eprops,
          });
          const varsMap = new Map<string, any>();
          varsMap.set(chain.start.variable, n1);
          varsMap.set(hop.node.variable, n2);
          if (hop.rel.variable) varsMap.set(hop.rel.variable, rel);
          if (chain.pathVariable) {
            varsMap.set(chain.pathVariable, { nodes: [n1, n2], relationships: [rel] });
          }
          if (chain.where && !evalWhere(chain.where, varsMap)) continue;
          const outRow: Record<string, any> = {};
          chain.returnItems.forEach((item: any, idx: number) => {
            if (item.expression.type === 'All') {
              for (const [k, v] of varsMap.entries()) outRow[k] = v;
            } else {
              const alias =
                item.alias ||
                (item.expression.type === 'Variable'
                  ? item.expression.name
                  : chain.returnItems.length === 1
                  ? 'value'
                  : `value${idx}`);
              outRow[alias] = self.evalExpr(item.expression, varsMap, params);
            }
          });
          yield outRow;
        }
        stmt.free();
      }

      return genChain();
    }

    if (ast.type === 'Unwind') {
      const un = ast as UnwindQuery;
      const self = this;
      async function* genUnwind() {
        let items: unknown[];
        if (Array.isArray(un.list)) {
          items = un.list;
        } else {
          const v = self.evalExpr(un.list, new Map(), params);
          items = Array.isArray(v) ? v : [];
        }
        for (const item of items) {
          const vars = new Map<string, any>();
          vars.set(un.variable, item);
          const val = self.evalExpr(un.returnExpression, vars, params);
          const alias =
            un.returnAlias ||
            (un.returnExpression.type === 'Variable' &&
            un.returnExpression.name === un.variable
              ? un.variable
              : 'value');
          yield { [alias]: val } as Record<string, any>;
        }
      }
      return genUnwind();
    }

    if (ast.type === 'Return') {
      const ret = ast as ReturnQuery;
      const aliasFor = (item: typeof ret.returnItems[number], idx: number): string => {
        if (item.alias) return item.alias;
        if (item.expression.type === 'Variable') return item.expression.name;
        return ret.returnItems.length === 1 ? 'value' : `value${idx}`;
      };
      const self = this;
      async function* genReturn() {
        const row: Record<string, any> = {};
        const aliasVars = new Map<string, any>();
        ret.returnItems.forEach((item, idx) => {
          if (item.expression.type === 'All') {
            // nothing in vars map
          } else {
            const val = self.evalExpr(item.expression, new Map(), params);
            const alias = aliasFor(item, idx);
            row[alias] = val;
            if (item.alias) aliasVars.set(item.alias, val);
          }
        });
        let include = true;
        let start = 0;
        if (ret.skip) start = Number(self.evalExpr(ret.skip, new Map(), params));
        let limit = ret.limit ? Number(self.evalExpr(ret.limit, new Map(), params)) : undefined;
        if (start > 0) include = false;
        if (include) {
          if (limit === undefined || limit > 0) {
            yield row;
          }
        }
      }
      return genReturn();
    }

    if (ast.type === 'Union') {
      const u = ast as UnionQuery;
      const leftIter = this.runTranspiledAST(u.left, params);
      const rightIter = this.runTranspiledAST(u.right, params);
      if (!leftIter || !rightIter) return null;
      const self = this;
      async function* genUnion() {
        const rows: Record<string, any>[] = [];
        const seen = new Set<string>();
        for await (const r of leftIter!) {
          if (u.all) {
            rows.push(r);
          } else {
            const k = JSON.stringify(r);
            if (!seen.has(k)) {
              seen.add(k);
              rows.push(r);
            }
          }
        }
        for await (const r of rightIter!) {
          if (u.all) {
            rows.push(r);
          } else {
            const k = JSON.stringify(r);
            if (!seen.has(k)) {
              seen.add(k);
              rows.push(r);
            }
          }
        }
        if (u.orderBy) {
          rows.sort((a, b) => {
            for (let i = 0; i < u.orderBy!.length; i++) {
              const ob = u.orderBy![i];
              const av = self.evalExpr(ob.expression, new Map(Object.entries(a)), params);
              const bv = self.evalExpr(ob.expression, new Map(Object.entries(b)), params);
              if (av === bv) continue;
              if (av === undefined) return 1;
              if (bv === undefined) return -1;
              let cmp = av > bv ? 1 : -1;
              if (ob.direction === 'DESC') cmp = -cmp;
              return cmp;
            }
            return 0;
          });
        }
        let start = 0;
        if (u.skip) start = Number(self.evalExpr(u.skip, new Map(), params));
        let end = rows.length;
        if (u.limit !== undefined) {
          end = Math.min(end, start + Number(self.evalExpr(u.limit, new Map(), params)));
        }
        for (let i = start; i < end; i++) {
          yield rows[i];
        }
      }
      return genUnion();
    }
    return null;
  }

  runTranspiled(
    cypher: string,
    params: Record<string, any>
  ): AsyncIterable<Record<string, unknown>> | null {
    const asts = parseMany(cypher);
    if (asts.length !== 1) return null;
    const ast = asts[0];
    if (ast.type === 'Union') {
      const leftIter = this.runTranspiledAST((ast as UnionQuery).left, params);
      const rightIter = this.runTranspiledAST((ast as UnionQuery).right, params);
      if (!leftIter || !rightIter) return null;
      const self = this;
      async function* gen() {
        const rows: Record<string, any>[] = [];
        const seen = new Set<string>();
        for await (const r of leftIter!) {
          if ((ast as UnionQuery).all) {
            rows.push(r);
          } else {
            const k = JSON.stringify(r);
            if (!seen.has(k)) {
              seen.add(k);
              rows.push(r);
            }
          }
        }
        for await (const r of rightIter!) {
          if ((ast as UnionQuery).all) {
            rows.push(r);
          } else {
            const k = JSON.stringify(r);
            if (!seen.has(k)) {
              seen.add(k);
              rows.push(r);
            }
          }
        }
        if ((ast as UnionQuery).orderBy) {
          rows.sort((a, b) => {
            for (let i = 0; i < (ast as UnionQuery).orderBy!.length; i++) {
              const ao = (ast as UnionQuery).orderBy![i];
              const av = self.evalExpr(ao.expression, new Map(Object.entries(a)), params);
              const bv = self.evalExpr(ao.expression, new Map(Object.entries(b)), params);
              if (av === bv) continue;
              if (av === undefined) return 1;
              if (bv === undefined) return -1;
              let cmp = av > bv ? 1 : -1;
              if (ao.direction === 'DESC') cmp = -cmp;
              return cmp;
            }
            return 0;
          });
        }
        let start = 0;
        if ((ast as UnionQuery).skip)
          start = Number(self.evalExpr((ast as UnionQuery).skip!, new Map(), params));
        let end = rows.length;
        if ((ast as UnionQuery).limit !== undefined) {
          end = Math.min(
            end,
            start + Number(self.evalExpr((ast as UnionQuery).limit!, new Map(), params))
          );
        }
        for (let i = start; i < end; i++) {
          yield rows[i];
        }
      }
      return gen();
    }
    if (ast.type === 'Call') {
      const call = ast as CallQuery;
      const subsIters = call.subquery.map(q => this.runTranspiledAST(q, params));
      if (subsIters.some(s => !s)) return null;
      const self = this;
      async function* genCall() {
        const local = new Map<string, any>();
        const outRows: Record<string, any>[] = [];
        for (let i = 0; i < subsIters.length; i++) {
          const it = subsIters[i]!;
          for await (const row of it) {
            if (i === subsIters.length - 1) {
              const varsWithRow = new Map(local);
              for (const [k, v] of Object.entries(row)) varsWithRow.set(k, v);
              const out: Record<string, any> = {};
              call.returnItems.forEach((item, idx) => {
                const alias =
                  item.alias ||
                  (item.expression.type === 'Variable'
                    ? item.expression.name
                    : call.returnItems.length === 1
                    ? 'value'
                    : `value${idx}`);
                out[alias] = self.evalExpr(item.expression, varsWithRow, params);
              });
              outRows.push(out);
            } else {
              for (const [k, v] of Object.entries(row)) local.set(k, v);
            }
          }
        }
        let rowsToYield = outRows;
        if (call.distinct) {
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
        if (call.orderBy) {
          rowsToYield.sort((a, b) => {
            for (let i = 0; i < call.orderBy!.length; i++) {
              const ob = call.orderBy![i];
              const av = self.evalExpr(ob.expression, new Map(Object.entries(a)), params);
              const bv = self.evalExpr(ob.expression, new Map(Object.entries(b)), params);
              if (av === bv) continue;
              if (av === undefined) return 1;
              if (bv === undefined) return -1;
              let cmp = av > bv ? 1 : -1;
              if (ob.direction === 'DESC') cmp = -cmp;
              return cmp;
            }
            return 0;
          });
        }
        let start = 0;
        if (call.skip) start = Number(self.evalExpr(call.skip, new Map(), params));
        let end = rowsToYield.length;
        if (call.limit !== undefined) {
          end = Math.min(end, start + Number(self.evalExpr(call.limit, new Map(), params)));
        }
        for (let i = start; i < end; i++) {
          yield rowsToYield[i];
        }
      }
      return genCall();
    }
    return this.runTranspiledAST(ast, params);
  }
}
