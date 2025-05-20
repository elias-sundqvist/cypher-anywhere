import {
  StorageAdapter,
  NodeRecord,
  RelRecord,
  NodeScanSpec,
  IndexMetadata,
  parseMany,
  MatchReturnQuery
} from '@cypher-anywhere/core';
import initSqlJs, { Database } from 'sql.js';

export interface NodeTableSpec {
  table: string;
  id: string;
  labels?: string[];
  labelColumn?: string;
  properties: string[];
}

export interface RelationshipTableSpec {
  table: string;
  id: string;
  start: string;
  end: string;
  type?: string;
  typeColumn?: string;
  properties: string[];
}

export interface SchemaSpec {
  nodes: NodeTableSpec[];
  relationships: RelationshipTableSpec[];
}

export interface SqlJsSchemaAdapterOptions {
  schema: SchemaSpec;
  setup?: (db: Database) => void | Promise<void>;
  indexes?: IndexMetadata[];
}

export class SqlJsSchemaAdapter implements StorageAdapter {
  private db!: Database;
  private ready: Promise<void>;
  private schema: SchemaSpec;
  private indexes: IndexMetadata[];

  constructor(options: SqlJsSchemaAdapterOptions) {
    this.schema = options.schema;
    this.indexes = options.indexes ?? [];
    this.ready = this.init(options);
  }

  private async init(options: SqlJsSchemaAdapterOptions): Promise<void> {
    const SQL = await initSqlJs();
    this.db = new SQL.Database();
    if (options.setup) await options.setup(this.db);
  }

  private async ensureReady(): Promise<void> {
    await this.ready;
  }

  private rowToNode(row: any, spec: NodeTableSpec): NodeRecord {
    const labels: string[] = [];
    if (spec.labels) labels.push(...spec.labels);
    if (spec.labelColumn && row[spec.labelColumn]) {
      const val = row[spec.labelColumn];
      if (typeof val === 'string') {
        try {
          const arr = JSON.parse(val);
          if (Array.isArray(arr)) labels.push(...arr);
          else labels.push(String(arr));
        } catch {
          if (val.includes(',')) labels.push(...val.split(',').map(s => s.trim()));
          else labels.push(val);
        }
      } else if (Array.isArray(val)) {
        labels.push(...val);
      }
    }
    const properties: Record<string, unknown> = {};
    for (const col of spec.properties) {
      properties[col] = row[col];
    }
    return { id: row[spec.id], labels, properties };
  }

  private rowToRel(row: any, spec: RelationshipTableSpec): RelRecord {
    const properties: Record<string, unknown> = {};
    for (const col of spec.properties) {
      properties[col] = row[col];
    }
    const type = spec.type ?? (spec.typeColumn ? row[spec.typeColumn] : undefined);
    return {
      id: row[spec.id],
      type: type as string,
      startNode: row[spec.start],
      endNode: row[spec.end],
      properties
    };
  }

  async getNodeById(id: number | string): Promise<NodeRecord | null> {
    await this.ensureReady();
    for (const spec of this.schema.nodes) {
      const stmt = this.db.prepare(`SELECT * FROM ${spec.table} WHERE ${spec.id} = ?`);
      stmt.bind([id]);
      const row = stmt.step() ? stmt.getAsObject() : null;
      stmt.free();
      if (row) return this.rowToNode(row, spec);
    }
    return null;
  }

  async *scanNodes(spec: NodeScanSpec = {}): AsyncIterable<NodeRecord> {
    await this.ensureReady();
    for (const table of this.schema.nodes) {
      const stmt = this.db.prepare(`SELECT * FROM ${table.table}`);
      while (stmt.step()) {
        const row = this.rowToNode(stmt.getAsObject(), table);
        const { label, labels } = spec;
        const labelMatch = label ? row.labels.includes(label) : true;
        const labelsMatch = labels ? labels.every(l => row.labels.includes(l)) : true;
        if (labelMatch && labelsMatch) {
          yield row;
        }
      }
      stmt.free();
    }
  }

  async getRelationshipById(id: number | string): Promise<RelRecord | null> {
    await this.ensureReady();
    for (const spec of this.schema.relationships) {
      const stmt = this.db.prepare(`SELECT * FROM ${spec.table} WHERE ${spec.id} = ?`);
      stmt.bind([id]);
      const row = stmt.step() ? stmt.getAsObject() : null;
      stmt.free();
      if (row) return this.rowToRel(row, spec);
    }
    return null;
  }

  async *scanRelationships(): AsyncIterable<RelRecord> {
    await this.ensureReady();
    for (const table of this.schema.relationships) {
      const stmt = this.db.prepare(`SELECT * FROM ${table.table}`);
      while (stmt.step()) {
        yield this.rowToRel(stmt.getAsObject(), table);
      }
      stmt.free();
    }
  }

  async listIndexes(): Promise<IndexMetadata[]> {
    await this.ensureReady();
    return this.indexes;
  }

  supportsTranspilation = true;

  runTranspiled(
    cypher: string,
    params: Record<string, any>
  ): AsyncIterable<Record<string, unknown>> | null {
    const asts = parseMany(cypher);
    if (asts.length !== 1) return null;
    const ast = asts[0];
    if (ast.type !== 'MatchReturn') return null;
    const matchAst = ast as MatchReturnQuery;
    if (
      matchAst.isRelationship ||
      (matchAst.labels && matchAst.labels.length > 1) ||
      matchAst.orderBy ||
      matchAst.skip ||
      matchAst.limit ||
      matchAst.optional
    )
      return null;
    if (matchAst.returnItems.length !== 1) return null;
    const ret = matchAst.returnItems[0];
    if (ret.expression.type !== 'Variable') return null;
    const alias = ret.alias || ret.expression.name;
    if (matchAst.variable !== ret.expression.name) return null;
    if (this.schema.nodes.length !== 1) return null;
    const table = this.schema.nodes[0];
    let sql = `SELECT * FROM ${table.table}`;
    const paramsArr: any[] = [];
    const conds: string[] = [];
    if (matchAst.labels && matchAst.labels.length === 1) {
      if (table.labels && !table.labels.includes(matchAst.labels[0])) return null;
    }
    if (conds.length > 0) sql += ' WHERE ' + conds.join(' AND ');
    const self = this;
    async function* gen() {
      await self.ensureReady();
      const stmt = self.db.prepare(sql);
      stmt.bind(paramsArr);
      while (stmt.step()) {
        const row = stmt.getAsObject();
        const node = self.rowToNode(row, table);
        if (matchAst.properties) {
          let ok = true;
          for (const [k, v] of Object.entries(matchAst.properties)) {
            const val =
              v && typeof v === 'object' && 'type' in v
                ? v.type === 'Literal'
                  ? (v as any).value
                  : v.type === 'Parameter'
                  ? params[(v as any).name]
                  : undefined
                : v;
            if (node.properties[k] !== val) {
              ok = false;
              break;
            }
          }
          if (!ok) continue;
        }

        function checkWhere(where: any): boolean {
          if (!where) return true;
          if (where.type === 'Condition') {
            if (
              where.operator === '=' &&
              where.left.type === 'Property' &&
              where.left.variable === matchAst.variable &&
              where.right
            ) {
              const prop = where.left.property;
              const val =
                where.right.type === 'Literal'
                  ? where.right.value
                  : where.right.type === 'Parameter'
                  ? params[where.right.name]
                  : undefined;
              return node.properties[prop] === val;
            }
            return false;
          }
          if (where.type === 'And') {
            return checkWhere(where.left) && checkWhere(where.right);
          }
          return false;
        }

        if (!checkWhere(matchAst.where)) continue;
        yield { [alias]: node };
      }
      stmt.free();
    }
    return gen();
  }
}
