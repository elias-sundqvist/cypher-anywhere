import {
  StorageAdapter,
  NodeRecord,
  RelRecord,
  NodeScanSpec,
  TransactionCtx,
  IndexMetadata,
} from '@cypher-anywhere/core';
import * as fs from 'fs';

interface Dataset {
  nodes: NodeRecord[];
  relationships: RelRecord[];
}

export interface JsonAdapterOptions {
  datasetPath?: string;
  dataset?: Dataset;
  indexes?: IndexMetadata[];
}

export class JsonAdapter implements StorageAdapter {
  private data: Dataset;
  private txData?: Dataset;
  private indexes: IndexMetadata[];
  private indexData: Map<string, Map<unknown, NodeRecord[] | NodeRecord>> = new Map();

  constructor(options: JsonAdapterOptions) {
    if (options.dataset) {
      this.data = options.dataset;
    } else if (options.datasetPath) {
      const text = fs.readFileSync(options.datasetPath, 'utf8');
      this.data = JSON.parse(text);
    } else {
      throw new Error('dataset or datasetPath must be provided');
    }
    this.indexes = options.indexes ?? [];
    this.buildIndexes();
  }

  private buildIndexes(): void {
    this.indexData.clear();
    const src = this.data;
    for (const idx of this.indexes) {
      if (idx.properties.length !== 1) continue;
      const prop = idx.properties[0];
      const key = `${idx.label ?? ''}:${prop}`;
      const map = new Map<unknown, NodeRecord[] | NodeRecord>();
      for (const node of src.nodes) {
        if (idx.label && !node.labels.includes(idx.label)) continue;
        const val = node.properties[prop];
        if (val === undefined) continue;
        if (idx.unique) {
          map.set(val, node);
        } else {
          const arr = (map.get(val) as NodeRecord[] | undefined) ?? [];
          arr.push(node);
          map.set(val, arr);
        }
      }
      this.indexData.set(key, map);
    }
  }

  async getNodeById(id: number | string): Promise<NodeRecord | null> {
    const src = this.txData ?? this.data;
    return src.nodes.find(n => n.id === id) || null;
  }

  async *scanNodes(spec: NodeScanSpec = {}): AsyncIterable<NodeRecord> {
    const { label, labels } = spec;
    const src = this.txData ?? this.data;
    for (const node of src.nodes) {
      const labelMatch = label ? node.labels.includes(label) : true;
      const labelsMatch = labels ? labels.every(l => node.labels.includes(l)) : true;
      if (labelMatch && labelsMatch) {
        yield node;
      }
    }
  }

  async createNode(labels: string[], properties: Record<string, unknown>): Promise<NodeRecord> {
    const target = this.txData ?? this.data;
    const maxId = target.nodes.reduce((m, n) => Math.max(m, Number(n.id)), 0);
    const id = maxId + 1;
    const node: NodeRecord = { id, labels, properties };
    target.nodes.push(node);
    for (const idx of this.indexes) {
      if (idx.properties.length !== 1) continue;
      const prop = idx.properties[0];
      if (idx.label && !labels.includes(idx.label)) continue;
      const val = properties[prop];
      if (val === undefined) continue;
      const key = `${idx.label ?? ''}:${prop}`;
      let map = this.indexData.get(key);
      if (!map) {
        map = new Map();
        this.indexData.set(key, map);
      }
      if (idx.unique) {
        map.set(val, node);
      } else {
        const arr = (map.get(val) as NodeRecord[] | undefined) ?? [];
        arr.push(node);
        map.set(val, arr);
      }
    }
    return node;
  }

  async deleteNode(id: number | string): Promise<void> {
    const target = this.txData ?? this.data;
    const node = target.nodes.find(n => n.id === id);
    target.nodes = target.nodes.filter(n => n.id !== id);
    if (node) {
      for (const idx of this.indexes) {
        if (idx.properties.length !== 1) continue;
        const prop = idx.properties[0];
        if (idx.label && !node.labels.includes(idx.label)) continue;
        const val = node.properties[prop];
        if (val === undefined) continue;
        const key = `${idx.label ?? ''}:${prop}`;
        const map = this.indexData.get(key);
        if (!map) continue;
        if (idx.unique) {
          map.delete(val);
        } else {
          const arr = map.get(val) as NodeRecord[] | undefined;
          if (arr) {
            const idxPos = arr.findIndex(n => n.id === node.id);
            if (idxPos >= 0) arr.splice(idxPos, 1);
            if (arr.length === 0) map.delete(val); else map.set(val, arr);
          }
        }
      }
    }
    target.relationships = target.relationships.filter(r => r.startNode !== id && r.endNode !== id);
  }

  async updateNodeProperties(id: number | string, properties: Record<string, unknown>): Promise<void> {
    const target = this.txData ?? this.data;
    const node = target.nodes.find(n => n.id === id);
    if (!node) throw new Error('node not found');
    for (const [k, v] of Object.entries(properties)) {
      for (const idx of this.indexes) {
        if (idx.properties.length !== 1 || idx.properties[0] !== k) continue;
        if (idx.label && !node.labels.includes(idx.label)) continue;
        const key = `${idx.label ?? ''}:${k}`;
        const map = this.indexData.get(key);
        if (map) {
          const oldVal = node.properties[k];
          if (idx.unique) {
            if (oldVal !== undefined) map.delete(oldVal);
            map.set(v, node);
          } else {
            if (oldVal !== undefined) {
              const arr = map.get(oldVal) as NodeRecord[] | undefined;
              if (arr) {
                const pos = arr.findIndex(n => n.id === node.id);
                if (pos >= 0) arr.splice(pos, 1);
                if (arr.length === 0) map.delete(oldVal); else map.set(oldVal, arr);
              }
            }
            const arr2 = (map.get(v) as NodeRecord[] | undefined) ?? [];
            arr2.push(node);
            map.set(v, arr2);
          }
        }
      }
      node.properties[k] = v;
    }
  }

  async findNode(labels: string[], properties: Record<string, unknown>): Promise<NodeRecord | null> {
    const src = this.txData ?? this.data;
    for (const node of src.nodes) {
      if (labels.length && !labels.every(l => node.labels.includes(l))) {
        continue;
      }
      let ok = true;
      for (const [k, v] of Object.entries(properties)) {
        if (node.properties[k] !== v) {
          ok = false;
          break;
        }
      }
      if (ok) return node;
    }
    return null;
  }

  async *indexLookup(
    label: string | undefined,
    property: string,
    value: unknown
  ): AsyncIterable<NodeRecord> {
    const key = `${label ?? ''}:${property}`;
    const srcMap = this.indexData.get(key);
    if (!srcMap) return;
    const entry = srcMap.get(value);
    if (!entry) return;
    if (Array.isArray(entry)) {
      for (const node of entry) {
        yield node;
      }
    } else {
      yield entry;
    }
  }

  async listIndexes(): Promise<IndexMetadata[]> {
    return this.indexes;
  }

  // Relationship APIs left unimplemented for this MVP
  async getRelationshipById(id: number | string): Promise<RelRecord | null> {
    const src = this.txData ?? this.data;
    return src.relationships.find(r => r.id === id) || null;
  }

  async *scanRelationships(): AsyncIterable<RelRecord> {
    const src = this.txData ?? this.data;
    for (const rel of src.relationships) {
      yield rel;
    }
  }

  async createRelationship(type: string, startNode: number | string, endNode: number | string, properties: Record<string, unknown>): Promise<RelRecord> {
    const target = this.txData ?? this.data;
    const maxId = target.relationships.reduce((m, r) => Math.max(m, Number(r.id)), 0);
    const id = maxId + 1;
    const rel: RelRecord = { id, type, startNode, endNode, properties };
    target.relationships.push(rel);
    return rel;
  }

  async deleteRelationship(id: number | string): Promise<void> {
    const target = this.txData ?? this.data;
    target.relationships = target.relationships.filter(r => r.id !== id);
  }

  async updateRelationshipProperties(id: number | string, properties: Record<string, unknown>): Promise<void> {
    const target = this.txData ?? this.data;
    const rel = target.relationships.find(r => r.id === id);
    if (!rel) throw new Error('relationship not found');
    Object.assign(rel.properties, properties);
  }

  async beginTransaction(): Promise<TransactionCtx> {
    if (this.txData) throw new Error('transaction already in progress');
    this.txData = JSON.parse(JSON.stringify(this.data));
    return {};
  }

  async commit(_: TransactionCtx): Promise<void> {
    if (this.txData) {
      this.data = this.txData;
      this.txData = undefined;
    }
  }

  async rollback(_: TransactionCtx): Promise<void> {
    this.txData = undefined;
  }
}
