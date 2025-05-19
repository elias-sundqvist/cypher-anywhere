import { StorageAdapter, NodeRecord, RelRecord, NodeScanSpec, TransactionCtx } from '@cypher-anywhere/core';
import * as fs from 'fs';

interface Dataset {
  nodes: NodeRecord[];
  relationships: RelRecord[];
}

export interface JsonAdapterOptions {
  datasetPath?: string;
  dataset?: Dataset;
}

export class JsonAdapter implements StorageAdapter {
  private data: Dataset;
  private txData?: Dataset;

  constructor(options: JsonAdapterOptions) {
    if (options.dataset) {
      this.data = options.dataset;
    } else if (options.datasetPath) {
      const text = fs.readFileSync(options.datasetPath, 'utf8');
      this.data = JSON.parse(text);
    } else {
      throw new Error('dataset or datasetPath must be provided');
    }
  }

  async getNodeById(id: number | string): Promise<NodeRecord | null> {
    const src = this.txData ?? this.data;
    return src.nodes.find(n => n.id === id) || null;
  }

  async *scanNodes(spec: NodeScanSpec = {}): AsyncIterable<NodeRecord> {
    const { label } = spec;
    const src = this.txData ?? this.data;
    for (const node of src.nodes) {
      if (!label || node.labels.includes(label)) {
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
    return node;
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
