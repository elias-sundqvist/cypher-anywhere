export interface NodeRecord {
  id: number | string;
  labels: string[];
  properties: Record<string, unknown>;
}

export interface RelRecord {
  id: number | string;
  type: string;
  startNode: number | string;
  endNode: number | string;
  properties: Record<string, unknown>;
}

export interface NodeScanSpec {
  label?: string;
}

export interface TransactionCtx {
  /** adapter specific context */
}

export interface StorageAdapter {
  getNodeById(id: number | string): Promise<NodeRecord | null>;
  scanNodes(spec?: NodeScanSpec): AsyncIterable<NodeRecord>;

  /** Optional: create a new node. Returns the created record. */
  createNode?(labels: string[], properties: Record<string, unknown>): Promise<NodeRecord>;

  /** Optional: delete a node by id */
  deleteNode?(id: number | string): Promise<void>;

  /** Optional: update properties on an existing node */
  updateNodeProperties?(id: number | string, properties: Record<string, unknown>): Promise<void>;

  /** Optional: find a node by labels and exact property match. */
  findNode?(labels: string[], properties: Record<string, unknown>): Promise<NodeRecord | null>;

  /** Optional: return all nodes matching the given equality predicates using an index if available. */
  indexLookup?(
    label: string | undefined,
    property: string,
    value: unknown
  ): AsyncIterable<NodeRecord>;

  /** Optional: list available indexes to aid the planner. */
  listIndexes?(): Promise<IndexMetadata[]>;

  getRelationshipById?(id: number | string): Promise<RelRecord | null>;
  scanRelationships?(): AsyncIterable<RelRecord>;

  /** Optional: create a new relationship */
  createRelationship?(type: string, startNode: number | string, endNode: number | string, properties: Record<string, unknown>): Promise<RelRecord>;

  /** Optional: delete a relationship by id */
  deleteRelationship?(id: number | string): Promise<void>;

  /** Optional: update properties on an existing relationship */
  updateRelationshipProperties?(id: number | string, properties: Record<string, unknown>): Promise<void>;

  /** Optional transactional hooks */
  beginTransaction?(): Promise<TransactionCtx>;
  commit?(tx: TransactionCtx): Promise<void>;
  rollback?(tx: TransactionCtx): Promise<void>;
}

export interface IndexMetadata {
  label?: string;
  type?: string;
  properties: string[];
  unique: boolean;
  selectivity?: number;
}

export interface GraphStatistics {
  nodeCount(label?: string): Promise<number>;
  relCount(type?: string): Promise<number>;
  distinctValues(label: string, property: string): Promise<number>;
}
