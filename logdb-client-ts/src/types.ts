/** A logdbd record. */
export interface LogRecord {
  namespaceId: number;
  streamId: number;
  seq: number;
  eventType: string;
  timestampNs: number;
  contentType: string;
  metadata: Record<string, string>;
  content: Buffer;
}

/** Response from append(). */
export interface AppendResult {
  namespaceId: number;
  streamId: number;
  seq: number;
  gid: number;
}

/** Batch append response. */
export interface BatchAppendResult {
  records: AppendResult[];
  error?: { code: string; message: string };
}

/** Namespace info. */
export interface NamespaceInfo {
  name: string;
  id: number;
  streamCount: number;
}

/** Stream info. */
export interface StreamInfo {
  name: string;
  id: number;
  firstSeq: number;
  durableSeq: number;
  recordCount: number;
}

/** Watermark. */
export interface Watermark {
  namespace: string;
  stream: string;
  oldestSeq: number;
  durableSeq: number;
  replicatedSeq: number;
  nodeId: string;
  role: string;
}

/** Node status. */
export interface StatusResult {
  nodeId: string;
  role: string;
  durableSequence: number;
  checkpoint: number;
  walBytesUsed: number;
  walBytesTotal: number;
}

/** Hash chain verification result. */
export interface VerifyResult {
  ok: boolean;
  verifiedFrom: number;
  verifiedTo: number;
  errorAtSeq: number;
  errorMessage: string;
}

/** Tail options. */
export interface TailOptions {
  fromSeq?: number;
  batchSize?: number;
  consumerGroup?: string;
  consumerId?: string;
}

// ── Structured query (cr-027) ──────────────────────────────────────────────

/** Result-shape selector for a structured query. */
export type QueryResultKind =
  | 'RECORDS'
  | 'COUNT'
  | 'COUNT_DISTINCT'
  | 'MIN'
  | 'MAX'
  | 'EXISTS'
  | 'DISTINCT_VALUES';

/** A metadata equality predicate (key = value). */
export interface MetadataFilter {
  key: string;
  value: string;
}

/** Anti-join (NOT EXISTS): drop records whose `joinKey` value also appears on a
 * record matching `peerEventTypes`. */
export interface AbsentMatch {
  peerEventTypes: string[];
  joinKey: string;
}

/** Structured query request — predicates (all AND-combined) + result shape.
 * The server reads the log segment directly at the committed cursor. */
export interface QueryRequest {
  namespace: string;
  stream: string;
  /** IN filter on event_type (empty/absent = all). */
  eventTypes?: string[];
  /** seq >= (inclusive). */
  fromSeq?: number;
  /** seq <= (inclusive). */
  toSeq?: number;
  /** Equality on metadata fields. */
  metadata?: MetadataFilter[];
  /** Result shape (defaults to RECORDS). */
  result?: QueryResultKind;
  /** COUNT_DISTINCT/MIN/MAX/DISTINCT_VALUES target (empty = seq). */
  aggregateField?: string;
  /** Anti-join. */
  absent?: AbsentMatch;
  /** 0 = unlimited. Ignored for aggregations. */
  limit?: number;
  /** Order records/values by seq descending. */
  descending?: boolean;
}

/** Typed query response — the server's `result` oneof, resolved to a
 * discriminated union keyed on `kind`. */
export type QueryResponse =
  | { kind: 'records'; records: LogRecord[] }
  | { kind: 'count'; count: number }
  | { kind: 'exists'; exists: boolean }
  | { kind: 'countDistinct'; countDistinct: number }
  | { kind: 'min'; min: number }
  | { kind: 'max'; max: number }
  | { kind: 'distinctValues'; values: string[] };
