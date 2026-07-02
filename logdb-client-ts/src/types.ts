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
