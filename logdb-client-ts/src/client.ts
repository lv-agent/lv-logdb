import * as grpc from '@grpc/grpc-js';
import * as protoLoader from '@grpc/proto-loader';
import * as path from 'path';

import { TailStream } from './tail-stream';
import type {
  AppendResult, BatchAppendResult, NamespaceInfo, LogRecord,
  StreamInfo, StatusResult, Watermark, VerifyResult, TailOptions,
} from './types';

export interface ClientOptions {
  /** TLS certificate authority (PEM). */
  tlsCa?: Buffer;
  /** TLS client cert (PEM) for mTLS. */
  tlsCert?: Buffer;
  /** TLS client key (PEM) for mTLS. */
  tlsKey?: Buffer;
  /** Bearer auth token. */
  authToken?: string;
}

let protoDef: any = null;

function loadProto(): any {
  if (protoDef) return protoDef;
  const protoPath = path.join(__dirname, '..', 'proto', 'logdbd.proto');
  const pkgDef = protoLoader.loadSync(protoPath, {
    keepCase: true,
    longs: String,
    enums: String,
    defaults: true,
    oneofs: true,
  });
  protoDef = grpc.loadPackageDefinition(pkgDef).logdbd;
  return protoDef;
}

export class Client {
  private client: any;
  private closed = false;

  /**
   * Create a new logdbd client.
   *
   * @param addr — host:port, e.g. "127.0.0.1:50051"
   * @param opts — optional TLS and auth settings
   */
  constructor(addr: string, opts?: ClientOptions) {
    const proto = loadProto();
    const isTls = opts?.tlsCa || addr.startsWith('https://');

    let creds: grpc.ChannelCredentials;
    if (isTls && opts?.tlsCert) {
      creds = grpc.credentials.createSsl(
        opts.tlsCa,
        opts.tlsKey,
        opts.tlsCert,
      );
    } else if (isTls) {
      creds = grpc.credentials.createSsl(opts?.tlsCa);
    } else {
      creds = grpc.credentials.createInsecure();
    }

    const target = addr.startsWith('http') ? addr : (isTls ? `https://${addr}` : `http://${addr}`);
    // Strip protocol for grpc-js
    const grpcTarget = target.replace(/^https?:\/\//, '');

    this.client = new proto.LogDbService(
      grpcTarget,
      creds,
      {
        'grpc.keepalive_time_ms': 30000,
        'grpc.keepalive_timeout_ms': 10000,
      },
    );
  }

  // ── Write ──────────────────────────────────────────────────────────

  /** Append a record. Returns the assigned seq. */
  append(
    namespace: string, stream: string,
    eventType: string, content: Buffer,
  ): Promise<AppendResult> {
    return this.appendFull({
      namespace, stream, eventType,
      content, contentType: 'application/json',
      metadata: {}, timestampNs: 0,
    });
  }

  /** Append with full options. */
  appendFull(opts: {
    namespace: string; stream: string; eventType: string;
    contentType?: string; metadata?: Record<string, string>;
    timestampNs?: number; content: Buffer;
  }): Promise<AppendResult> {
    return new Promise((resolve, reject) => {
      this.client.Append({
        namespace: opts.namespace,
        stream: opts.stream,
        eventType: opts.eventType,
        content: opts.content,
        contentType: opts.contentType || 'application/json',
        metadata: (opts.metadata || {}) as any,
        timestampNs: opts.timestampNs || 0,
      }, (err: any, resp: any) => {
        if (err) return reject(err);
        resolve({
          namespaceId: resp.namespaceId,
          streamId: resp.streamId,
          seq: resp.seq,
          gid: resp.gid,
        });
      });
    });
  }

  /** Batch append — all in same namespace+stream. */
  appendBatch(requests: Array<{
    namespace: string; stream: string; eventType: string; content: Buffer;
  }>): Promise<BatchAppendResult> {
    return new Promise((resolve, reject) => {
      this.client.BatchAppend({ requests }, (err: any, resp: any) => {
        if (err) return reject(err);
        resolve({
          records: (resp.records || []).map((r: any) => ({
            namespaceId: r.namespaceId,
            streamId: r.streamId,
            seq: r.seq,
            gid: r.gid,
          })),
          error: resp.error ? { code: resp.error.code, message: resp.error.message } : undefined,
        });
      });
    });
  }

  // ── Read ───────────────────────────────────────────────────────────

  /** Read a record by seq. Returns null if not found. */
  read(namespace: string, stream: string, seq: number): Promise<LogRecord | null> {
    return new Promise((resolve, reject) => {
      this.client.Read({ namespace, stream, seq }, (err: any, resp: any) => {
        if (err) return reject(err);
        if (!resp.found || !resp.record) return resolve(null);
        resolve(convertRecord(resp.record));
      });
    });
  }

  /** Scan records in range. Returns array of records. */
  scanAll(namespace: string, stream: string, fromSeq: number, limit = 10000): Promise<LogRecord[]> {
    return new Promise((resolve, reject) => {
      const call = this.client.Scan({ namespace, stream, fromSeq, toSeq: 0, limit });
      const records: LogRecord[] = [];
      call.on('data', (resp: any) => {
        for (const r of resp.records || []) records.push(convertRecord(r));
      });
      call.on('end', () => resolve(records));
      call.on('error', (err: any) => reject(err));
    });
  }

  // ── Subscribe ───────────────────────────────────────────────────────

  /** Subscribe to matching event types in real-time.
   *
   * Returns a gRPC server-streaming call.  Records are pushed as they
   * are committed and filtered by `eventTypes`.  The consumer offset
   * is tracked server-side.
   *
   * Usage:
   *   const stream = client.subscribe('my-app', 'main', ['tool.call'], 'group', 'w1');
   *   stream.on('data', (rec) => console.log(rec.eventType, rec.content));
   */
  subscribe(
    namespace: string,
    stream: string,
    eventTypes: string[],
    consumerGroup: string,
    consumerId: string,
  ): grpc.ClientReadableStream<any> {
    return this.client.Subscribe({
      namespace,
      stream,
      eventTypes,
      consumerGroup,
      consumerId,
    }) as grpc.ClientReadableStream<any>;
  }

  /** Subscribe to new records via Tail. */
  tail(namespace: string, stream: string, opts?: TailOptions): TailStream {
    const call = this.client.Tail({
      namespace, stream,
      fromSeq: opts?.fromSeq ?? 0,
      batchSize: opts?.batchSize ?? 100,
      consumerGroup: opts?.consumerGroup ?? '',
      consumerId: opts?.consumerId ?? '',
    });
    return new TailStream(call, convertRecord);
  }

  // ── Query ──────────────────────────────────────────────────────────

  /** Execute a read-only SQL query against a stream's query cache.
   *
   * Only SELECT is allowed — enforced at the SQLite kernel level.
   * Each row is returned as a JSON string.
   */
  query(namespace: string, stream: string, sql: string): Promise<string[]> {
    return new Promise((resolve, reject) => {
      this.client.Query({ namespace, stream, sql }, (err: any, resp: any) => {
        if (err) return reject(err);
        resolve(resp.rows || []);
      });
    });
  }

  // ── Watermark ──────────────────────────────────────────────────────

  /** Get the watermark for a namespace/stream. */
  watermark(namespace: string, stream: string): Promise<Watermark> {
    return new Promise((resolve, reject) => {
      this.client.GetWatermark({ namespace, stream }, (err: any, resp: any) => {
        if (err) return reject(err);
        resolve({
          namespace: resp.namespace,
          stream: resp.stream,
          oldestSeq: Number(resp.oldestSeq),
          durableSeq: Number(resp.durableSeq),
          replicatedSeq: Number(resp.replicatedSeq),
          nodeId: resp.nodeId,
          role: resp.role,
        });
      });
    });
  }

  // ── Admin ──────────────────────────────────────────────────────────

  /** List all namespaces. */
  listNamespaces(): Promise<NamespaceInfo[]> {
    return new Promise((resolve, reject) => {
      this.client.ListNamespaces({}, (err: any, resp: any) => {
        if (err) return reject(err);
        resolve((resp.namespaces || []).map((n: any) => ({
          name: n.name, id: n.id, streamCount: Number(n.streamCount),
        })));
      });
    });
  }

  /** List streams in a namespace. */
  listStreams(namespace: string): Promise<StreamInfo[]> {
    return new Promise((resolve, reject) => {
      this.client.ListStreams({ namespace }, (err: any, resp: any) => {
        if (err) return reject(err);
        resolve((resp.streams || []).map((s: any) => ({
          name: s.name, id: Number(s.id),
          firstSeq: Number(s.firstSeq), durableSeq: Number(s.durableSeq),
          recordCount: Number(s.recordCount),
        })));
      });
    });
  }

  /** Get node status. */
  status(): Promise<StatusResult> {
    return new Promise((resolve, reject) => {
      this.client.Status({}, (err: any, resp: any) => {
        if (err) return reject(err);
        resolve({
          nodeId: resp.nodeId,
          role: resp.role || '',
          durableSequence: Number(resp.durableSequence),
          checkpoint: Number(resp.checkpoint),
          walBytesUsed: Number(resp.walBytesUsed),
          walBytesTotal: Number(resp.walBytesTotal),
        });
      });
    });
  }

  /** Verify hash chain for a stream. */
  verifyChain(namespace: string, stream: string, fromSeq = 0, toSeq = 0): Promise<VerifyResult> {
    return new Promise((resolve, reject) => {
      this.client.VerifyChain({ namespace, stream, fromSeq, toSeq }, (err: any, resp: any) => {
        if (err) return reject(err);
        resolve({
          ok: resp.ok,
          verifiedFrom: Number(resp.verifiedFrom),
          verifiedTo: Number(resp.verifiedTo),
          errorAtSeq: Number(resp.errorAtSeq),
          errorMessage: resp.errorMessage,
        });
      });
    });
  }

  /** Commit consumer offset. */
  commitOffset(
    namespace: string, stream: string,
    consumerGroup: string, consumerId: string, seq: number,
  ): Promise<void> {
    return new Promise((resolve, reject) => {
      this.client.CommitOffset(
        { namespace, stream, consumerGroup, consumerId, committedSeq: seq },
        (err: any) => { if (err) return reject(err); resolve(); },
      );
    });
  }

  /** Get committed offset for a consumer. */
  committedOffset(
    namespace: string, stream: string,
    consumerGroup: string, consumerId: string,
  ): Promise<number> {
    return new Promise((resolve, reject) => {
      this.client.GetCommittedOffset(
        { namespace, stream, consumerGroup, consumerId },
        (err: any, resp: any) => {
          if (err) return reject(err);
          resolve(Number(resp.committedSeq));
        },
      );
    });
  }

  /** Create a stream (and namespace if needed). Requires admin token. */
  createStream(namespace: string, stream: string): Promise<{ namespaceId: number; streamId: number; created: boolean }> {
    return new Promise((resolve, reject) => {
      this.client.CreateStream({ namespace, stream, maxRecords: 0, maxBytes: 0 }, (err: any, resp: any) => {
        if (err) return reject(err);
        resolve({ namespaceId: resp.namespaceId, streamId: resp.streamId, created: resp.created });
      });
    });
  }

  /** Mark all records in a stream as deleted. Requires admin token. */
  deleteStream(namespace: string, stream: string): Promise<{ deleted: boolean; deletedCount: number }> {
    return new Promise((resolve, reject) => {
      this.client.DeleteStream({ namespace, stream }, (err: any, resp: any) => {
        if (err) return reject(err);
        resolve({ deleted: resp.deleted, deletedCount: Number(resp.deletedCount) });
      });
    });
  }

  /** Close the client connection. */
  close(): void {
    if (!this.closed) {
      this.closed = true;
      grpc.closeClient(this.client);
    }
  }
}

function convertRecord(r: any): LogRecord {
  return {
    namespaceId: r.namespaceId,
    streamId: Number(r.streamId),
    seq: Number(r.seq),
    eventType: r.eventType,
    timestampNs: Number(r.timestampNs),
    contentType: r.contentType,
    metadata: r.metadata || {},
    content: Buffer.isBuffer(r.content) ? r.content : Buffer.from(r.content || ''),
  };
}
