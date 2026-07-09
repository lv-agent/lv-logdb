/**
 * Broker client — TypeScript SDK for logdb-broker (cr-037).
 *
 * Consumers and producers both talk ONLY to the broker (symmetric gateway).
 *
 * ```typescript
 * import { BrokerClient } from 'logdb-client';
 *
 * const broker = new BrokerClient('127.0.0.1:9091');
 *
 * // Produce
 * const { gid, seq } = await broker.produce('my-app', 'main', 'my.event',
 *   Buffer.from('hello'), { shardKey: 'session-42' });
 *
 * // Consume (consumer-group)
 * const { generation, assignedShards } = await broker.joinGroup('my-app', 'main', 'g1', 'c1');
 * const stream = await broker.consume('my-app', 'main', 'g1', 'c1', generation);
 * for await (const rec of stream) {
 *   await broker.commitShardOffset('my-app', 'main', 'g1', rec.shardId, rec.seq);
 * }
 * ```
 */

import * as grpc from '@grpc/grpc-js';
import * as protoLoader from '@grpc/proto-loader';
import * as path from 'path';

// ── Proto loading ────────────────────────────────────────────────────────────
let brokerProto: any = null;

function loadBrokerProto(): any {
  if (brokerProto) return brokerProto;
  const protoPath = path.join(__dirname, '..', 'proto', 'broker.proto');
  const pkgDef = protoLoader.loadSync(protoPath, { keepCase: false });
  brokerProto = grpc.loadPackageDefinition(pkgDef).logdbbroker;
  return brokerProto;
}

// ── Types ────────────────────────────────────────────────────────────────────

export interface BrokerProduceOptions {
  shardKey?: string;
  timestampNs?: number;
  contentType?: string;
  metadata?: Record<string, string>;
}

export interface JoinGroupResult {
  generation: number;
  numShards: number;
  assignedShards: number[];
}

export interface BrokerRecord {
  namespaceId: number;
  streamId: number;
  seq: number;
  eventType: string;
  timestampNs: number;
  contentType: string;
  metadata: Record<string, string>;
  content: Buffer;
  shardId: number;
}

// ── Client ───────────────────────────────────────────────────────────────────

export class BrokerClient {
  private client: any;

  constructor(address: string, insecure: boolean = true) {
    const proto = loadBrokerProto();
    const creds = insecure
      ? grpc.credentials.createInsecure()
      : grpc.credentials.createSsl();
    this.client = new proto.BrokerService(address, creds);
  }

  /** Produce a single record. Returns (gid, seq). */
  produce(
    ns: string, stream: string, eventType: string,
    content: Buffer, opts: BrokerProduceOptions = {},
  ): Promise<{ gid: number; seq: number }> {
    return new Promise((resolve, reject) => {
      this.client.produce({
        namespace: ns, stream, eventType,
        content, shardKey: opts.shardKey,
        timestampNs: opts.timestampNs ?? 0,
        contentType: opts.contentType ?? 'application/json',
        metadata: opts.metadata ?? {},
      }, (err: any, resp: any) => {
        if (err) return reject(err);
        resolve({ gid: resp.gid, seq: resp.seq });
      });
    });
  }

  /** Join a consumer group. */
  joinGroup(
    ns: string, stream: string, group: string, consumerId: string,
  ): Promise<JoinGroupResult> {
    return new Promise((resolve, reject) => {
      this.client.joinGroup({
        namespace: ns, stream, group, consumerId,
      }, (err: any, resp: any) => {
        if (err) return reject(err);
        resolve({
          generation: resp.generation,
          numShards: resp.numShards,
          assignedShards: resp.assignedShards,
        });
      });
    });
  }

  /** Consume records for the caller's assigned shards. */
  consume(
    ns: string, stream: string, group: string, consumerId: string,
    generation: number,
  ): grpc.ClientReadableStream<BrokerRecord> {
    return this.client.consume({
      namespace: ns, stream, group, consumerId, generation,
    }).on('data', (chunk: any) => {
      // Flatten the oneof: ConsumeResponse.record → BrokerRecord
      if (chunk.record) {
        const r = chunk.record;
        (chunk as any).namespaceId = r.namespaceId;
        (chunk as any).streamId = r.streamId;
        (chunk as any).seq = r.seq;
        (chunk as any).eventType = r.eventType;
        (chunk as any).timestampNs = r.timestampNs;
        (chunk as any).contentType = r.contentType;
        (chunk as any).metadata = r.metadata ?? {};
        (chunk as any).content = r.content instanceof Buffer ? r.content : Buffer.from(r.content ?? '');
        (chunk as any).shardId = r.shardId ?? 0;
      }
    });
  }

  /** Commit the per-shard processed offset. */
  commitShardOffset(
    ns: string, stream: string, group: string, shardId: number, committedSeq: number,
  ): Promise<{ advanced: boolean }> {
    return new Promise((resolve, reject) => {
      this.client.commitShardOffset({
        namespace: ns, stream, group, shardId, committedSeq,
      }, (err: any, resp: any) => {
        if (err) return reject(err);
        resolve({ advanced: resp.advanced });
      });
    });
  }

  /** Leave a consumer group. */
  leaveGroup(
    ns: string, stream: string, group: string, consumerId: string,
    generation: number,
  ): Promise<void> {
    return new Promise((resolve, reject) => {
      this.client.leaveGroup({
        namespace: ns, stream, group, consumerId, generation,
      }, (err: any) => {
        if (err) return reject(err);
        resolve();
      });
    });
  }
}
