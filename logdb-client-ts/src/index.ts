/**
 * logdb-client — TypeScript SDK for logdbd
 *
 * ```typescript
 * import { Client } from 'logdb-client';
 *
 * const client = new Client('127.0.0.1:50051');
 *
 * // Append
 * const { seq } = await client.append('my-app', 'main', 'test.event', Buffer.from('hello'));
 *
 * // Read
 * const rec = await client.read('my-app', 'main', 1);
 *
 * // Tail (live subscription)
 * for await (const rec of client.tail('my-app', 'main', { fromSeq: 0 })) {
 *   console.log(rec.seq, rec.eventType);
 * }
 * ```
 */

export { Client, ClientOptions } from './client';
export { TailStream } from './tail-stream';
export {
  LogRecord, AppendResult, Watermark, VerifyResult, TailOptions,
  QueryRequest, QueryResponse, QueryResultKind, MetadataFilter, AbsentMatch,
} from './types';
