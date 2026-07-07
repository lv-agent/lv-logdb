# @lv-agent/logdb-client — TypeScript SDK for logdbd

## Install

```bash
npm install @lv-agent/logdb-client
```

## Quick Start

```typescript
import { Client } from '@lv-agent/logdb-client';

const client = new Client('127.0.0.1:50051');

// Append
const { seq } = await client.append('my-app', 'main', 'test.event', Buffer.from('hello'));
console.log('appended at seq', seq);

// Read
const rec = await client.read('my-app', 'main', 1);
console.log(rec?.eventType);

// Scan all
const records = await client.scanAll('my-app', 'main', 0);
for (const r of records) {
  console.log(`[${r.seq}] ${r.eventType}`);
}

// Tail (live subscription)
const stream = client.tail('my-app', 'main', { fromSeq: 0, consumerGroup: 'workers', consumerId: 'w1' });
for await (const rec of stream) {
  console.log(`[${rec.seq}] ${rec.eventType}`);
  await client.commitOffset('my-app', 'main', 'workers', 'w1', rec.seq);
}

// Subscribe to event types in real-time
const sub = client.subscribe('my-app', 'main',
  ['tool.call', 'llm.call'], 'sandbox-processors', 'worker-1');
sub.on('data', (rec) => {
  console.log(`[${rec.seq}] ${rec.eventType}`);
  client.commitOffset('my-app', 'main', 'sandbox-processors', 'worker-1', rec.seq);
});
```

## API

| Method | Description |
|--------|-------------|
| `append(ns, stream, eventType, content)` | Write a record |
| `read(ns, stream, seq)` | Point read |
| `scanAll(ns, stream, fromSeq)` | Scan all records |
| `query(req)` | Structured query → discriminated `QueryResponse` (see Query) |
| `tail(ns, stream, opts)` | Live subscription (async iterable) |
| `subscribe(ns, stream, eventTypes, group, id)` | Event-type push subscription |
| `listNamespaces()` | List all namespaces |
| `listStreams(ns)` | List streams in a namespace |
| `status()` | Node status |
| `verifyChain(ns, stream)` | Verify hash chain |
| `createStream(ns, stream)` | Create a stream (admin) |
| `deleteStream(ns, stream)` | Delete all records in a stream (admin) |
| `commitOffset(...)` / `committedOffset(...)` | Consumer group offset management |

## Query

logdbd's `Query` RPC is a native structured-filter engine that reads the log
segment directly at the committed cursor (no SQL, no SQLite cache, no Indexer).
Build a `QueryRequest` with predicates + a result shape; the response is a
discriminated union keyed on `kind`.

```typescript
// Count records of a given type
const { count } = await client.query({
  namespace: 'my-app', stream: 'main',
  eventTypes: ['tool.call'],
  result: 'COUNT',
});

// Filter by metadata + seq range, return records (newest first, top 10)
const { records } = await client.query({
  namespace: 'my-app', stream: 'main',
  fromSeq: 100, toSeq: 200,
  metadata: [{ key: 'turn_id', value: '7' }],
  result: 'RECORDS',
  descending: true, limit: 10,
});

// Max of a metadata field (numeric; the engine parses string values as u64)
const { max } = await client.query({
  namespace: 'my-app', stream: 'main',
  eventTypes: ['turn_started'],
  aggregateField: 'turn_id',
  result: 'MAX',
});

// Anti-join: turn_started with no matching turn terminal
const { records: incomplete } = await client.query({
  namespace: 'my-app', stream: 'main',
  eventTypes: ['turn_started'],
  absent: {
    peerEventTypes: ['turn_completed', 'turn_failed', 'turn_canceled', 'turn_blocked'],
    joinKey: 'turn_id',
  },
  result: 'RECORDS',
});
```

Predicates (all AND-combined): `eventTypes`, `fromSeq`/`toSeq` (inclusive),
`metadata` (`key` = `value`), `absent` (anti-join). Result shapes: `RECORDS`,
`COUNT`, `EXISTS`, `COUNT_DISTINCT`, `MIN`, `MAX`, `DISTINCT_VALUES`.

## TLS / mTLS

```typescript
import * as fs from 'fs';

const client = new Client('logdbd.example.com:50051', {
  tlsCa: fs.readFileSync('/etc/certs/ca.crt'),
  tlsCert: fs.readFileSync('/etc/certs/client.crt'),  // mTLS
  tlsKey: fs.readFileSync('/etc/certs/client.key'),   // mTLS
});
```

## RBAC / Auth

Roles: `admin` (all), `writer` (append), `reader` (read/query), `subscriber` (subscribe).

```typescript
// Pass token via ClientOptions
const client = new Client('logdbd.example.com:50051', {
  authToken: 'admin-secret-token',
});
```

## License

Apache-2.0
