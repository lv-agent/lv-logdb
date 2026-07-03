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

// SQL query against cache
const rows = await client.query('my-app', 'main',
  "SELECT seq, event_type FROM records WHERE event_type = 'llm.call' ORDER BY seq DESC LIMIT 10");
for (const row of rows) console.log(JSON.parse(row));

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
| `tail(ns, stream, opts)` | Live subscription (async iterable) |
| `query(ns, stream, sql)` | SQL SELECT against query cache |
| `subscribe(ns, stream, eventTypes, group, id)` | Event-type push subscription |
| `listNamespaces()` | List all namespaces |
| `listStreams(ns)` | List streams in a namespace |
| `status()` | Node status |
| `verifyChain(ns, stream)` | Verify hash chain |
| `createStream(ns, stream)` | Create a stream (admin) |
| `deleteStream(ns, stream)` | Delete all records in a stream (admin) |
| `commitOffset(...)` / `committedOffset(...)` | Consumer group offset management |

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
