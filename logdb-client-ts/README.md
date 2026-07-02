# logdb-client — TypeScript SDK for logdbd

## Install

```bash
npm install logdb-client
```

## Quick Start

```typescript
import { Client } from 'logdb-client';

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
  // Commit progress
  await client.commitOffset('my-app', 'main', 'workers', 'w1', rec.seq);
}
```

## API

| Method | Description |
|--------|-------------|
| `append(ns, stream, eventType, content)` | Write a record |
| `read(ns, stream, seq)` | Point read |
| `scanAll(ns, stream, fromSeq)` | Scan all records |
| `tail(ns, stream, opts)` | Live subscription (async iterable) |
| `listNamespaces()` | List all namespaces |
| `listStreams(ns)` | List streams in a namespace |
| `status()` | Node status |
| `verifyChain(ns, stream)` | Verify hash chain |
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

## License

Apache-2.0
