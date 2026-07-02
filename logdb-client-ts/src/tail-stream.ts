import type { LogRecord } from './types';

/** Async iterable stream of records from a Tail subscription. */
export class TailStream implements AsyncIterable<LogRecord> {
  private call: any = null;
  private buffer: LogRecord[] = [];
  private done = false;

  constructor(
    call: any,
    private readonly convert: (r: any) => LogRecord
  ) {
    this.call = call;
    call.on('data', (resp: any) => {
      if (resp.heartbeat) return;
      for (const r of resp.records || []) {
        this.buffer.push(convert(r));
      }
    });
    call.on('end', () => { this.done = true; });
    call.on('error', () => { this.done = true; });
  }

  /** Get the next record (waits for new data). */
  async next(): Promise<LogRecord | null> {
    while (this.buffer.length === 0 && !this.done) {
      await new Promise(resolve => setTimeout(resolve, 10));
    }
    if (this.buffer.length > 0) {
      return this.buffer.shift()!;
    }
    return null;
  }

  /** Cancel the subscription. */
  cancel(): void {
    if (this.call) {
      this.call.cancel();
      this.call = null;
    }
    this.done = true;
  }

  [Symbol.asyncIterator](): AsyncIterator<LogRecord> {
    return {
      next: async () => {
        const rec = await this.next();
        return rec ? { value: rec, done: false } : { value: undefined as any, done: true };
      }
    };
  }
}
