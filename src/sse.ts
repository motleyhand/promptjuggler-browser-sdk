/**
 * A single parsed Server-Sent Event. `data` lines are joined with newlines per
 * the spec, though PromptJuggler payloads are always single-line JSON.
 */
export interface SseFrame {
  event: string;
  data: string;
  id?: string;
}

/**
 * Incremental SSE parser: feed it network chunks, get complete frames out.
 * Native `EventSource` cannot send an `Authorization` header, so the SDK reads
 * the stream through `fetch` and parses frames itself — this is that parser,
 * handling frames split across chunks, CRLF line endings, comment heartbeats,
 * and multi-byte characters cut at chunk boundaries (TextDecoder in streaming
 * mode carries the partial sequence over).
 */
export class SseParser {
  private readonly decoder = new TextDecoder();
  private buffer = '';
  private event = '';
  private data: string[] = [];
  private id: string | undefined;

  /** Parse a chunk, returning every frame it completed. */
  push(chunk: Uint8Array): SseFrame[] {
    this.buffer += this.decoder.decode(chunk, { stream: true });

    const frames: SseFrame[] = [];
    for (;;) {
      const newline = this.buffer.indexOf('\n');
      if (newline === -1) {
        break;
      }
      let line = this.buffer.slice(0, newline);
      this.buffer = this.buffer.slice(newline + 1);
      if (line.endsWith('\r')) {
        line = line.slice(0, -1);
      }

      const frame = this.line(line);
      if (frame) {
        frames.push(frame);
      }
    }

    return frames;
  }

  private line(line: string): SseFrame | undefined {
    if (line === '') {
      // Blank line dispatches the accumulated frame.
      if (this.data.length === 0 && this.event === '') {
        return undefined;
      }
      const frame: SseFrame = {
        event: this.event || 'message',
        data: this.data.join('\n'),
        id: this.id,
      };
      this.event = '';
      this.data = [];

      return frame;
    }
    if (line.startsWith(':')) {
      return undefined; // comment — the server's heartbeat
    }

    const colon = line.indexOf(':');
    const field = colon === -1 ? line : line.slice(0, colon);
    let value = colon === -1 ? '' : line.slice(colon + 1);
    if (value.startsWith(' ')) {
      value = value.slice(1);
    }

    switch (field) {
      case 'event':
        this.event = value;
        break;
      case 'data':
        this.data.push(value);
        break;
      case 'id':
        this.id = value;
        break;
      // `retry` is intentionally ignored: the SDK owns its reconnect timing and
      // reconnects immediately on the server's reconnect event.
    }

    return undefined;
  }
}
