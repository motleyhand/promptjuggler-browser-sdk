import { describe, expect, test } from 'vitest';
import { SseParser } from '../src/sse';

const encode = (text: string) => new TextEncoder().encode(text);

describe('SseParser', () => {
  test('parses a complete frame', () => {
    const frames = new SseParser().push(encode('event: token\nid: 4\ndata: {"a":1}\n\n'));
    expect(frames).toEqual([{ event: 'token', id: '4', data: '{"a":1}' }]);
  });

  test('reassembles frames split across chunks at arbitrary points', () => {
    const parser = new SseParser();
    const wire = 'event: token\nid: 1\ndata: {"text":"héllo"}\n\nevent: done\ndata: {}\n\n';
    const bytes = encode(wire);

    // Split inside the multi-byte é to prove the decoder carries partials over.
    const cut = wire.indexOf('é') + 1;
    const frames = [
      ...parser.push(bytes.slice(0, cut)),
      ...parser.push(bytes.slice(cut, cut + 3)),
      ...parser.push(bytes.slice(cut + 3)),
    ];

    expect(frames).toHaveLength(2);
    expect(frames[0]?.data).toBe('{"text":"héllo"}');
    expect(frames[1]?.event).toBe('done');
  });

  test('tolerates CRLF line endings', () => {
    const frames = new SseParser().push(encode('event: done\r\ndata: {}\r\n\r\n'));
    expect(frames).toEqual([{ event: 'done', id: undefined, data: '{}' }]);
  });

  test('ignores comment heartbeats between frames', () => {
    const frames = new SseParser().push(encode(': heartbeat\n\nevent: done\ndata: {}\n\n'));
    expect(frames).toHaveLength(1);
    expect(frames[0]?.event).toBe('done');
  });

  test('joins multiple data lines with newlines', () => {
    const frames = new SseParser().push(encode('data: a\ndata: b\n\n'));
    expect(frames[0]).toMatchObject({ event: 'message', data: 'a\nb' });
  });
});
