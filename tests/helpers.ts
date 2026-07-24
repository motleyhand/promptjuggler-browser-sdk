import { createServer, type IncomingMessage, type Server, type ServerResponse } from 'node:http';
import { PromptJugglerStream, type PromptJugglerStreamOptions } from '../src/stream';

export interface Connection {
  request: IncomingMessage;
  /** Write one SSE frame. */
  send: (event: string, data: string, id?: number) => void;
  /** Tell the client to hop, as the streamer does on shutdown. */
  reconnect: () => void;
  end: () => void;
}

/**
 * A scripted stand-in for the Go streamer: a real `node:http` SSE server the
 * SDK's real `fetch` talks to. Each incoming request lands in `connections`
 * and is driven by the test.
 */
export class SseServer {
  readonly connections: Connection[] = [];
  /** Status to answer the next request(s) with, instead of streaming. */
  rejectWith: number | undefined;
  private readonly server: Server;
  private url = '';

  constructor() {
    this.server = createServer((request, response) => {
      this.handle(request, response);
    });
  }

  async start(): Promise<string> {
    await new Promise<void>((resolve) => {
      this.server.listen(0, '127.0.0.1', resolve);
    });
    const address = this.server.address();
    if (address === null || typeof address === 'string') {
      throw new Error('no address');
    }
    this.url = `http://127.0.0.1:${address.port}/stream/thread-1`;

    return this.url;
  }

  stop(): Promise<void> {
    this.connections.forEach((connection) => {
      connection.end();
    });
    this.server.closeAllConnections();

    return new Promise((resolve) => {
      this.server.close(() => {
        resolve();
      });
    });
  }

  /** Resolves once the nth (1-based) connection has arrived. */
  async connection(n: number): Promise<Connection> {
    const deadline = Date.now() + 5000;
    for (;;) {
      const connection = this.connections[n - 1];
      if (connection) {
        return connection;
      }
      if (Date.now() > deadline) {
        throw new Error(`connection ${n} never arrived`);
      }
      await new Promise((resolve) => setTimeout(resolve, 5));
    }
  }

  private handle(request: IncomingMessage, response: ServerResponse): void {
    if (this.rejectWith !== undefined) {
      response.writeHead(this.rejectWith).end();

      return;
    }

    response.writeHead(200, { 'Content-Type': 'text/event-stream' });
    response.flushHeaders();

    this.connections.push({
      request,
      send: (event, data, id) => {
        const idLine = id === undefined ? '' : `id: ${id}\n`;
        response.write(`event: ${event}\n${idLine}data: ${data}\n\n`);
      },
      reconnect: () => {
        response.write('retry: 100\nevent: reconnect\ndata: {}\n\n');
        response.end();
      },
      end: () => {
        response.end();
      },
    });
  }
}

/** A stream wired to the fixture server with test-friendly backoff. */
export function connect(url: string, options: Partial<PromptJugglerStreamOptions> = {}): PromptJugglerStream {
  return new PromptJugglerStream({
    getToken: () => Promise.resolve({ token: 'test-token', url }),
    reconnectDelayMs: { min: 10, max: 50 },
    ...options,
  });
}

/**
 * Collect events of one type; `next()` consumes them in order, resolving
 * immediately for events that already arrived — no lost race between the
 * network and the assertion.
 */
export function record<T>(): { events: T[]; push: (event: T) => void; next: () => Promise<T> } {
  const events: T[] = [];
  const waiters: ((event: T) => void)[] = [];
  let cursor = 0;

  return {
    events,
    push: (event: T) => {
      events.push(event);
      const waiter = waiters.shift();
      if (waiter) {
        cursor += 1;
        waiter(event);
      }
    },
    next: () => {
      const buffered = events[cursor];
      if (buffered !== undefined) {
        cursor += 1;

        return Promise.resolve(buffered);
      }

      return new Promise<T>((resolve, reject) => {
        const timer = setTimeout(() => {
          reject(new Error('timed out waiting for an event'));
        }, 5000);
        waiters.push((event) => {
          clearTimeout(timer);
          resolve(event);
        });
      });
    },
  };
}
