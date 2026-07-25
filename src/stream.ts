import { RunBuffer, type DataItem } from './buffer';
import { SseParser } from './sse';

export type { DataItem } from './buffer';

/** What `getToken` resolves to — hand back `createStreamToken`'s response verbatim. */
export interface StreamTokenGrant {
  token: string;
  /** Fully-resolved SSE endpoint for the thread; overrides `streamUrl`. */
  url?: string;
}

export interface PromptJugglerStreamOptions {
  /**
   * Fetch a stream credential from your own backend, which calls
   * `createStreamToken` with the API key that must never reach the browser.
   * Called on the initial connect and on every reconnect, so expiry renews
   * itself. Return the endpoint's response verbatim (`{ token, url }`) and no
   * other configuration is needed.
   */
  getToken: () => Promise<string | StreamTokenGrant>;
  /**
   * The thread's SSE endpoint — only needed when `getToken` returns a bare
   * token string rather than the `{ token, url }` grant.
   */
  streamUrl?: string;
  /**
   * Conversation lanes to follow. A workflow declares which of its prompt
   * nodes are user-facing by assigning channels; omit to receive every lane.
   */
  channels?: string[];
  /** Reconnect backoff bounds in milliseconds. */
  reconnectDelayMs?: { min?: number; max?: number };
}

export interface TokenEvent {
  runId: string;
  channel: string;
  segment: number;
  seq: number;
  text: string;
}

/** The SDK-maintained full text of a run after applying a delta or reset. */
export interface TextEvent {
  runId: string;
  channel: string;
  text: string;
  /** True when events were (or may have been) lost: treat `text` as a possibly-incomplete prefix. */
  gapped: boolean;
}

/**
 * This run's maintained text is incomplete: it was first seen mid-history, its
 * tail was published after its terminal frame, or its sequence broke. The text
 * freezes at a true prefix, a retry's reset heals it, and the authoritative
 * result is a `getPromptRun` away once `done` arrives.
 */
export interface GapEvent {
  runId: string;
  channel: string;
  segment: number;
}

export interface ResetEvent {
  runId: string;
  channel: string;
  segment: number;
}

/**
 * The run's emit-tool payloads so far — structured data the model produced
 * beside its prose. Maintained like `text`: each event carries the full list,
 * with retried segments dropped. `payload` is whatever the tool's schema
 * describes; cast it at the use site.
 */
export interface DataEvent {
  runId: string;
  channel: string;
  data: DataItem[];
}

export interface DoneEvent {
  runId: string;
  channel: string;
  /** True when the streamed text may be incomplete — fetch the run for the real result. */
  gapped: boolean;
}

export interface FailureEvent {
  runId: string;
  channel: string;
  code: string;
  message: string;
  /** True when the streamed text may be incomplete — fetch the run for the real result. */
  gapped: boolean;
}

export interface StreamEvents {
  /** A raw text delta, exactly as streamed. */
  token: TokenEvent;
  /** The run's full text after each change — subscribe to this to just render. */
  text: TextEvent;
  /** A segment is being re-generated; the SDK has already discarded its buffer. */
  reset: ResetEvent;
  /** The run's emit-tool payloads, maintained — subscribe to render structured data. */
  data: DataEvent;
  /**
   * One run's text is incomplete; frozen at a true prefix. May also follow a
   * terminal event, when a straggling token proves the verdict it carried was
   * optimistic.
   */
  gap: GapEvent;
  /**
   * The server could not resume this subscription without loss — events were
   * shed, or the reconnect outlived the replay window. The SDK has dropped
   * every buffer; runs still streaming should be treated as gapped until their
   * events rebuild them.
   */
  stale: undefined;
  /** Run finished; fetch the authoritative result via getPromptRun if needed. */
  done: DoneEvent;
  /** Run failed, after all retries. */
  failure: FailureEvent;
  connected: undefined;
  disconnected: { reason: string };
}

interface WireEvent {
  kind?: string;
  runId?: string;
  channel?: string;
  segment?: number;
  seq?: number;
  text?: string;
  code?: string;
  message?: string;
  tool?: string;
  payload?: unknown;
}

type Listener<E extends keyof StreamEvents> = (event: StreamEvents[E]) => void;

/**
 * A live subscription to a thread's token stream.
 *
 * The stream is a replayed log: reconnects resume exactly where they left off
 * (automatically — with backoff after failures, immediately when the server
 * hands the connection off during a deploy), and when the server cannot
 * resume without loss it says `stale`. Connect before triggering runs: a
 * fresh subscription starts at the live tip, not in the past.
 */
export class PromptJugglerStream {
  private readonly options: PromptJugglerStreamOptions;
  private readonly listeners = new Map<keyof StreamEvents, Set<Listener<keyof StreamEvents>>>();
  private readonly buffers = new Map<string, RunBuffer>();
  private readonly channels = new Map<string, string>();
  // Terminal verdicts outlive their buffers: the backend re-publishes a
  // terminal event when its handlers retry, and the duplicate must repeat the
  // first verdict — with the buffer already gone, it would otherwise report a
  // gapped run as whole. A boolean per run keeps long threads lean where
  // keeping the buffers could not.
  private readonly verdicts = new Map<string, boolean>();
  private abort: AbortController | undefined;
  private lastEventId: string | undefined;
  private attempts = 0;
  private running = false;
  // Bumped by connect() and disconnect(): a loop parked in a backoff delay
  // compares its own generation on wake, so a disconnect/connect pair during
  // the delay cannot revive it into a second, duplicate connection.
  private generation = 0;

  constructor(options: PromptJugglerStreamOptions) {
    this.options = options;
  }

  on<E extends keyof StreamEvents>(event: E, listener: Listener<E>): () => void {
    let set = this.listeners.get(event);
    if (!set) {
      set = new Set();
      this.listeners.set(event, set);
    }
    set.add(listener as Listener<keyof StreamEvents>);

    return () => set.delete(listener as Listener<keyof StreamEvents>);
  }

  /** Open the stream and keep it open until {@link disconnect}. */
  connect(): void {
    if (this.running) {
      return;
    }
    this.running = true;
    this.generation += 1;
    void this.run(this.generation);
  }

  /** Close the stream and stop reconnecting. */
  disconnect(): void {
    this.running = false;
    this.generation += 1;
    this.abort?.abort();
  }

  /** The maintained full text for a run, if any has streamed. */
  text(runId: string): string | undefined {
    return this.buffers.get(runId)?.text();
  }

  private emit<E extends keyof StreamEvents>(event: E, payload: StreamEvents[E]): void {
    this.listeners.get(event)?.forEach((listener) => {
      listener(payload);
    });
  }

  private async run(generation: number): Promise<void> {
    // Both conditions re-check after every attempt and delay: running covers a
    // plain disconnect, and the generation match keeps this loop from resuming
    // after it has been superseded while it slept.
    while (this.running && generation === this.generation) {
      const immediately = await this.attempt(generation);
      if (generation !== this.generation) {
        return;
      }
      if (immediately) {
        // The server handed us off (deploy, broker restart): a fresh pod is
        // already waiting and the cursor resumes exactly — no reason to wait.
        this.attempts = 0;
        continue;
      }
      await this.delay();
    }
  }

  /** One connection attempt; resolves true when the server asked for an immediate hop. */
  private async attempt(generation: number): Promise<boolean> {
    // The controller stays local: a stale attempt waking from a slow
    // getToken() must find its own controller — already fired by the
    // disconnect that superseded it — never the replacement attempt's live
    // one, which it could otherwise ride into a duplicate connection.
    const abort = new AbortController();
    this.abort = abort;

    let response: Response;
    try {
      const grant = await this.options.getToken();
      if (generation !== this.generation) {
        return false;
      }
      const { token, url } = typeof grant === 'string' ? { token: grant, url: undefined } : grant;
      const target = url ?? this.options.streamUrl;
      if (!target) {
        throw new Error('No stream URL: return { token, url } from getToken or set streamUrl.');
      }

      const headers: Record<string, string> = { Authorization: `Bearer ${token}` };
      if (this.lastEventId !== undefined) {
        headers['Last-Event-ID'] = this.lastEventId;
      }

      response = await fetch(withChannels(target, this.options.channels ?? []), {
        headers,
        signal: abort.signal,
        cache: 'no-store',
      });
    } catch (error) {
      if (this.running && generation === this.generation) {
        this.emit('disconnected', { reason: message(error) });
      }

      return false;
    }

    if (generation !== this.generation) {
      abort.abort();

      return false;
    }

    if (!response.ok || !response.body) {
      this.emit('disconnected', { reason: `HTTP ${response.status}` });

      return false;
    }

    this.attempts = 0;
    this.emit('connected', undefined);

    try {
      return await this.consume(response.body, abort, generation);
    } catch (error) {
      if (this.running && generation === this.generation) {
        this.emit('disconnected', { reason: message(error) });
      }

      return false;
    }
  }

  /** Read frames until the stream ends; true when the server sent `reconnect`. */
  private async consume(
    body: ReadableStream<Uint8Array>,
    abort: AbortController,
    generation: number,
  ): Promise<boolean> {
    const parser = new SseParser();
    const reader = body.getReader();
    for (;;) {
      const { done, value } = await reader.read();
      if (generation !== this.generation) {
        abort.abort();

        return false;
      }
      if (done) {
        this.emit('disconnected', { reason: 'stream ended' });

        return false;
      }
      for (const frame of parser.push(value)) {
        if (frame.id !== undefined) {
          this.lastEventId = frame.id;
        }
        if (frame.event === 'reconnect') {
          abort.abort();
          this.emit('disconnected', { reason: 'server handoff' });

          return true;
        }
        this.dispatch(frame.event, frame.data);
      }
    }
  }

  private dispatch(kind: string, data: string): void {
    let wire: WireEvent;
    try {
      wire = JSON.parse(data) as WireEvent;
    } catch {
      return; // never sent by our streamer; ignore rather than break the stream
    }
    if (kind === 'stale') {
      // The server could not resume without loss — at connect time (cursor
      // beyond the replay window) or mid-stream (events shed before reaching
      // the log). Everything held is now unprovable, and what follows may
      // replay entries already applied: drop the world and rebuild from the
      // stream. Runs whose history is gone stay wherever the consumer last
      // saw them, flagged by the consumer via this event.
      this.buffers.clear();
      this.channels.clear();
      this.verdicts.clear();
      this.emit('stale', undefined);

      return;
    }
    const runId = wire.runId;
    if (runId === undefined) {
      return;
    }
    if ((kind === 'token' || kind === 'reset' || kind === 'data') && this.verdicts.has(runId)) {
      // The runner's tokens ride a lagging async publisher while the backend
      // publishes the terminal frame directly, so stragglers can trail done.
      // The run stays terminal — no buffer revival, no status flip — but a
      // straggler is proof the text was incomplete when the verdict settled:
      // upgrade it and say so, once.
      if (!(this.verdicts.get(runId) ?? false)) {
        this.verdicts.set(runId, true);
        this.emit('gap', { runId, channel: wire.channel ?? '', segment: wire.segment ?? 0 });
      }

      return;
    }
    const channel = wire.channel ?? this.channels.get(runId) ?? '';
    this.channels.set(runId, channel);

    switch (kind) {
      case 'token': {
        const segment = wire.segment ?? 0;
        const seq = wire.seq ?? 0;
        const buffer = this.buffer(runId);
        const outcome = buffer.append(segment, seq, wire.text ?? '');
        // The raw delta always goes out — consumers assembling text themselves
        // have the seq to judge it by. Only the maintained view withholds
        // post-gap fragments, so its text stays a true prefix of the run.
        this.emit('token', { runId, channel, segment, seq, text: wire.text ?? '' });
        if (outcome === 'gap') {
          this.emit('gap', { runId, channel, segment });
        }
        if (outcome !== 'dropped') {
          this.emit('text', { runId, channel, text: buffer.text(), gapped: buffer.gapped() });
        }
        break;
      }
      case 'reset': {
        const segment = wire.segment ?? 0;
        const buffer = this.buffer(runId);
        const dataBefore = buffer.data().length;
        const { changed, orphaned } = buffer.reset(segment, wire.seq ?? 0);
        this.emit('reset', { runId, channel, segment });
        if (orphaned) {
          // First sight of the run at a continuation: earlier segments ran
          // unobserved, and gap subscribers deserve the signal, not just
          // readers of the text flag.
          this.emit('gap', { runId, channel, segment });
        }
        if (changed) {
          this.emit('text', { runId, channel, text: buffer.text(), gapped: buffer.gapped() });
        }
        if (buffer.data().length !== dataBefore) {
          // The reset discarded payloads from the re-generated segment; the
          // retry re-emits them, so publish the shorter maintained list now.
          this.emit('data', { runId, channel, data: buffer.data() });
        }
        break;
      }
      case 'data': {
        const segment = wire.segment ?? 0;
        const seq = wire.seq ?? 0;
        const buffer = this.buffer(runId);
        const before = buffer.data().length;
        const outcome = buffer.addData(segment, seq, { tool: wire.tool ?? '', payload: wire.payload });
        if (outcome === 'gap') {
          // A data frame rides the token sequence, so a hole here means a token
          // was lost before it: flag the run and let text subscribers see the
          // now-frozen prefix, exactly as a token gap would.
          this.emit('gap', { runId, channel, segment });
          this.emit('text', { runId, channel, text: buffer.text(), gapped: buffer.gapped() });
        }
        if (buffer.data().length !== before) {
          this.emit('data', { runId, channel, data: buffer.data() });
        }
        break;
      }
      case 'done': {
        this.emit('done', { runId, channel, gapped: this.verdict(runId) });
        this.forget(runId);
        break;
      }
      case 'failure': {
        this.emit('failure', {
          runId,
          channel,
          code: wire.code ?? 'unknown',
          message: wire.message ?? '',
          gapped: this.verdict(runId),
        });
        this.forget(runId);
        break;
      }
    }
  }

  private buffer(runId: string): RunBuffer {
    let buffer = this.buffers.get(runId);
    if (!buffer) {
      buffer = new RunBuffer();
      this.buffers.set(runId, buffer);
    }

    return buffer;
  }

  /** The run's terminal gapped verdict — settled on first sight, repeated for duplicates. */
  private verdict(runId: string): boolean {
    let verdict = this.verdicts.get(runId);
    if (verdict === undefined) {
      // Every attempt opens with a reset and a reset creates the buffer, so a
      // subscribed client holds one for every run that executed — even a
      // zero-text run. Holding none at terminal time means the whole stream
      // was missed (a reconnect window, or a late connect): the largest gap
      // there is, not a clean run.
      verdict = this.buffers.get(runId)?.gapped() ?? true;
      this.verdicts.set(runId, verdict);
    }

    return verdict;
  }

  /** Terminal event: the buffer's job is over; drop it (not its verdict) so long threads stay lean. */
  private forget(runId: string): void {
    this.buffers.delete(runId);
    this.channels.delete(runId);
  }

  private async delay(): Promise<void> {
    const { min = 500, max = 10_000 } = this.options.reconnectDelayMs ?? {};
    const wait = Math.min(min * 2 ** this.attempts, max);
    this.attempts += 1;
    await new Promise((resolve) => setTimeout(resolve, wait));
  }
}

function message(error: unknown): string {
  return error instanceof Error ? error.message : 'connection failed';
}

/**
 * Append channel filters without parsing the URL: a same-origin relative
 * `streamUrl` has no base for `new URL` and must survive untouched.
 */
export function withChannels(url: string, channels: string[]): string {
  if (channels.length === 0) {
    return url;
  }
  const query = channels.map((channel) => `channel=${encodeURIComponent(channel)}`).join('&');

  return url + (url.includes('?') ? '&' : '?') + query;
}
