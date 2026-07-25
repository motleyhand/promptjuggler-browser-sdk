import { afterEach, beforeEach, describe, expect, test } from 'vitest';
import type { DataEvent, DoneEvent, TextEvent } from '../src/stream';
import { PromptJugglerStream, withChannels } from '../src/stream';
import { connect, record, SseServer } from './helpers';

describe('PromptJugglerStream', () => {
  let server: SseServer;
  let url: string;
  let stream: PromptJugglerStream | undefined;

  beforeEach(async () => {
    server = new SseServer();
    url = await server.start();
    stream = undefined;
  });

  afterEach(async () => {
    stream?.disconnect();
    await server.stop();
  });

  test('sends the bearer token and receives typed events', async () => {
    stream = connect(url);
    const tokens = record<{ runId: string; text: string }>();
    const done = record<{ runId: string }>();
    stream.on('token', tokens.push);
    stream.on('done', done.push);
    stream.connect();

    const connection = await server.connection(1);
    expect(connection.request.headers.authorization).toBe('Bearer test-token');

    connection.send(
      'token',
      '{"kind":"token","runId":"r1","channel":"default","segment":0,"seq":1,"text":"Hi"}',
      1,
    );
    connection.send('done', '{"kind":"done","runId":"r1","channel":"default"}', 2);

    expect(await tokens.next()).toMatchObject({ runId: 'r1', text: 'Hi' });
    expect(await done.next()).toMatchObject({ runId: 'r1' });
  });

  test('maintains full text across segments and applies scoped resets', async () => {
    stream = connect(url);
    const texts = record<TextEvent>();
    stream.on('text', texts.push);
    stream.connect();

    const connection = await server.connection(1);
    const send = (segment: number, seq: number, text: string) => {
      connection.send(
        'token',
        JSON.stringify({ kind: 'token', runId: 'r1', channel: 'default', segment, seq, text }),
      );
    };

    send(0, 1, 'Let me check… ');
    expect((await texts.next()).text).toBe('Let me check… ');

    // Continuation after an async tool call: higher segment, reset discards nothing.
    connection.send('reset', '{"kind":"reset","runId":"r1","channel":"default","segment":3,"seq":0}');
    send(3, 1, 'half an ans');
    expect((await texts.next()).text).toBe('Let me check… half an ans');

    // Retry of that continuation: same segment, only its text is discarded.
    connection.send('reset', '{"kind":"reset","runId":"r1","channel":"default","segment":3,"seq":0}');
    expect((await texts.next()).text).toBe('Let me check… ');
    send(3, 1, 'the answer');
    expect((await texts.next()).text).toBe('Let me check… the answer');

    expect(stream.text('r1')).toBe('Let me check… the answer');
  });

  test('a lost event freezes the text at a true prefix and surfaces the gap', async () => {
    stream = connect(url);
    const texts = record<TextEvent>();
    const gaps = record<{ runId: string; segment: number }>();
    stream.on('text', texts.push);
    stream.on('gap', gaps.push);
    stream.connect();

    const connection = await server.connection(1);
    const token = (seq: number, text: string) => {
      connection.send(
        'token',
        JSON.stringify({ kind: 'token', runId: 'r1', channel: 'default', segment: 0, seq, text }),
      );
    };

    token(1, 'The answer');
    expect(await texts.next()).toMatchObject({ text: 'The answer', gapped: false });

    // seq 2 fell into a reconnect window: appending would fabricate text.
    token(3, ' 42.');
    expect(await gaps.next()).toMatchObject({ runId: 'r1', segment: 0 });
    expect(await texts.next()).toMatchObject({ text: 'The answer', gapped: true });

    token(4, ' Really.'); // still past the hole: withheld, no text event

    // A retry re-streams the whole segment, which heals the gap.
    connection.send('reset', '{"kind":"reset","runId":"r1","channel":"default","segment":0,"seq":0}');
    expect(await texts.next()).toMatchObject({ text: '', gapped: false });
    token(1, 'The answer is 42.');
    expect(await texts.next()).toMatchObject({ text: 'The answer is 42.', gapped: false });
  });

  test('a drop and resume is loss-free: no flags unless the server says stale', async () => {
    stream = connect(url, { reconnectDelayMs: { min: 10, max: 20 } });
    const texts = record<TextEvent>();
    const done = record<DoneEvent>();
    stream.on('text', texts.push);
    stream.on('done', done.push);
    stream.connect();

    const first = await server.connection(1);
    first.send(
      'token',
      '{"kind":"token","runId":"r1","channel":"default","segment":0,"seq":1,"text":"Hello"}',
    );
    expect(await texts.next()).toMatchObject({ text: 'Hello', gapped: false });
    first.end();

    // The log replays from the cursor: the resumed connection continues the
    // run exactly, and the terminal verdict stays clean.
    const second = await server.connection(2);
    second.send(
      'token',
      '{"kind":"token","runId":"r1","channel":"default","segment":0,"seq":2,"text":" world"}',
    );
    expect(await texts.next()).toMatchObject({ text: 'Hello world', gapped: false });
    second.send('done', '{"kind":"done","runId":"r1","channel":"default"}');
    expect(await done.next()).toMatchObject({ runId: 'r1', gapped: false });
  });

  test('stale drops the world and rebuilds it from the stream', async () => {
    stream = connect(url, { reconnectDelayMs: { min: 10, max: 20 } });
    const stales = record<undefined>();
    const done = record<DoneEvent>();
    stream.on('stale', stales.push);
    stream.on('done', done.push);
    stream.connect();

    const connection = await server.connection(1);
    connection.send(
      'token',
      '{"kind":"token","runId":"r1","channel":"default","segment":0,"seq":1,"text":"Hello"}',
    );
    connection.send('done', '{"kind":"done","runId":"r1","channel":"default"}');
    expect(await done.next()).toMatchObject({ runId: 'r1', gapped: false });

    // The server sheds events (or the cursor outlived the replay window) and
    // says so; whatever follows may replay entries already applied.
    connection.send('stale', '{"kind":"stale"}');
    await stales.next();
    expect(stream.text('r1')).toBeUndefined();

    // The replayed backlog rebuilds r1 wholesale — including its verdict,
    // which must not read the replayed tokens as post-terminal stragglers.
    connection.send('reset', '{"kind":"reset","runId":"r1","channel":"default","segment":0,"seq":0}');
    connection.send(
      'token',
      '{"kind":"token","runId":"r1","channel":"default","segment":0,"seq":1,"text":"Hello"}',
    );
    connection.send('done', '{"kind":"done","runId":"r1","channel":"default"}');
    expect(await done.next()).toMatchObject({ runId: 'r1', gapped: false });
    expect(stream.text('r1')).toBeUndefined(); // forgotten again after the rebuilt done
  });

  test('a mid-attempt rebuild after stale is flagged, a full one is clean', async () => {
    stream = connect(url);
    const texts = record<TextEvent>();
    const gaps = record<{ runId: string }>();
    stream.on('text', texts.push);
    stream.on('gap', gaps.push);
    stream.connect();

    const connection = await server.connection(1);
    connection.send('stale', '{}'); // connect-time form: empty payload

    // r1's backlog survived only from mid-attempt: its head is gone for good.
    connection.send(
      'token',
      '{"kind":"token","runId":"r1","channel":"default","segment":0,"seq":7,"text":"tail"}',
    );
    expect(await texts.next()).toMatchObject({ runId: 'r1', text: '', gapped: true });
    expect(await gaps.next()).toMatchObject({ runId: 'r1' });

    // r2's backlog is complete from its reset: rebuilt without a flag.
    connection.send('reset', '{"kind":"reset","runId":"r2","channel":"default","segment":0,"seq":0}');
    connection.send(
      'token',
      '{"kind":"token","runId":"r2","channel":"default","segment":0,"seq":1,"text":"whole"}',
    );
    expect(await texts.next()).toMatchObject({ runId: 'r2', text: 'whole', gapped: false });

    // r3 is first seen at a continuation: the run's head streamed before the
    // retained window, and gap subscribers must hear about it.
    connection.send('reset', '{"kind":"reset","runId":"r3","channel":"default","segment":4,"seq":0}');
    expect(await gaps.next()).toMatchObject({ runId: 'r3', segment: 4 });
    expect(gaps.events).toHaveLength(2);
  });

  test('a terminal event for a run never seen streaming is flagged', async () => {
    stream = connect(url);
    const done = record<DoneEvent>();
    stream.on('done', done.push);
    stream.connect();

    const connection = await server.connection(1);
    // The run lived entirely inside a reconnect window (or predates this
    // subscription). Every attempt opens with a reset, so never having seen
    // one means the whole stream was missed — not that the run was empty.
    connection.send('done', '{"kind":"done","runId":"r1","channel":"default"}');
    expect(await done.next()).toMatchObject({ runId: 'r1', gapped: true });
  });

  test('a duplicate done repeats the gapped verdict after the buffer is gone', async () => {
    stream = connect(url);
    const done = record<DoneEvent>();
    stream.on('done', done.push);
    stream.connect();

    const connection = await server.connection(1);
    connection.send(
      'token',
      '{"kind":"token","runId":"r1","channel":"default","segment":0,"seq":1,"text":"Hel"}',
    );
    connection.send(
      'token',
      '{"kind":"token","runId":"r1","channel":"default","segment":0,"seq":3,"text":"rld"}',
    );
    connection.send('done', '{"kind":"done","runId":"r1","channel":"default"}');
    expect(await done.next()).toMatchObject({ runId: 'r1', gapped: true });

    // The backend's lifecycle handlers are idempotent and may re-publish; the
    // duplicate must not report the run whole now that the buffer is gone.
    connection.send('done', '{"kind":"done","runId":"r1","channel":"default"}');
    expect(await done.next()).toMatchObject({ runId: 'r1', gapped: true });
  });

  test('an attempt parked in getToken cannot ride a replacement connection', async () => {
    // The token mint spans a disconnect/connect pair: when the stale attempt
    // finally wakes it must find itself superseded, not open a second stream
    // with the new attempt's abort signal.
    let releaseFirst: ((grant: { token: string; url: string }) => void) | undefined;
    let mints = 0;
    stream = connect(url, {
      getToken: () => {
        mints += 1;
        if (mints === 1) {
          return new Promise((resolve) => {
            releaseFirst = resolve;
          });
        }

        return Promise.resolve({ token: `token-${mints}`, url });
      },
    });
    stream.connect(); // attempt 1 is now parked inside the first mint
    stream.disconnect();
    stream.connect();

    const connection = await server.connection(1);
    expect(connection.request.headers.authorization).toBe('Bearer token-2');

    if (!releaseFirst) {
      throw new Error('the first mint never started');
    }
    releaseFirst({ token: 'token-1', url }); // the stale attempt wakes…
    await new Promise((resolve) => setTimeout(resolve, 150));
    expect(server.connections).toHaveLength(1); // …and goes nowhere
  });

  test('appends the channel filter as query parameters', async () => {
    stream = connect(url, { channels: ['support', 'summary'] });
    stream.connect();

    const connection = await server.connection(1);
    expect(connection.request.url).toContain('channel=support');
    expect(connection.request.url).toContain('channel=summary');
  });

  test('channel filters survive relative and query-bearing stream URLs', () => {
    // node's fetch rejects relative URLs outright, so the composition is
    // asserted directly — in a browser the relative form rides the page base.
    expect(withChannels('/stream/t1', ['support'])).toBe('/stream/t1?channel=support');
    expect(withChannels('/stream/t1?x=1', ['a', 'b'])).toBe('/stream/t1?x=1&channel=a&channel=b');
    expect(withChannels('/stream/t1', ['a b&c'])).toBe('/stream/t1?channel=a%20b%26c');
    expect(withChannels('/stream/t1', [])).toBe('/stream/t1');
  });

  test('reconnects immediately on server handoff, with a fresh token and Last-Event-ID', async () => {
    let mints = 0;
    stream = connect(url, {
      getToken: () => {
        mints += 1;

        return Promise.resolve({ token: `token-${mints}`, url });
      },
    });
    stream.connect();

    const first = await server.connection(1);
    first.send(
      'token',
      '{"kind":"token","runId":"r1","channel":"default","segment":0,"seq":1,"text":"Hi"}',
      7,
    );
    first.reconnect();

    const second = await server.connection(2);
    expect(second.request.headers.authorization).toBe('Bearer token-2');
    // No replay in v1 — the server only logs this, but the protocol sends it.
    expect(second.request.headers['last-event-id']).toBe('7');
    expect(mints).toBe(2);
  });

  test('retries with backoff after an HTTP rejection', async () => {
    server.rejectWith = 429;
    stream = connect(url);
    const drops = record<{ reason: string }>();
    stream.on('disconnected', drops.push);
    stream.connect();

    expect((await drops.next()).reason).toBe('HTTP 429');

    server.rejectWith = undefined;
    await server.connection(1); // a later attempt lands
  });

  test('disconnect stops reconnecting', async () => {
    stream = connect(url);
    stream.connect();
    const connection = await server.connection(1);

    stream.disconnect();
    connection.end();

    // Give any (buggy) reconnect a chance to happen, then assert it did not.
    await new Promise((resolve) => setTimeout(resolve, 150));
    expect(server.connections).toHaveLength(1);
  });

  test('a disconnect/connect pair during backoff cannot revive the old loop', async () => {
    // The revival race: attempt fails, the loop parks in its backoff delay,
    // disconnect() then connect() land during the delay — on wake the old loop
    // must see itself superseded, or two loops would stream side by side.
    server.rejectWith = 429;
    stream = connect(url, { reconnectDelayMs: { min: 200, max: 200 } });
    const drops = record<{ reason: string }>();
    stream.on('disconnected', drops.push);
    stream.connect();
    await drops.next(); // the rejected attempt: the loop is now in its delay

    stream.disconnect();
    server.rejectWith = undefined;
    stream.connect();

    await server.connection(1);
    // Outlive the old loop's delay, then make sure it stayed dead.
    await new Promise((resolve) => setTimeout(resolve, 400));
    expect(server.connections).toHaveLength(1);
  });

  test('stragglers arriving after the terminal verdict are dropped', async () => {
    stream = connect(url);
    const tokens = record<{ runId: string }>();
    const done = record<DoneEvent>();
    stream.on('token', tokens.push);
    stream.on('done', done.push);
    stream.connect();

    const connection = await server.connection(1);
    connection.send(
      'token',
      '{"kind":"token","runId":"r1","channel":"default","segment":0,"seq":1,"text":"Hi"}',
    );
    connection.send('done', '{"kind":"done","runId":"r1","channel":"default"}');
    expect(await tokens.next()).toMatchObject({ runId: 'r1' });
    await done.next();

    // The runner's async publisher lagged behind the backend's terminal frame:
    // its tail tokens and reset must not revive the finished run.
    connection.send(
      'token',
      '{"kind":"token","runId":"r1","channel":"default","segment":0,"seq":2,"text":"!"}',
    );
    connection.send('reset', '{"kind":"reset","runId":"r1","channel":"default","segment":0,"seq":0}');
    connection.send(
      'token',
      '{"kind":"token","runId":"r2","channel":"default","segment":0,"seq":1,"text":"next run"}',
    );

    // The fence: the next token event is already the second run's.
    expect(await tokens.next()).toMatchObject({ runId: 'r2' });
    expect(stream.text('r1')).toBeUndefined();
  });

  test('a straggler after a clean done upgrades the verdict and announces the gap', async () => {
    stream = connect(url);
    const gaps = record<{ runId: string }>();
    const done = record<DoneEvent>();
    stream.on('gap', gaps.push);
    stream.on('done', done.push);
    stream.connect();

    const connection = await server.connection(1);
    connection.send(
      'token',
      '{"kind":"token","runId":"r1","channel":"default","segment":0,"seq":1,"text":"Hello wor"}',
    );
    connection.send('done', '{"kind":"done","runId":"r1","channel":"default"}');
    expect(await done.next()).toMatchObject({ runId: 'r1', gapped: false });

    // The tail token lost the race against the backend's terminal frame: its
    // arrival is the proof that the finished text was truncated.
    connection.send(
      'token',
      '{"kind":"token","runId":"r1","channel":"default","segment":0,"seq":2,"text":"ld!"}',
    );
    expect(await gaps.next()).toMatchObject({ runId: 'r1' });

    // Further stragglers say nothing new; a re-published done now carries it.
    connection.send(
      'token',
      '{"kind":"token","runId":"r1","channel":"default","segment":0,"seq":3,"text":"!!"}',
    );
    connection.send('done', '{"kind":"done","runId":"r1","channel":"default"}');
    expect(await done.next()).toMatchObject({ runId: 'r1', gapped: true });
    expect(gaps.events).toHaveLength(1);
  });

  test("data events maintain the run's emit payload list", async () => {
    stream = connect(url);
    const datas = record<DataEvent>();
    stream.on('data', datas.push);
    stream.connect();

    const connection = await server.connection(1);
    connection.send(
      'data',
      '{"kind":"data","runId":"r1","channel":"default","segment":0,"seq":1,"tool":"cards","payload":{"ids":[333]}}',
    );
    expect(await datas.next()).toMatchObject({
      runId: 'r1',
      data: [{ tool: 'cards', payload: { ids: [333] } }],
    });

    connection.send(
      'data',
      '{"kind":"data","runId":"r1","channel":"default","segment":0,"seq":2,"tool":"cards","payload":{"ids":[412]}}',
    );
    // The event carries the full maintained list, not just the new item.
    expect(await datas.next()).toMatchObject({
      data: [{ payload: { ids: [333] } }, { payload: { ids: [412] } }],
    });
  });

  test('a data frame between tokens does not false-gap the continuation', async () => {
    stream = connect(url);
    const texts = record<TextEvent>();
    const datas = record<DataEvent>();
    const gaps = record<{ runId: string }>();
    stream.on('text', texts.push);
    stream.on('data', datas.push);
    stream.on('gap', gaps.push);
    stream.connect();

    const connection = await server.connection(1);
    // The runner numbers the data frame in the same segment sequence as the
    // surrounding tokens: reset 0, token 1, data 2, continuation token 3.
    connection.send('reset', '{"kind":"reset","runId":"r1","channel":"default","segment":0,"seq":0}');
    connection.send(
      'token',
      '{"kind":"token","runId":"r1","channel":"default","segment":0,"seq":1,"text":"Pulling those up"}',
    );
    connection.send(
      'data',
      '{"kind":"data","runId":"r1","channel":"default","segment":0,"seq":2,"tool":"cards","payload":{"ids":[9]}}',
    );
    connection.send(
      'token',
      '{"kind":"token","runId":"r1","channel":"default","segment":0,"seq":3,"text":" — done."}',
    );

    expect((await texts.next()).text).toBe('Pulling those up');
    // Seq 3 lands straight after the data frame's seq 2: contiguous, so the
    // text keeps growing instead of freezing at a phantom gap.
    expect(await texts.next()).toMatchObject({ text: 'Pulling those up — done.', gapped: false });
    expect(await datas.next()).toMatchObject({ data: [{ tool: 'cards', payload: { ids: [9] } }] });
    expect(gaps.events).toHaveLength(0);
  });

  test('a data straggler after done is dropped and flags the run', async () => {
    stream = connect(url);
    const datas = record<DataEvent>();
    const gaps = record<{ runId: string }>();
    const done = record<DoneEvent>();
    stream.on('data', datas.push);
    stream.on('gap', gaps.push);
    stream.on('done', done.push);
    stream.connect();

    const connection = await server.connection(1);
    // A clean run first — a token so its verdict settles un-gapped.
    connection.send(
      'token',
      '{"kind":"token","runId":"r1","channel":"default","segment":0,"seq":1,"text":"Hi"}',
    );
    connection.send('done', '{"kind":"done","runId":"r1","channel":"default"}');
    expect(await done.next()).toMatchObject({ runId: 'r1', gapped: false });

    connection.send(
      'data',
      '{"kind":"data","runId":"r1","channel":"default","segment":0,"seq":2,"tool":"cards","payload":{}}',
    );
    // A late emit proves the settled data was incomplete: flagged, not applied.
    expect(await gaps.next()).toMatchObject({ runId: 'r1' });
    expect(datas.events).toHaveLength(0);
  });

  test('terminal events drop the run buffer', async () => {
    stream = connect(url);
    const done = record<{ runId: string }>();
    stream.on('done', done.push);
    stream.connect();

    const connection = await server.connection(1);
    connection.send(
      'token',
      '{"kind":"token","runId":"r1","channel":"default","segment":0,"seq":1,"text":"Hi"}',
    );
    connection.send('done', '{"kind":"done","runId":"r1","channel":"default"}');
    await done.next();

    expect(stream.text('r1')).toBeUndefined();
  });
});
