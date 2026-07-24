// @vitest-environment jsdom
import { act, renderHook, waitFor } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, test } from 'vitest';
import { usePromptJugglerStream } from '../src/react';
import { SseServer } from './helpers';

describe('usePromptJugglerStream', () => {
  let server: SseServer;
  let url: string;

  beforeEach(async () => {
    server = new SseServer();
    url = await server.start();
  });

  afterEach(async () => {
    await server.stop();
  });

  test('renders streamed text and terminal states per run', async () => {
    const { result, unmount } = renderHook(() =>
      usePromptJugglerStream({
        getToken: () => Promise.resolve({ token: 'test-token', url }),
        reconnectDelayMs: { min: 10, max: 50 },
      }),
    );

    const connection = await server.connection(1);
    await waitFor(() => {
      expect(result.current.connected).toBe(true);
    });

    act(() => {
      connection.send(
        'token',
        '{"kind":"token","runId":"r1","channel":"default","segment":0,"seq":1,"text":"Hello"}',
      );
    });
    await waitFor(() => {
      expect(result.current.runs.r1).toMatchObject({ text: 'Hello', status: 'streaming' });
    });

    act(() => {
      connection.send('done', '{"kind":"done","runId":"r1","channel":"default"}');
    });
    await waitFor(() => {
      expect(result.current.runs.r1).toMatchObject({ text: 'Hello', status: 'done' });
    });

    unmount();
  });

  test('changed deps drop the old subscription and its runs', async () => {
    const { result, rerender, unmount } = renderHook(
      ({ thread }: { thread: string }) =>
        usePromptJugglerStream(
          {
            getToken: () => Promise.resolve({ token: 'test-token', url }),
            reconnectDelayMs: { min: 10, max: 50 },
          },
          [thread],
        ),
      { initialProps: { thread: 'thread-1' } },
    );

    const first = await server.connection(1);
    act(() => {
      first.send(
        'token',
        '{"kind":"token","runId":"r1","channel":"default","segment":0,"seq":1,"text":"old thread"}',
      );
    });
    await waitFor(() => {
      expect(result.current.runs.r1).toBeDefined();
    });

    rerender({ thread: 'thread-2' });

    // A new subscription connects and the old thread's runs are gone.
    await server.connection(2);
    await waitFor(() => {
      expect(result.current.runs.r1).toBeUndefined();
    });

    unmount();
  });

  test('keeps a gapped run flagged through its terminal state', async () => {
    const { result, unmount } = renderHook(() =>
      usePromptJugglerStream({
        getToken: () => Promise.resolve({ token: 'test-token', url }),
        reconnectDelayMs: { min: 10, max: 50 },
      }),
    );

    const connection = await server.connection(1);
    act(() => {
      connection.send(
        'token',
        '{"kind":"token","runId":"r1","channel":"default","segment":0,"seq":1,"text":"The ans"}',
      );
      // seq 2 lost: the text freezes at its prefix and the run is flagged.
      connection.send(
        'token',
        '{"kind":"token","runId":"r1","channel":"default","segment":0,"seq":3,"text":"wer"}',
      );
      connection.send('done', '{"kind":"done","runId":"r1","channel":"default"}');
    });
    await waitFor(() => {
      // done is the consumer's cue to fetch the authoritative result.
      expect(result.current.runs.r1).toMatchObject({
        text: 'The ans',
        status: 'done',
        gapped: true,
      });
    });

    unmount();
  });

  test('stale flags streaming runs and leaves settled ones alone', async () => {
    const { result, unmount } = renderHook(() =>
      usePromptJugglerStream({
        getToken: () => Promise.resolve({ token: 'test-token', url }),
        reconnectDelayMs: { min: 10, max: 50 },
      }),
    );

    const connection = await server.connection(1);
    act(() => {
      connection.send(
        'token',
        '{"kind":"token","runId":"r1","channel":"default","segment":0,"seq":1,"text":"done run"}',
      );
      connection.send('done', '{"kind":"done","runId":"r1","channel":"default"}');
      connection.send(
        'token',
        '{"kind":"token","runId":"r2","channel":"default","segment":0,"seq":1,"text":"live run"}',
      );
      connection.send('stale', '{"kind":"stale"}');
    });

    await waitFor(() => {
      // The live run may be missing its tail; the settled one is already true.
      expect(result.current.runs.r2).toMatchObject({ status: 'streaming', gapped: true });
      expect(result.current.runs.r1).toMatchObject({ status: 'done', gapped: false });
    });

    unmount();
  });

  test('a stale replay cannot revive or rewrite a settled run', async () => {
    const { result, unmount } = renderHook(() =>
      usePromptJugglerStream({
        getToken: () => Promise.resolve({ token: 'test-token', url }),
        reconnectDelayMs: { min: 10, max: 50 },
      }),
    );

    const connection = await server.connection(1);
    act(() => {
      connection.send(
        'token',
        '{"kind":"token","runId":"r1","channel":"default","segment":0,"seq":1,"text":"The full answer"}',
      );
      connection.send('done', '{"kind":"done","runId":"r1","channel":"default"}');
      // The server loses history, then replays a partial rebuild of r1.
      connection.send('stale', '{"kind":"stale"}');
      connection.send('reset', '{"kind":"reset","runId":"r1","channel":"default","segment":0,"seq":0}');
      connection.send(
        'token',
        '{"kind":"token","runId":"r1","channel":"default","segment":0,"seq":1,"text":"The par"}',
      );
      connection.send('done', '{"kind":"done","runId":"r1","channel":"default"}');
      // The fence: once r2 shows up, everything above has been processed.
      connection.send(
        'token',
        '{"kind":"token","runId":"r2","channel":"default","segment":0,"seq":1,"text":"next"}',
      );
    });

    await waitFor(() => {
      expect(result.current.runs.r2).toBeDefined();
    });
    expect(result.current.runs.r1).toMatchObject({
      text: 'The full answer',
      status: 'done',
      gapped: false,
    });

    unmount();
  });

  test('a straggler after done flags the run without reviving it', async () => {
    const { result, unmount } = renderHook(() =>
      usePromptJugglerStream({
        getToken: () => Promise.resolve({ token: 'test-token', url }),
        reconnectDelayMs: { min: 10, max: 50 },
      }),
    );

    const connection = await server.connection(1);
    act(() => {
      connection.send(
        'token',
        '{"kind":"token","runId":"r1","channel":"default","segment":0,"seq":1,"text":"Hello wor"}',
      );
      connection.send('done', '{"kind":"done","runId":"r1","channel":"default"}');
      // The tail token lost the race against the terminal frame.
      connection.send(
        'token',
        '{"kind":"token","runId":"r1","channel":"default","segment":0,"seq":2,"text":"ld!"}',
      );
    });
    await waitFor(() => {
      expect(result.current.runs.r1).toMatchObject({
        text: 'Hello wor',
        status: 'done',
        gapped: true,
      });
    });

    unmount();
  });

  test('marks failed runs with the error', async () => {
    const { result, unmount } = renderHook(() =>
      usePromptJugglerStream({
        getToken: () => Promise.resolve({ token: 'test-token', url }),
        reconnectDelayMs: { min: 10, max: 50 },
      }),
    );

    const connection = await server.connection(1);
    act(() => {
      connection.send(
        'failure',
        '{"kind":"failure","runId":"r1","channel":"default","code":"run_failed","message":"boom"}',
      );
    });
    await waitFor(() => {
      expect(result.current.runs.r1).toMatchObject({
        status: 'failed',
        error: { code: 'run_failed', message: 'boom' },
      });
    });

    unmount();
  });
});
