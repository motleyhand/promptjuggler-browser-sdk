import { useEffect, useRef, useState } from 'react';
import {
  applyData,
  applyDone,
  applyFailure,
  applyGap,
  applyStale,
  applyText,
  applyTranscript,
} from './runstate';
import type { RunState, RunStates } from './runstate';
import { PromptJugglerStream, type PromptJugglerStreamOptions } from './stream';

export type { RunState } from './runstate';

export interface UsePromptJugglerStream {
  /** Whether the SSE connection is currently open. */
  connected: boolean;
  /** Live state per run id — render `runs[runId].text` and you have a chatbot. */
  runs: Record<string, RunState>;
}

/**
 * Subscribe a component to a thread's token stream. Connects on mount and
 * whenever `deps` change — pass the values that identify the subscription
 * (typically the thread id) so a component that moves between threads drops
 * the old stream and its runs. `options` itself is read through a ref, so an
 * inline object literal (and an inline `getToken`) is fine and never causes a
 * reconnect by identity alone.
 */
export function usePromptJugglerStream(
  options: PromptJugglerStreamOptions,
  deps: readonly unknown[] = [],
): UsePromptJugglerStream {
  const [connected, setConnected] = useState(false);
  const [runs, setRuns] = useState<RunStates>({});

  const latest = useRef(options);
  latest.current = options;

  useEffect(() => {
    // A fresh subscription target: the previous thread's runs are not ours to show.
    setRuns({});
    setConnected(false);

    const stream = new PromptJugglerStream({
      // Delegate through the ref so the stream always calls the newest getToken
      // without the effect depending on (and reconnecting for) every render.
      getToken: () => latest.current.getToken(),
      streamUrl: latest.current.streamUrl,
      channels: latest.current.channels,
      reconnectDelayMs: latest.current.reconnectDelayMs,
    });

    const subscriptions = [
      stream.on('connected', () => {
        setConnected(true);
      }),
      stream.on('disconnected', () => {
        setConnected(false);
      }),
      stream.on('text', (event) => {
        setRuns((previous) => applyText(previous, event));
      }),
      stream.on('data', (event) => {
        setRuns((previous) => applyData(previous, event));
      }),
      stream.on('transcript', (event) => {
        setRuns((previous) => applyTranscript(previous, event));
      }),
      stream.on('stale', () => {
        setRuns(applyStale);
      }),
      stream.on('gap', (event) => {
        setRuns((previous) => applyGap(previous, event));
      }),
      stream.on('done', (event) => {
        setRuns((previous) => applyDone(previous, event));
      }),
      stream.on('failure', (event) => {
        setRuns((previous) => applyFailure(previous, event));
      }),
    ];

    stream.connect();

    return () => {
      subscriptions.forEach((unsubscribe) => {
        unsubscribe();
      });
      stream.disconnect();
    };
    // Deliberately the caller's deps, not the options object: the caller names
    // what identifies the subscription; everything else rides the ref above.
  }, deps);

  return { connected, runs };
}
