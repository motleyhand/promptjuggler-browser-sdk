import { useEffect, useRef, useState } from 'react';
import { PromptJugglerStream, type PromptJugglerStreamOptions } from './stream';

/** One run's live view: the streamed text so far and where it stands. */
export interface RunState {
  text: string;
  channel: string;
  status: 'streaming' | 'done' | 'failed';
  /** Events were (or may have been) lost: fetch the run for the full result. */
  gapped?: boolean;
  error?: { code: string; message: string };
}

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
/** A run that reached a terminal state; nothing the stream replays reopens it. */
function settled(run: RunState | undefined): boolean {
  return run !== undefined && run.status !== 'streaming';
}

export function usePromptJugglerStream(
  options: PromptJugglerStreamOptions,
  deps: readonly unknown[] = [],
): UsePromptJugglerStream {
  const [connected, setConnected] = useState(false);
  const [runs, setRuns] = useState<Record<string, RunState>>({});

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
      stream.on('text', ({ runId, channel, text, gapped }) => {
        // A stale replay rebuilds history a run may have already finished:
        // settled runs stay settled, showing their complete text — never a
        // partial rebuild.
        setRuns((previous) =>
          settled(previous[runId])
            ? previous
            : { ...previous, [runId]: { text, channel, status: 'streaming', gapped } },
        );
      }),
      stream.on('stale', () => {
        // The server lost history: every run still streaming may be missing
        // its tail. Their events rebuild them if retained; terminal runs are
        // already settled and stay untouched.
        setRuns((previous) =>
          Object.fromEntries(
            Object.entries(previous).map(([runId, run]) => [
              runId,
              run.status === 'streaming' ? { ...run, gapped: true } : run,
            ]),
          ),
        );
      }),
      stream.on('gap', ({ runId, channel }) => {
        // Usually followed by a text event carrying the same flag — but after
        // a terminal event, this is the only signal that a straggling token
        // proved the finished text incomplete.
        setRuns((previous) => ({
          ...previous,
          [runId]: {
            text: '',
            channel,
            status: 'streaming',
            ...previous[runId],
            gapped: true,
          },
        }));
      }),
      stream.on('done', ({ runId, channel, gapped }) => {
        // The terminal verdict comes from the event, not the last text event —
        // only the stream knows whether the tail may have been lost. A run
        // already settled stays as it is: later arrivals are re-publishes or
        // stale-replay rebuilds, and flag upgrades ride the gap event.
        setRuns((previous) =>
          settled(previous[runId])
            ? previous
            : {
                ...previous,
                [runId]: { text: previous[runId]?.text ?? '', channel, status: 'done', gapped },
              },
        );
      }),
      stream.on('failure', ({ runId, channel, code, message, gapped }) => {
        setRuns((previous) =>
          settled(previous[runId])
            ? previous
            : {
                ...previous,
                [runId]: {
                  text: previous[runId]?.text ?? '',
                  channel,
                  status: 'failed',
                  gapped,
                  error: { code, message },
                },
              },
        );
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
