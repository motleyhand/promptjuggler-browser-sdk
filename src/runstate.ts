import type { DoneEvent, FailureEvent, GapEvent, TextEvent } from './stream';

/** One run's live view: the streamed text so far and where it stands. */
export interface RunState {
  text: string;
  channel: string;
  status: 'streaming' | 'done' | 'failed';
  /** Events were (or may have been) lost: fetch the run for the full result. */
  gapped?: boolean;
  error?: { code: string; message: string };
}

export type RunStates = Record<string, RunState>;

/**
 * Pure reducers folding stream events into per-run view state — the one
 * definition of that fold, shared by the React and Angular adapters so their
 * semantics cannot drift. Every reducer returns the previous object untouched
 * when nothing changes, so identity-based change detection short-circuits.
 *
 * The load-bearing rule: a run that reached a terminal state is settled.
 * Later text and terminal events for it are re-publishes or stale-replay
 * rebuilds and change nothing; only a gap event may still amend it, because a
 * straggling token can retroactively prove a settled text incomplete.
 */
export function applyText(runs: RunStates, { runId, channel, text, gapped }: TextEvent): RunStates {
  if (settled(runs[runId])) {
    return runs;
  }

  return { ...runs, [runId]: { text, channel, status: 'streaming', gapped } };
}

export function applyDone(runs: RunStates, { runId, channel, gapped }: DoneEvent): RunStates {
  if (settled(runs[runId])) {
    return runs;
  }

  // The terminal verdict comes from the event, not the last text event — only
  // the stream knows whether the tail may have been lost.
  return {
    ...runs,
    [runId]: { text: runs[runId]?.text ?? '', channel, status: 'done', gapped },
  };
}

export function applyFailure(
  runs: RunStates,
  { runId, channel, code, message, gapped }: FailureEvent,
): RunStates {
  if (settled(runs[runId])) {
    return runs;
  }

  return {
    ...runs,
    [runId]: {
      text: runs[runId]?.text ?? '',
      channel,
      status: 'failed',
      gapped,
      error: { code, message },
    },
  };
}

/**
 * Usually followed by a text event carrying the same flag — but after a
 * terminal event, this is the only signal that a straggling token proved the
 * finished text incomplete.
 */
export function applyGap(runs: RunStates, { runId, channel }: GapEvent): RunStates {
  return {
    ...runs,
    [runId]: {
      text: '',
      channel,
      status: 'streaming',
      ...runs[runId],
      gapped: true,
    },
  };
}

/**
 * The server lost history: every run still streaming may be missing its tail.
 * Their events rebuild them if retained; settled runs are already true.
 */
export function applyStale(runs: RunStates): RunStates {
  const flagged = Object.entries(runs).filter(([, run]) => run.status === 'streaming' && !run.gapped);
  if (flagged.length === 0) {
    return runs;
  }

  return Object.fromEntries(
    Object.entries(runs).map(([runId, run]) => [
      runId,
      run.status === 'streaming' ? { ...run, gapped: true } : run,
    ]),
  );
}

/** A run that reached a terminal state; nothing the stream replays reopens it. */
function settled(run: RunState | undefined): boolean {
  return run !== undefined && run.status !== 'streaming';
}
