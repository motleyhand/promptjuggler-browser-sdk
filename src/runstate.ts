import type {
  DataEvent,
  DataItem,
  DoneEvent,
  FailureEvent,
  GapEvent,
  TextEvent,
  TranscriptEvent,
  TranscriptItem,
} from './stream';

/** One run's live view: the streamed content so far and where it stands. */
export interface RunState {
  text: string;
  channel: string;
  status: 'streaming' | 'done' | 'failed';
  /** Emit-tool payloads this run produced, if any — structured data beside the text. */
  data?: DataItem[];
  /**
   * The same run as a renderable sequence — text, tool chips, emit payloads in
   * position. `text` is the prose alone; render this instead when the UI shows
   * tool activity. Same shape as a fetched run's `transcript`.
   *
   * On a `failed` run it keeps what the failed attempt produced, exactly as
   * `text` and `data` do, while the API reports a failed run as having no result
   * at all (`transcript: []`, `output: null`, `emitted: []`). Showing the partial
   * answer beside the error beats blanking it, but a component that refetches
   * after failure will see it disappear — branch on `status` if that matters.
   */
  transcript?: TranscriptItem[];
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

  // Carry `data` and `transcript` across: text, payloads and the positioned
  // sequence are independent views of the same run, each maintained by its own
  // event.
  return {
    ...runs,
    [runId]: {
      text,
      channel,
      status: 'streaming',
      gapped,
      data: runs[runId]?.data,
      transcript: runs[runId]?.transcript,
    },
  };
}

/** The run's maintained emit payloads changed; text and status are untouched. */
export function applyData(runs: RunStates, { runId, channel, data }: DataEvent): RunStates {
  if (settled(runs[runId])) {
    return runs;
  }

  const previous = runs[runId] ?? { text: '', channel, status: 'streaming' as const };

  return { ...runs, [runId]: { ...previous, data } };
}

/** The run's maintained transcript changed; text and status are untouched. */
export function applyTranscript(runs: RunStates, { runId, channel, transcript }: TranscriptEvent): RunStates {
  if (settled(runs[runId])) {
    return runs;
  }

  const previous = runs[runId] ?? { text: '', channel, status: 'streaming' as const };

  return { ...runs, [runId]: { ...previous, transcript } };
}

export function applyDone(runs: RunStates, { runId, channel, gapped }: DoneEvent): RunStates {
  if (settled(runs[runId])) {
    return runs;
  }

  // The terminal verdict comes from the event, not the last text event — only
  // the stream knows whether the tail may have been lost.
  return {
    ...runs,
    [runId]: {
      text: runs[runId]?.text ?? '',
      channel,
      status: 'done',
      gapped,
      data: runs[runId]?.data,
      transcript: runs[runId]?.transcript,
    },
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
      data: runs[runId]?.data,
      transcript: runs[runId]?.transcript,
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
  // A streaming run needs amending if it isn't already flagged, or still holds
  // content the wiped stream can no longer vouch for. Text self-heals on replay
  // — every token re-publishes it — but data and the transcript only move on
  // their own frames, so a rebuild that drops an emit or a tool would otherwise
  // settle with stale cards and chips. Clear them here and let fresh frames
  // restore whatever still applies.
  const affected = Object.values(runs).some(
    (run) =>
      run.status === 'streaming' && (!run.gapped || run.data !== undefined || run.transcript !== undefined),
  );
  if (!affected) {
    return runs;
  }

  return Object.fromEntries(
    Object.entries(runs).map(([runId, run]) => [
      runId,
      run.status === 'streaming' ? { ...run, gapped: true, data: undefined, transcript: undefined } : run,
    ]),
  );
}

/** A run that reached a terminal state; nothing the stream replays reopens it. */
function settled(run: RunState | undefined): boolean {
  return run !== undefined && run.status !== 'streaming';
}
