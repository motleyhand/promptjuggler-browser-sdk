import { describe, expect, test } from 'vitest';
import { applyData, applyDone, applyGap, applyStale, applyText } from '../src/runstate';
import type { RunState, RunStates } from '../src/runstate';

const streaming: RunStates = {
  r1: { text: 'Hello', channel: 'default', status: 'streaming', gapped: false },
};
const settledRun: RunState = { text: 'Hello', channel: 'default', status: 'done', gapped: false };
const done: RunStates = { r1: settledRun };

describe('run state reducers', () => {
  test('settled runs ignore later text and terminal events', () => {
    expect(applyText(done, { runId: 'r1', channel: 'default', text: 'par', gapped: false })).toBe(done);
    expect(applyDone(done, { runId: 'r1', channel: 'default', gapped: true })).toBe(done);
  });

  test('a gap amends a settled run without reopening it', () => {
    const next = applyGap(done, { runId: 'r1', channel: 'default', segment: 0 });
    expect(next.r1).toMatchObject({ text: 'Hello', status: 'done', gapped: true });
  });

  test('stale flags streaming runs and leaves settled ones alone', () => {
    const next = applyStale({ ...streaming, r2: settledRun });
    expect(next.r1?.gapped).toBe(true);
    expect(next.r2).toBe(settledRun);
  });

  test('stale clears emit payloads a streaming run can no longer prove', () => {
    const withData: RunStates = {
      r1: {
        text: 'Hello',
        channel: 'default',
        status: 'streaming',
        gapped: false,
        data: [{ tool: 'cards', payload: { ids: [1] } }],
      },
    };
    const next = applyStale(withData);
    // Flagged and its cards dropped: a clean rebuild that no longer emits must
    // not settle the run with the pre-stale payload still attached.
    expect(next.r1).toMatchObject({ gapped: true });
    expect(next.r1?.data).toBeUndefined();
  });

  test('applyData sets data and preserves text; applyText preserves data', () => {
    const withText = applyText({}, { runId: 'r1', channel: 'default', text: 'Hi', gapped: false });
    const withData = applyData(withText, {
      runId: 'r1',
      channel: 'default',
      data: [{ tool: 'cards', payload: { ids: [1] } }],
    });
    expect(withData.r1).toMatchObject({ text: 'Hi', data: [{ tool: 'cards', payload: { ids: [1] } }] });

    // A later text delta must not wipe the maintained data.
    const next = applyText(withData, { runId: 'r1', channel: 'default', text: 'Hi there', gapped: false });
    expect(next.r1).toMatchObject({ text: 'Hi there', data: [{ tool: 'cards', payload: { ids: [1] } }] });
  });

  test('applyData ignores settled runs', () => {
    expect(applyData(done, { runId: 'r1', channel: 'default', data: [{ tool: 't', payload: 1 }] })).toBe(
      done,
    );
  });

  test('reducers return the same object when nothing changes', () => {
    expect(applyStale(done)).toBe(done);
  });
});
