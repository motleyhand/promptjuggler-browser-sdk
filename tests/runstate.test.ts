import { describe, expect, test } from 'vitest';
import { applyDone, applyGap, applyStale, applyText } from '../src/runstate';
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

  test('reducers return the same object when nothing changes', () => {
    expect(applyStale(done)).toBe(done);
  });
});
