import { describe, expect, test } from 'vitest';
import { RunBuffer } from '../src/buffer';

describe('RunBuffer', () => {
  test('appends within a segment and orders segments', () => {
    const buffer = new RunBuffer();
    buffer.append(0, 1, 'Hello');
    buffer.append(3, 1, ' world');
    buffer.append(3, 2, '!');
    expect(buffer.text()).toBe('Hello world!');
    expect(buffer.gapped()).toBe(false);
  });

  test('reset discards the segment being re-generated but keeps earlier ones', () => {
    // The async-tool shape: segment 0 streamed before the tool call and stays
    // valid; segment 3 is the continuation, which a retry re-generates.
    const buffer = new RunBuffer();
    buffer.append(0, 1, 'Let me look that up… ');
    buffer.append(3, 1, 'half an ans');

    expect(buffer.reset(3, 0).changed).toBe(true);
    expect(buffer.text()).toBe('Let me look that up… ');

    buffer.append(3, 1, 'the full answer');
    expect(buffer.text()).toBe('Let me look that up… the full answer');
  });

  test('a continuation reset discards nothing', () => {
    const buffer = new RunBuffer();
    buffer.append(0, 1, 'before the tool');
    expect(buffer.reset(3, 0).changed).toBe(false);
    expect(buffer.text()).toBe('before the tool');
  });

  test('a seq gap freezes the segment at its prefix and drops the tail', () => {
    const buffer = new RunBuffer();
    expect(buffer.append(0, 1, 'Hel')).toBe('appended');
    // seq 2 lost: appending "rld" here would fabricate "Helrld".
    expect(buffer.append(0, 3, 'rld')).toBe('gap');
    expect(buffer.append(0, 4, '!')).toBe('dropped');
    expect(buffer.text()).toBe('Hel');
    expect(buffer.gapped()).toBe(true);
  });

  test('a reset heals a gapped segment', () => {
    const buffer = new RunBuffer();
    buffer.append(0, 1, 'Hel');
    buffer.append(0, 3, 'rld');

    expect(buffer.reset(0, 0).changed).toBe(true);
    expect(buffer.gapped()).toBe(false);
    expect(buffer.append(0, 1, 'Hello!')).toBe('appended');
    expect(buffer.text()).toBe('Hello!');
  });

  test('a fresh segment starting at seq 1 missed only the harmless reset', () => {
    // Each attempt opens with a reset at seq 0; on a never-buffered segment
    // that reset wipes nothing, so a token at seq 1 is a complete head.
    const buffer = new RunBuffer();
    expect(buffer.append(0, 1, 'Hi')).toBe('appended');
    expect(buffer.gapped()).toBe(false);
  });

  test('a fresh segment starting past seq 1 already lost its head', () => {
    const buffer = new RunBuffer();
    expect(buffer.append(0, 5, 'mid-stream')).toBe('gap');
    expect(buffer.text()).toBe('');
    expect(buffer.gapped()).toBe(true);
  });

  test('a run first seen at a continuation is flagged: the head may have streamed unseen', () => {
    const buffer = new RunBuffer();
    // First frame for this run is a continuation's reset — segment 0 already
    // ran, and whether it streamed text is undecidable from here.
    expect(buffer.reset(3, 0)).toMatchObject({ changed: true, orphaned: true });
    expect(buffer.append(3, 1, 'The answer is 42.')).toBe('appended');
    expect(buffer.text()).toBe('');
    expect(buffer.gapped()).toBe(true);
  });

  test('a run first seen at a nonzero segment token is flagged the same way', () => {
    const buffer = new RunBuffer();
    // The segment itself is whole, but the phantom head makes this a gap:
    // subscribers of the loss signal must hear about it.
    expect(buffer.append(3, 1, 'The answer is 42.')).toBe('gap');
    expect(buffer.text()).toBe('');
    expect(buffer.gapped()).toBe(true);
  });

  test('a continuation after a buffered head is clean', () => {
    const buffer = new RunBuffer();
    buffer.reset(0, 0); // even a text-less head segment leaves its mark
    buffer.reset(3, 0);
    buffer.append(3, 1, 'The answer is 42.');
    expect(buffer.text()).toBe('The answer is 42.');
    expect(buffer.gapped()).toBe(false);
  });

  test('segments above a gapped one are withheld so the text stays a true prefix', () => {
    const buffer = new RunBuffer();
    buffer.append(0, 1, 'Let me chec');
    buffer.append(0, 3, 'k that'); // seg 0's tail is lost — frozen at its prefix

    // The async-tool continuation streams on, but its text sits on the far
    // side of the hole: joining it would no longer be a prefix of the run.
    buffer.reset(3, 0);
    expect(buffer.append(3, 1, 'The answer is 42.')).toBe('appended');
    expect(buffer.text()).toBe('Let me chec');
    expect(buffer.gapped()).toBe(true);
  });

  test('data payloads accumulate in segment then arrival order', () => {
    const buffer = new RunBuffer();
    buffer.reset(0, 0); // the run opens on segment 0
    // A later continuation emits before segment 0's own payloads: the sort
    // still orders by segment, then by arrival within a segment.
    buffer.addData(3, 1, { tool: 'chart', payload: { n: 2 } });
    buffer.addData(0, 1, { tool: 'cards', payload: { ids: [1] } });
    buffer.addData(0, 2, { tool: 'cards', payload: { ids: [2] } });

    expect(buffer.data()).toEqual([
      { tool: 'cards', payload: { ids: [1] } },
      { tool: 'cards', payload: { ids: [2] } },
      { tool: 'chart', payload: { n: 2 } },
    ]);
  });

  test('a reset discards data for the re-generated segment and above', () => {
    const buffer = new RunBuffer();
    buffer.reset(0, 0);
    buffer.addData(0, 1, { tool: 'cards', payload: 'kept' });
    buffer.reset(3, 0);
    buffer.addData(3, 1, { tool: 'cards', payload: 'dropped' });

    buffer.reset(3, 0); // segment 3 re-generates
    expect(buffer.data()).toEqual([{ tool: 'cards', payload: 'kept' }]);
  });

  test('a data frame between tokens advances the seq so the next token is not a false gap', () => {
    // The runner numbers a data event in the same per-segment seq as tokens
    // (reset 0, token 1, data 2, continuation token 3). The data frame must
    // consume its slot, or the token at seq 3 reads as a skip from 1.
    const buffer = new RunBuffer();
    buffer.reset(0, 0);
    expect(buffer.append(0, 1, 'Pulling those up')).toBe('appended');
    expect(buffer.addData(0, 2, { tool: 'cards', payload: { ids: [7] } })).toBe('appended');
    expect(buffer.append(0, 3, ' — here.')).toBe('appended');
    expect(buffer.text()).toBe('Pulling those up — here.');
    expect(buffer.gapped()).toBe(false);
    expect(buffer.data()).toEqual([{ tool: 'cards', payload: { ids: [7] } }]);
  });

  test('a data frame detects a token lost just before it', () => {
    // The mirror case: a hole in front of a data frame must freeze the segment,
    // not slip through unnoticed because data ignored the sequence.
    const buffer = new RunBuffer();
    buffer.reset(0, 0);
    buffer.append(0, 1, 'Pulling');
    // seq 2 (a token) never arrived; the data frame comes at seq 3.
    expect(buffer.addData(0, 3, { tool: 'cards', payload: {} })).toBe('gap');
    expect(buffer.gapped()).toBe(true);
    // The payload sits across a hole, so it is withheld from the maintained list.
    expect(buffer.data()).toEqual([]);
  });

  test('data up to the first gap is kept; beyond it is withheld like text', () => {
    // A hole in segment 0, then a continuation in segment 3 that emits. Emits
    // between the hole and the continuation are unknown, so segment 3's payload
    // must not present as the full list — mirror what text() withholds.
    const buffer = new RunBuffer();
    buffer.reset(0, 0);
    buffer.append(0, 1, 'Let me chec');
    buffer.addData(0, 2, { tool: 'cards', payload: { ids: [1] } }); // before the hole — kept
    buffer.append(0, 4, 'k'); // seq 3 lost — segment 0 is gapped
    buffer.reset(3, 0);
    buffer.addData(3, 1, { tool: 'cards', payload: { ids: [5] } }); // beyond the hole — withheld

    expect(buffer.gapped()).toBe(true);
    expect(buffer.text()).toBe('Let me chec');
    expect(buffer.data()).toEqual([{ tool: 'cards', payload: { ids: [1] } }]);
  });

  test('a regressing sequence freezes the segment rather than fabricate a splice', () => {
    // Cannot happen on an ordered, replayed log — a retry's reset always
    // precedes its tokens. If it happens anyway, freezing is the honest move.
    const buffer = new RunBuffer();
    buffer.append(0, 1, 'half an ans');
    expect(buffer.append(0, 1, 'the answer')).toBe('gap');
    expect(buffer.text()).toBe('half an ans');
    expect(buffer.gapped()).toBe(true);
  });
});
