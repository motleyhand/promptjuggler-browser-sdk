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
