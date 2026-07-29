import { readdirSync, readFileSync } from 'node:fs';
import { join } from 'node:path';
import { afterEach, beforeEach, describe, expect, test } from 'vitest';
import type { PromptJugglerStream, TranscriptEvent, TranscriptItem } from '../src/stream';
import { connect, record, SseServer } from './helpers';

interface Vector {
  description: string;
  streamNote?: string;
  events: Record<string, unknown>[] | null;
  expected: TranscriptItem[];
}

const VECTOR_DIR = join(import.meta.dirname, '../../../vectors/transcript');

const vectors = readdirSync(VECTOR_DIR)
  .filter((file) => file.endsWith('.json'))
  .map((file) => ({
    name: file.replace(/\.json$/, ''),
    vector: JSON.parse(readFileSync(join(VECTOR_DIR, file), 'utf8')) as Vector,
  }));

/**
 * The browser half of the shared cross-language vectors in `vectors/transcript/`.
 * The backend asserts the same `expected` by projecting the stored interaction,
 * which is the whole point: what a chat client renders live and what it renders
 * after a refetch have to be the same thing.
 *
 * Vectors with `events: null` exercise something the stream cannot carry —
 * citations, search queries — and are asserted by the backend alone. Their
 * `streamNote` says which.
 */
describe('transcript vectors', () => {
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

  test('the vector directory is non-empty and every skip is explained', () => {
    expect(vectors.length).toBeGreaterThan(0);
    vectors
      .filter(({ vector }) => vector.events === null)
      .forEach(({ name, vector }) => {
        expect(vector.streamNote, `${name} skips the stream without saying why`).toBeTruthy();
      });
  });

  vectors
    .filter(({ vector }) => vector.events !== null)
    .forEach(({ name, vector }) => {
      test(`${name}: ${vector.description}`, async () => {
        stream = connect(url);
        const transcripts = record<TranscriptEvent>();
        stream.on('transcript', transcripts.push);
        stream.connect();

        const connection = await server.connection(1);
        const events = vector.events ?? [];
        events.forEach((event, index) => {
          connection.send(String(event.kind), JSON.stringify(event), index + 1);
        });

        // The last transcript event carries the whole maintained list, so waiting
        // for the fold to settle is waiting for the frames to land.
        await expect
          .poll(() => transcripts.events[transcripts.events.length - 1]?.transcript)
          .toEqual(vector.expected);
      });
    });
});
