/** One emit-tool payload the run produced, exactly as the model sent it. */
export interface DataItem {
  tool: string;
  payload: unknown;
}

/**
 * Per-run text assembly with the protocol's segment semantics.
 *
 * A run generates in segments: retries re-generate the segment they belong to,
 * while async-tool continuations open a higher one. A `reset` carries the
 * segment being (re-)generated and means "discard buffered content with
 * segment >= mine" — so a retry wipes exactly the partial text it is about to
 * re-send, and a continuation wipes nothing, because earlier segments stay
 * valid and are never re-streamed.
 *
 * The stream itself is an ordered, replayed log: a reconnect resumes exactly
 * where it left off, and the server announces `stale` when it cannot — the one
 * loss signal, handled by the stream, not here. What remains here is honesty
 * at the edges of a subscription:
 *
 * `seq` numbers the events of one attempt — reset first (seq 0), then tokens
 * and emit-data frames interleaved from 1.
 * A run first seen mid-history — after a `stale` rebuild, or a late join —
 * carries its own truncation: a first token above seq 1 means the segment's
 * head was never delivered, and a first sight at a nonzero segment means
 * earlier segments ran unobserved (whether they streamed text is undecidable,
 * so a phantom gapped head below the first-seen segment flags the run). A
 * broken sequence mid-segment should not happen on an ordered log; if it does,
 * the segment freezes at what it has — a true prefix — rather than fabricate
 * text across a hole. Gapped segments withhold everything above them, so
 * `text()` is always a true prefix of the run, and a reset heals by
 * re-streaming from scratch.
 */
export class RunBuffer {
  private readonly segments = new Map<number, Segment>();
  // Emit payloads keyed by the segment that produced them, so a reset drops
  // them alongside that segment's text — a retry re-emits, and stale wipes all.
  private readonly dataBySegment = new Map<number, DataItem[]>();

  /** Feed a token delta to its segment; the result says what became of it. */
  append(segment: number, seq: number, text: string): 'appended' | 'gap' | 'dropped' {
    const { outcome, target } = this.advance(segment, seq);
    if (target) {
      target.text += text;
    }

    return outcome;
  }

  /**
   * Advance a segment's sequence by one event, applying the contiguity rules a
   * token and a data frame share — both ride the same per-segment seq, reset
   * first (seq 0), events from 1. Returns the event's fate, for the caller's
   * gap/text signalling, and the segment to apply the payload to — or undefined
   * when the event fell outside a live, in-order slot and its payload must be
   * withheld so the maintained view stays a true prefix.
   */
  private advance(
    segment: number,
    seq: number,
  ): { outcome: 'appended' | 'gap' | 'dropped'; target: Segment | undefined } {
    const orphaned = this.ensureHead(segment);
    const existing = this.segments.get(segment);
    if (!existing) {
      // First sight of this segment. Its attempt opened with a reset at seq 0,
      // so a first event at seq 1 means we missed only that reset — harmless,
      // since a reset on a never-buffered segment has nothing to wipe. Anything
      // later and the segment's head is already lost.
      if (seq === 1) {
        const fresh: Segment = { text: '', lastSeq: seq, gapped: false };
        this.segments.set(segment, fresh);

        // The segment itself is whole, but a phantom head just flagged the
        // run: that loss surfaces as a gap, not as a clean-looking append.
        return { outcome: orphaned ? 'gap' : 'appended', target: fresh };
      }
      this.segments.set(segment, { text: '', lastSeq: seq, gapped: true });

      return { outcome: 'gap', target: undefined };
    }
    if (existing.gapped) {
      return { outcome: 'dropped', target: undefined };
    }
    if (seq !== existing.lastSeq + 1) {
      existing.gapped = true;

      return { outcome: 'gap', target: undefined };
    }
    existing.lastSeq = seq;

    return { outcome: 'appended', target: existing };
  }

  /**
   * Apply a reset: drop segments >= the reset's, and expect that segment's
   * tokens to restart from the reset's seq. Reports whether the run's visible
   * state changed — text discarded or a gap healed — and whether this first
   * sight of the run created the phantom head.
   */
  reset(segment: number, seq: number): { changed: boolean; orphaned: boolean } {
    const orphaned = this.ensureHead(segment);
    let changed = orphaned;
    for (const [s, state] of this.segments) {
      if (s >= segment) {
        this.segments.delete(s);
        changed ||= state.text !== '' || state.gapped;
      }
    }
    this.segments.set(segment, { text: '', lastSeq: seq, gapped: false });

    for (const s of this.dataBySegment.keys()) {
      if (s >= segment) {
        this.dataBySegment.delete(s);
      }
    }

    return { changed, orphaned };
  }

  /**
   * Record an emit payload against the segment that produced it. A data frame
   * rides the same per-segment seq as tokens, so this advances the segment's
   * sequence exactly as a token would: an in-order payload is kept, and a hole
   * — a token lost just before this frame — freezes the segment rather than let
   * the following token read as contiguous. Returns the frame's fate so the
   * caller can signal a gap the same way it does for a token.
   */
  addData(segment: number, seq: number, item: DataItem): 'appended' | 'gap' | 'dropped' {
    const { outcome, target } = this.advance(segment, seq);
    if (target) {
      const list = this.dataBySegment.get(segment);
      if (list) {
        list.push(item);
      } else {
        this.dataBySegment.set(segment, [item]);
      }
    }

    return outcome;
  }

  /**
   * The run's emit payloads so far, in emission order (by segment, then
   * arrival) — and, like {@link text}, a true prefix: segments beyond the first
   * gap are withheld, because any emit calls before that unresolved hole are
   * unknown and the list would otherwise present a suffix as the whole.
   */
  data(): DataItem[] {
    const frontier = this.frontier();

    return [...this.dataBySegment.entries()]
      .filter(([segment]) => segment <= frontier)
      .sort(([a], [b]) => a - b)
      .flatMap(([, items]) => items);
  }

  /**
   * A run first seen at a nonzero segment ran earlier segments unobserved —
   * text-bearing or not, nothing can ever tell. The phantom head (segment -1,
   * below any real one) is permanently gapped, so the withholding and verdict
   * logic flags the run without a special case.
   */
  private ensureHead(segment: number): boolean {
    if (this.segments.size > 0 || segment <= 0) {
      return false;
    }
    this.segments.set(-1, { text: '', lastSeq: 0, gapped: true });

    return true;
  }

  /**
   * The highest segment whose content is still provable — the first gapped
   * segment (inclusive), or Infinity when nothing is gapped. Segments past it
   * sit on the far side of a hole, so both text and data withhold them.
   */
  private frontier(): number {
    const gapped = [...this.segments.entries()]
      .filter(([, state]) => state.gapped)
      .map(([segment]) => segment);

    return gapped.length === 0 ? Infinity : Math.min(...gapped);
  }

  /**
   * The run's text so far — always a true prefix of the run. Segments join in
   * order up to and including the first gapped one, which contributes its
   * frozen prefix; anything above it would sit on the far side of a hole.
   */
  text(): string {
    const frontier = this.frontier();

    return [...this.segments.entries()]
      .filter(([segment]) => segment <= frontier)
      .sort(([a], [b]) => a - b)
      .map(([, state]) => state.text)
      .join('');
  }

  /** Whether any segment lost events — `text()` is then an incomplete prefix. */
  gapped(): boolean {
    return [...this.segments.values()].some((state) => state.gapped);
  }
}

interface Segment {
  text: string;
  lastSeq: number;
  gapped: boolean;
}
