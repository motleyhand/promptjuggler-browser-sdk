/** One emit-tool payload the run produced, exactly as the model sent it. */
export interface DataItem {
  tool: string;
  payload: unknown;
}

/** A source the model cited. Only ever populated by a fetched run — see {@link TranscriptItem}. */
export interface Citation {
  title: string;
  url: string;
}

/** How a tool call ended. `pending` is a call that has not reported back yet. */
export type ToolStatus = 'ok' | 'error' | 'pending';

/** What a provider-side search reported: its queries and the sources it returned. */
export interface SearchResult {
  queries: string[];
  citations: Citation[];
}

/**
 * One entry in a run's transcript — the same shape `getPromptRun` returns as
 * `transcript`, so a chat client renders live and refetched runs with one
 * component.
 *
 * One field the API populates is always empty here, because the stream cannot
 * carry it: a **text** block's `citations`, which the provider attaches to the
 * finished message rather than to the deltas it was streamed as. Fetch the run
 * when you need those. A tool's own `queries` and `citations` do arrive live,
 * on the frame that resolves it.
 */
export type TranscriptItem =
  | { type: 'text'; content: string; citations: Citation[] }
  | { type: 'tool'; name: string; status: ToolStatus; queries: string[]; citations: Citation[] }
  | { type: 'data'; tool: string; payload: unknown };

/**
 * What a segment actually stores. Identical to TranscriptItem except that a tool
 * carries the `ref` that a later tool_end resolves it by — a correlator the
 * client needs and the rendered transcript has no business exposing.
 */
type BufferedItem =
  | { type: 'text'; content: string }
  | {
      type: 'tool';
      ref: string;
      name: string;
      status: ToolStatus;
      queries: string[];
      citations: Citation[];
    }
  | { type: 'data'; tool: string; payload: unknown };

/**
 * Per-run transcript assembly with the protocol's segment semantics.
 *
 * A run generates in segments: retries re-generate the segment they belong to,
 * while async-tool continuations open a higher one. A `reset` carries the
 * segment being (re-)generated and means "discard buffered content with
 * segment >= mine" — so a retry wipes exactly the partial content it is about
 * to re-send, and a continuation wipes nothing, because earlier segments stay
 * valid and are never re-streamed.
 *
 * The stream itself is an ordered, replayed log: a reconnect resumes exactly
 * where it left off, and the server announces `stale` when it cannot — the one
 * loss signal, handled by the stream, not here. What remains here is honesty
 * at the edges of a subscription:
 *
 * `seq` numbers the events of one attempt — reset first (seq 0), then tokens,
 * emit-data frames and tool_starts interleaved from 1. (A tool_end carries no
 * seq: it is a status update found by ref, not a position, which is what lets
 * the backend publish it for async calls it resolves long after the runner
 * returned.)
 * A run first seen mid-history — after a `stale` rebuild, or a late join —
 * carries its own truncation: a first event above seq 1 means the segment's
 * head was never delivered, and a first sight at a nonzero segment means
 * earlier segments ran unobserved (whether they streamed anything is
 * undecidable, so a phantom gapped head below the first-seen segment flags the
 * run). A broken sequence mid-segment should not happen on an ordered log; if
 * it does, the segment freezes at what it has — a true prefix — rather than
 * fabricate content across a hole. Gapped segments withhold everything above
 * them, so {@link text} is always a true prefix of the run, and a reset heals
 * by re-streaming from scratch.
 *
 * {@link text} and {@link data} are views over the same item list rather than
 * state of their own, so the three cannot drift.
 */
export class RunBuffer {
  private readonly segments = new Map<number, Segment>();
  // Resolutions whose chip has not arrived yet — see endTool.
  private readonly earlyEnds = new Map<string, { status: ToolStatus } & SearchResult>();

  /** Feed a token delta to its segment; the result says what became of it. */
  append(segment: number, seq: number, text: string): 'appended' | 'gap' | 'dropped' {
    const { outcome, target } = this.advance(segment, seq);
    if (target) {
      // Merge into the trailing text item: consecutive deltas are one block, and
      // only a tool or a payload between them starts a new one — the same rule
      // the backend applies to adjacent assistant messages.
      const last = target.items[target.items.length - 1];
      if (last?.type === 'text') {
        last.content += text;
      } else {
        target.items.push({ type: 'text', content: text });
      }
    }

    return outcome;
  }

  /**
   * Advance a segment's sequence by one event, applying the contiguity rules
   * every in-band frame shares — token, data and tool_start all ride the same
   * per-segment seq, reset first (seq 0), events from 1. Returns the event's
   * fate, for the caller's gap/text signalling, and the segment to apply the
   * payload to — or undefined when the event fell outside a live, in-order slot
   * and its payload must be withheld so the maintained view stays a true prefix.
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
        const fresh: Segment = { items: [], lastSeq: seq, gapped: false };
        this.segments.set(segment, fresh);

        // The segment itself is whole, but a phantom head just flagged the
        // run: that loss surfaces as a gap, not as a clean-looking append.
        return { outcome: orphaned ? 'gap' : 'appended', target: fresh };
      }
      this.segments.set(segment, { items: [], lastSeq: seq, gapped: true });

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
   * events to restart from the reset's seq. Reports whether the run's visible
   * state changed — content discarded or a gap healed — and whether this first
   * sight of the run created the phantom head.
   */
  reset(segment: number, seq: number): { changed: boolean; orphaned: boolean } {
    const orphaned = this.ensureHead(segment);
    let changed = orphaned;
    let discarded = false;
    for (const [s, state] of this.segments) {
      if (s >= segment) {
        this.segments.delete(s);
        discarded = true;
        changed ||= state.items.length > 0 || state.gapped;
      }
    }
    this.segments.set(segment, { items: [], lastSeq: seq, gapped: false });
    if (discarded) {
      // Only a reset that threw an attempt away invalidates a held status: that
      // attempt re-calls its tools and publishes fresh ends, and an old status
      // must not settle the new chip. The resets that discard nothing — the one
      // opening the run, and a continuation opening a higher segment — abandon
      // no call, so a status waiting on a chip still in flight keeps waiting.
      this.earlyEnds.clear();
    }

    return { changed, orphaned };
  }

  /**
   * Record an emit payload at the position the model produced it. A data frame
   * rides the same per-segment seq as tokens, so this advances the segment's
   * sequence exactly as a token would: an in-order payload is kept, and a hole
   * — a token lost just before this frame — freezes the segment rather than let
   * the following token read as contiguous. Returns the frame's fate so the
   * caller can signal a gap the same way it does for a token.
   */
  addData(segment: number, seq: number, item: DataItem): 'appended' | 'gap' | 'dropped' {
    const { outcome, target } = this.advance(segment, seq);
    target?.items.push({ type: 'data', tool: item.tool, payload: item.payload });

    return outcome;
  }

  /**
   * Open a tool chip where the model called it. Pending until a tool_end for the
   * same ref arrives — which for an async call (workflow, prompt, knowledge
   * search) comes from the backend, seconds later, and may never come at all if
   * the run dies first. Rides the sequence like a token, because its position
   * between two text blocks is the whole point.
   */
  startTool(segment: number, seq: number, ref: string, tool: string): 'appended' | 'gap' | 'dropped' {
    const { outcome, target } = this.advance(segment, seq);
    // An end that got here first resolves the chip on arrival. The two frames come
    // from different publishers — the runner's ride a buffered hand-off, the
    // backend's go out directly — so a fast async tool can be answered before its
    // start has been written, and the order is not ours to rely on.
    const early = this.earlyEnds.get(ref);
    if (early !== undefined) {
      this.earlyEnds.delete(ref);
    }
    target?.items.push({
      type: 'tool',
      ref,
      name: tool,
      status: early?.status ?? 'pending',
      queries: early?.queries ?? [],
      citations: early?.citations ?? [],
    });

    return outcome;
  }

  /**
   * Resolve a started tool. Out-of-band: no seq, so it never consumes a slot the
   * client would then find missing. Reports whether anything changed — a ref
   * from a segment a reset already discarded resolves nothing, which is correct.
   *
   * `found` is what a provider-side search reported, empty for every other tool.
   * It arrives here rather than on the start because the end is when the provider
   * knows it.
   */
  endTool(ref: string, status: ToolStatus, found: SearchResult): boolean {
    for (const segment of this.ordered()) {
      for (const item of segment.items) {
        // First still-pending match: refs collide only when the provider sends
        // no call ids (Gemini), and the earliest unresolved call is the honest
        // reading of a status that cannot say which one it meant.
        if (item.type === 'tool' && item.ref === ref && item.status === 'pending') {
          item.status = status;
          item.queries = found.queries;
          item.citations = dedupe(found.citations);

          return true;
        }
      }
    }

    // No chip yet: hold the resolution for the start still in flight. Bounded by the
    // tools one run can have open, and dropped wholesale by a reset, so a status
    // from an abandoned attempt never lands on the retry's chip.
    this.earlyEnds.set(ref, { status, queries: found.queries, citations: dedupe(found.citations) });

    return false;
  }

  /**
   * A run first seen at a nonzero segment ran earlier segments unobserved —
   * content-bearing or not, nothing can ever tell. The phantom head (segment -1,
   * below any real one) is permanently gapped, so the withholding and verdict
   * logic flags the run without a special case.
   */
  private ensureHead(segment: number): boolean {
    if (this.segments.size > 0 || segment <= 0) {
      return false;
    }
    this.segments.set(-1, { items: [], lastSeq: 0, gapped: true });

    return true;
  }

  /**
   * The highest segment whose content is still provable — the first gapped
   * segment (inclusive), or Infinity when nothing is gapped. Segments past it
   * sit on the far side of a hole and are withheld from every view.
   */
  private frontier(): number {
    const gapped = [...this.segments.entries()]
      .filter(([, state]) => state.gapped)
      .map(([segment]) => segment);

    return gapped.length === 0 ? Infinity : Math.min(...gapped);
  }

  /** Provable segments in order — the one place the frontier is applied. */
  private ordered(): Segment[] {
    const frontier = this.frontier();

    return [...this.segments.entries()]
      .filter(([segment]) => segment <= frontier)
      .sort(([a], [b]) => a - b)
      .map(([, state]) => state);
  }

  /**
   * The run so far as a renderable sequence, always a true prefix. Text blocks
   * merge across the segment boundary too, for the same reason they merge within
   * one: only a tool or a payload between them makes them separate blocks.
   */
  transcript(): TranscriptItem[] {
    const items: TranscriptItem[] = [];
    for (const segment of this.ordered()) {
      for (const item of segment.items) {
        const previous = items[items.length - 1];
        if (item.type === 'text' && previous?.type === 'text') {
          previous.content += item.content;
          continue;
        }
        items.push(publish(item));
      }
    }

    return items;
  }

  /**
   * The run's text so far — always a true prefix of the run. Read straight off
   * the same item list {@link transcript} walks, so the two cannot disagree, and
   * without building a transcript it would only throw away — this runs on every
   * token.
   */
  text(): string {
    let text = '';
    for (const segment of this.ordered()) {
      for (const item of segment.items) {
        if (item.type === 'text') {
          text += item.content;
        }
      }
    }

    return text;
  }

  /**
   * The run's emit payloads so far, in emission order — and, like {@link text},
   * a true prefix: segments beyond the first gap are withheld, because any emit
   * calls before that unresolved hole are unknown and the list would otherwise
   * present a suffix as the whole.
   */
  data(): DataItem[] {
    const data: DataItem[] = [];
    for (const segment of this.ordered()) {
      for (const item of segment.items) {
        if (item.type === 'data') {
          data.push({ tool: item.tool, payload: item.payload });
        }
      }
    }

    return data;
  }

  /** Whether any segment lost events — the maintained views are then incomplete. */
  gapped(): boolean {
    return [...this.segments.values()].some((state) => state.gapped);
  }

  /**
   * Whether any sequenced frame has landed. Every attempt opens with a reset and
   * every in-band frame claims a segment, so this is what separates a run that
   * was watched from one whose buffer exists only because an out-of-band
   * tool_end arrived alone — the latter proves nothing about what was missed.
   */
  observed(): boolean {
    return this.segments.size > 0;
  }
}

/** Strip the internal correlator and fill the fields the stream cannot carry. */
function publish(item: BufferedItem): TranscriptItem {
  switch (item.type) {
    case 'text':
      return { type: 'text', content: item.content, citations: [] };
    case 'tool':
      return {
        type: 'tool',
        name: item.name,
        status: item.status,
        queries: item.queries,
        citations: item.citations,
      };
    case 'data':
      return { type: 'data', tool: item.tool, payload: item.payload };
  }
}

/**
 * First occurrence per URL, so merged results keep the order the model searched
 * in. Mirrors the backend's own dedupe: a provider can return the same source
 * more than once across a search's result blocks, and the two views of one run
 * have to collapse them the same way.
 */
function dedupe(citations: Citation[]): Citation[] {
  const byUrl = new Map<string, Citation>();
  for (const citation of citations) {
    if (!byUrl.has(citation.url)) {
      byUrl.set(citation.url, citation);
    }
  }

  return [...byUrl.values()];
}

interface Segment {
  items: BufferedItem[];
  lastSeq: number;
  gapped: boolean;
}
