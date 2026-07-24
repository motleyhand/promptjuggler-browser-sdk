# @promptjuggler/browser

Subscribe to live PromptJuggler token streams from the browser. Everything in
this package is safe to ship to an end user — it never sees your API key.

## How it fits together

1. Your **backend** calls `createStreamToken(threadId)` with a server-side SDK
   (`@promptjuggler/sdk`, or the PHP/Python/Java/Go equivalent) and returns the
   response to your frontend. The credential grants read access to one thread
   and nothing else.
2. Your **frontend** hands that response to this SDK and renders events.
3. Connect **before** triggering runs: a fresh subscription starts at the
   live tip. Reconnects resume exactly where they left off; when the server
   cannot resume without loss it says `stale`, and terminal events carry a
   `gapped` flag — `true` means fetch the run for the authoritative result.

```ts
import { PromptJugglerStream } from '@promptjuggler/browser';

const stream = new PromptJugglerStream({
  // Called on connect and on every reconnect, so expiry renews itself.
  getToken: async () => {
    const res = await fetch(`/my-api/stream-token?thread=${threadId}`);
    return res.json(); // { token, url } — createStreamToken's response, verbatim
  },
});

stream.on('text', ({ runId, text }) => render(runId, text)); // full text, maintained for you
stream.on('done', ({ runId, gapped }) => gapped && refetch(runId)); // fetch only when told to
stream.on('failure', ({ runId, code, message }) => showError(runId, message));
stream.connect();
```

`token` (raw deltas), `reset`, `gap`, `stale`, `connected` and `disconnected`
events are also emitted for append-style UIs — segment handling, resets, and
resume are already applied to the `text` view, so most apps never need them.
One edge to know: `gap` can fire even after a clean `done` (a late token
proving the finished text incomplete) — treat it as a refetch cue for runs
you have already settled. The React hook handles this for you.

Workflows stream every prompt node; follow specific conversation lanes with
`channels: ['support']` (the channel each node declares in the workflow editor).

## React

```tsx
import { usePromptJugglerStream } from '@promptjuggler/browser/react';

const { connected, runs } = usePromptJugglerStream({ getToken }, [threadId]);
// runs[runId] = { text, status: 'streaming' | 'done' | 'failed', gapped?, error? }
```

## Angular (17+)

```ts
import { injectPromptJugglerStream } from '@promptjuggler/browser/angular';

// Signals read during the factory call drive resubscription — hoist them into
// locals (a read deferred into getToken happens too late to be tracked).
readonly stream = injectPromptJugglerStream(() => {
  const thread = this.threadId();
  return { getToken: () => this.mintToken(thread) };
});
// stream.connected: Signal<boolean>; stream.runs: Signal<Record<string, RunState>>
```

## Reconnection

Automatic: exponential backoff after failures, immediate when the server hands
the connection off during a deploy. `disconnect()` stops everything.
