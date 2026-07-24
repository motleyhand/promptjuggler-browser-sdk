# @promptjuggler/browser

Subscribe to live PromptJuggler token streams from the browser. Everything in
this package is safe to ship to an end user — it never sees your API key.

## How it fits together

1. Your **backend** calls `createStreamToken(threadId)` with a server-side SDK
   (`@promptjuggler/sdk`, or the PHP/Python/Java/Go equivalent) and returns the
   response to your frontend. The credential grants read access to one thread
   and nothing else.
2. Your **frontend** hands that response to this SDK and renders events.
3. Connect **before** triggering runs: the stream has no replay — anything
   missed is recovered by fetching the run when its `done` event arrives.

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
stream.on('done', ({ runId }) => refetch(runId)); // authoritative result via the API
stream.on('failure', ({ runId, code, message }) => showError(runId, message));
stream.connect();
```

`token` (raw deltas), `reset`, `connected` and `disconnected` events are also
emitted for append-style UIs — segment-scoped reset handling is already applied
to the `text` view, so most apps never need them.

Workflows stream every prompt node; follow specific conversation lanes with
`channels: ['support']` (the channel each node declares in the workflow editor).

## React

```tsx
import { usePromptJugglerStream } from '@promptjuggler/browser/react';

const { connected, runs } = usePromptJugglerStream({ getToken });
// runs[runId] = { text, status: 'streaming' | 'done' | 'failed', error? }
```

## Reconnection

Automatic: exponential backoff after failures, immediate when the server hands
the connection off during a deploy. `disconnect()` stops everything.
