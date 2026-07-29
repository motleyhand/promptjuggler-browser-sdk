import {
  assertInInjectionContext,
  effect,
  signal,
  untracked,
  type Injector,
  type Signal,
} from '@angular/core';
import {
  applyData,
  applyDone,
  applyFailure,
  applyGap,
  applyStale,
  applyText,
  applyTranscript,
} from './runstate';
import type { RunState, RunStates } from './runstate';
import { PromptJugglerStream, type PromptJugglerStreamOptions } from './stream';

export type { RunState } from './runstate';

export interface PromptJugglerStreamSignals {
  /** Whether the SSE connection is currently open. */
  connected: Signal<boolean>;
  /** Live state per run id — render `runs()[runId].text` and you have a chatbot. */
  runs: Signal<Record<string, RunState>>;
}

/**
 * Subscribe a component to a thread's token stream, as signals.
 *
 * The options come from a factory, and any signal read during the factory
 * call is tracked: when one changes (typically the thread id), the old
 * subscription — and its runs — are dropped and a fresh one connects. That is
 * the whole reactivity contract, and it cuts both ways: a signal read
 * deferred into `getToken` runs after tracking ends and is NOT followed —
 * hoist it into a local inside the factory and close over that. Values read
 * without going through a signal are captured once per subscription.
 *
 * Call in an injection context (constructor or field initializer), or pass an
 * injector explicitly. The subscription disconnects when that context is
 * destroyed.
 */
export function injectPromptJugglerStream(
  options: () => PromptJugglerStreamOptions,
  injector?: Injector,
): PromptJugglerStreamSignals {
  if (!injector) {
    assertInInjectionContext(injectPromptJugglerStream);
  }
  const connected = signal(false);
  const runs = signal<RunStates>({});

  effect(
    (onCleanup) => {
      const resolved = options(); // tracked: signal reads here drive resubscription
      untracked(() => {
        // A fresh subscription target: the previous thread's runs are not ours.
        connected.set(false);
        runs.set({});

        const stream = new PromptJugglerStream(resolved);
        const subscriptions = [
          stream.on('connected', () => {
            connected.set(true);
          }),
          stream.on('disconnected', () => {
            connected.set(false);
          }),
          stream.on('text', (event) => {
            runs.update((previous) => applyText(previous, event));
          }),
          stream.on('data', (event) => {
            runs.update((previous) => applyData(previous, event));
          }),
          stream.on('transcript', (event) => {
            runs.update((previous) => applyTranscript(previous, event));
          }),
          stream.on('stale', () => {
            runs.update(applyStale);
          }),
          stream.on('gap', (event) => {
            runs.update((previous) => applyGap(previous, event));
          }),
          stream.on('done', (event) => {
            runs.update((previous) => applyDone(previous, event));
          }),
          stream.on('failure', (event) => {
            runs.update((previous) => applyFailure(previous, event));
          }),
        ];
        stream.connect();

        onCleanup(() => {
          subscriptions.forEach((unsubscribe) => {
            unsubscribe();
          });
          stream.disconnect();
        });
      });
    },
    { injector },
  );

  return { connected, runs };
}
