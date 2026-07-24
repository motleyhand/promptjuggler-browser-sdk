// @vitest-environment jsdom
import 'zone.js';
import { signal } from '@angular/core';
import { TestBed } from '@angular/core/testing';
import { BrowserTestingModule, platformBrowserTesting } from '@angular/platform-browser/testing';
import { afterEach, beforeEach, describe, expect, test, vi } from 'vitest';
import { injectPromptJugglerStream } from '../src/angular';
import { SseServer } from './helpers';

TestBed.initTestEnvironment(BrowserTestingModule, platformBrowserTesting());

describe('injectPromptJugglerStream', () => {
  let server: SseServer;
  let url: string;

  beforeEach(async () => {
    server = new SseServer();
    url = await server.start();
  });

  afterEach(async () => {
    TestBed.resetTestingModule();
    await server.stop();
  });

  test('streams runs into signals', async () => {
    const { connected, runs } = TestBed.runInInjectionContext(() =>
      injectPromptJugglerStream(() => ({
        getToken: () => Promise.resolve({ token: 'test-token', url }),
        reconnectDelayMs: { min: 10, max: 50 },
      })),
    );
    TestBed.tick(); // run the effect: the subscription connects

    const connection = await server.connection(1);
    await vi.waitFor(() => {
      expect(connected()).toBe(true);
    });

    connection.send(
      'token',
      '{"kind":"token","runId":"r1","channel":"default","segment":0,"seq":1,"text":"Hello"}',
    );
    connection.send('done', '{"kind":"done","runId":"r1","channel":"default"}');
    await vi.waitFor(() => {
      expect(runs().r1).toMatchObject({ text: 'Hello', status: 'done', gapped: false });
    });
  });

  test('a signal read in the options factory drives resubscription', async () => {
    const thread = signal('thread-1');
    const { runs } = TestBed.runInInjectionContext(() =>
      injectPromptJugglerStream(() => {
        const current = thread(); // tracked: changing it resubscribes
        return {
          getToken: () => Promise.resolve({ token: `token-${current}`, url }),
          reconnectDelayMs: { min: 10, max: 50 },
        };
      }),
    );
    TestBed.tick();

    const first = await server.connection(1);
    expect(first.request.headers.authorization).toBe('Bearer token-thread-1');
    first.send('token', '{"kind":"token","runId":"r1","channel":"default","segment":0,"seq":1,"text":"old"}');
    await vi.waitFor(() => {
      expect(runs().r1).toBeDefined();
    });

    thread.set('thread-2');
    TestBed.tick();

    const second = await server.connection(2);
    expect(second.request.headers.authorization).toBe('Bearer token-thread-2');
    // A fresh subscription target: the previous thread's runs are gone.
    expect(runs()).toEqual({});
  });

  test('destroying the context disconnects the stream', async () => {
    const { runs } = TestBed.runInInjectionContext(() =>
      injectPromptJugglerStream(() => ({
        getToken: () => Promise.resolve({ token: 'test-token', url }),
        reconnectDelayMs: { min: 10, max: 50 },
      })),
    );
    TestBed.tick();
    const connection = await server.connection(1);

    TestBed.resetTestingModule(); // destroys the injector → onCleanup disconnects

    connection.send(
      'token',
      '{"kind":"token","runId":"r1","channel":"default","segment":0,"seq":1,"text":"late"}',
    );
    await new Promise((resolve) => setTimeout(resolve, 100));
    expect(runs()).toEqual({});
    expect(server.connections).toHaveLength(1); // and no reconnect either
  });
});
