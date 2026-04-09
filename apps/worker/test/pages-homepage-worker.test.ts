import { afterEach, describe, expect, it, vi } from 'vitest';

import pageWorker from '../../web/public/_worker.js';

type CacheMatcher = (request: Request) => Response | undefined;

function installDefaultCacheMock(match: CacheMatcher) {
  const put = vi.fn(async () => undefined);

  Object.defineProperty(globalThis, 'caches', {
    configurable: true,
    value: {
      default: {
        async match(request: Request) {
          return match(request)?.clone();
        },
        put,
      },
    },
  });

  return { put };
}

function makeEnv(indexHtml = '<!doctype html><html><head></head><body><div id="root"></div></body></html>') {
  return {
    ASSETS: {
      fetch: vi.fn(async () => new Response(indexHtml, { status: 200 })),
    },
    UPTIMER_API_ORIGIN: 'https://api.example.com',
  };
}

describe('pages homepage worker', () => {
  const originalCaches = (globalThis as { caches?: unknown }).caches;
  const originalFetch = globalThis.fetch;

  afterEach(() => {
    if (originalCaches === undefined) {
      delete (globalThis as { caches?: unknown }).caches;
    } else {
      Object.defineProperty(globalThis, 'caches', {
        configurable: true,
        value: originalCaches,
      });
    }

    globalThis.fetch = originalFetch;
    vi.restoreAllMocks();
  });

  it('serves cached injected HTML without calling the homepage API', async () => {
    installDefaultCacheMock((request) =>
      request.url === 'https://status.example.com/'
        ? new Response('<html>cached homepage</html>', { status: 200 })
        : undefined,
    );
    const env = makeEnv();
    const fetchSpy = vi.fn();
    globalThis.fetch = fetchSpy as never;

    const res = await pageWorker.fetch(
      new Request('https://status.example.com/', {
        headers: { Accept: 'text/html' },
      }),
      env,
      { waitUntil: vi.fn() },
    );

    expect(await res.text()).toContain('cached homepage');
    expect(env.ASSETS.fetch).not.toHaveBeenCalled();
    expect(fetchSpy).not.toHaveBeenCalled();
  });

  it('falls back to the cached injected homepage when snapshot fetch fails', async () => {
    installDefaultCacheMock((request) =>
      request.url === 'https://status.example.com/__uptimer_homepage_fallback__'
        ? new Response('<html>fallback homepage</html>', { status: 200 })
        : undefined,
    );
    const env = makeEnv();
    globalThis.fetch = vi.fn(async () => {
      throw new Error('network failed');
    }) as never;

    const res = await pageWorker.fetch(
      new Request('https://status.example.com/', {
        headers: { Accept: 'text/html' },
      }),
      env,
      { waitUntil: vi.fn() },
    );

    expect(await res.text()).toContain('fallback homepage');
  });

  it('injects the precomputed homepage artifact and updates both html caches on success', async () => {
    const { put } = installDefaultCacheMock(() => undefined);
    const env = makeEnv();
    globalThis.fetch = vi.fn(async () =>
      new Response(
        JSON.stringify({
          generated_at: 1_728_000_000,
          style_tag: '<style id="uptimer-preload-style">body{}</style>',
          preload_html: '<div id="uptimer-preload"><main>artifact preload</main></div>',
          bootstrap_script:
            '<script>globalThis.__UPTIMER_INITIAL_HOMEPAGE__={"site_title":"Status Hub"};</script>',
          meta_title: 'Status Hub',
          meta_description: 'Production',
        }),
        { status: 200 },
      ),
    ) as never;

    const res = await pageWorker.fetch(
      new Request('https://status.example.com/', {
        headers: { Accept: 'text/html' },
      }),
      env,
      { waitUntil: vi.fn((promise) => promise) },
    );

    const html = await res.text();
    expect(html).toContain('__UPTIMER_INITIAL_HOMEPAGE__');
    expect(html).not.toContain('__UPTIMER_INITIAL_STATUS__');
    expect(html).toContain('artifact preload');
    expect(put).toHaveBeenCalledTimes(2);
  });
});
