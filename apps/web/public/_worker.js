const SNAPSHOT_MAX_AGE_SECONDS = 60;
const PREFERRED_MAX_AGE_SECONDS = 30;
const FALLBACK_HTML_MAX_AGE_SECONDS = 600;

function acceptsHtml(request) {
  const accept = request.headers.get('Accept') || '';
  return accept.includes('text/html');
}

function escapeHtml(value) {
  return String(value)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

function normalizeSnapshotText(value, fallback) {
  if (typeof value !== 'string') return fallback;
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : fallback;
}

function computeCacheControl(ageSeconds) {
  const remaining = Math.max(0, SNAPSHOT_MAX_AGE_SECONDS - ageSeconds);
  const maxAge = Math.min(PREFERRED_MAX_AGE_SECONDS, remaining);
  const stale = Math.max(0, remaining - maxAge);
  return `public, max-age=${maxAge}, stale-while-revalidate=${stale}, stale-if-error=${stale}`;
}

function upsertHeadTag(html, pattern, tag) {
  if (pattern.test(html)) {
    return html.replace(pattern, tag);
  }
  return html.replace('</head>', `  ${tag}\n</head>`);
}

function injectStatusMetaTags(html, artifact, url) {
  const siteTitle = normalizeSnapshotText(artifact?.meta_title, 'Uptimer');
  const siteDescription = normalizeSnapshotText(
    artifact?.meta_description,
    'Real-time status and incident updates.',
  )
    .replace(/\s+/g, ' ')
    .trim();
  const pageUrl = new URL('/', url).toString();

  const escapedTitle = escapeHtml(siteTitle);
  const escapedDescription = escapeHtml(siteDescription);
  const escapedUrl = escapeHtml(pageUrl);

  let injected = html;
  injected = upsertHeadTag(injected, /<title>[^<]*<\/title>/i, `<title>${escapedTitle}</title>`);
  injected = upsertHeadTag(
    injected,
    /<meta[^>]+name=["']description["'][^>]*>/i,
    `<meta name="description" content="${escapedDescription}" />`,
  );
  injected = upsertHeadTag(
    injected,
    /<meta[^>]+property=["']og:type["'][^>]*>/i,
    '<meta property="og:type" content="website" />',
  );
  injected = upsertHeadTag(
    injected,
    /<meta[^>]+property=["']og:title["'][^>]*>/i,
    `<meta property="og:title" content="${escapedTitle}" />`,
  );
  injected = upsertHeadTag(
    injected,
    /<meta[^>]+property=["']og:description["'][^>]*>/i,
    `<meta property="og:description" content="${escapedDescription}" />`,
  );
  injected = upsertHeadTag(
    injected,
    /<meta[^>]+property=["']og:site_name["'][^>]*>/i,
    `<meta property="og:site_name" content="${escapedTitle}" />`,
  );
  injected = upsertHeadTag(
    injected,
    /<meta[^>]+property=["']og:url["'][^>]*>/i,
    `<meta property="og:url" content="${escapedUrl}" />`,
  );
  injected = upsertHeadTag(
    injected,
    /<meta[^>]+name=["']twitter:card["'][^>]*>/i,
    '<meta name="twitter:card" content="summary" />',
  );
  injected = upsertHeadTag(
    injected,
    /<meta[^>]+name=["']twitter:title["'][^>]*>/i,
    `<meta name="twitter:title" content="${escapedTitle}" />`,
  );
  injected = upsertHeadTag(
    injected,
    /<meta[^>]+name=["']twitter:description["'][^>]*>/i,
    `<meta name="twitter:description" content="${escapedDescription}" />`,
  );

  return injected;
}

async function fetchIndexHtml(env, url) {
  const indexUrl = new URL('/index.html', url);

  // Do not pass the original navigation request as init. In Pages runtime the
  // navigation request can carry redirect mode = manual; if we forward that
  // into `env.ASSETS.fetch`, we might accidentally return a redirect response
  // (and cache it), causing ERR_TOO_MANY_REDIRECTS.
  const req = new Request(indexUrl.toString(), {
    method: 'GET',
    headers: { Accept: 'text/html' },
    redirect: 'follow',
  });

  return env.ASSETS.fetch(req);
}

async function fetchPublicHomepageArtifact(env) {
  const apiOrigin = env.UPTIMER_API_ORIGIN;
  if (typeof apiOrigin !== 'string' || apiOrigin.length === 0) return null;

  const statusUrl = new URL('/api/v1/public/homepage-artifact', apiOrigin);

  // Keep HTML fast: if the API is slow, fall back to a static HTML shell.
  const controller = new AbortController();
  const t = setTimeout(() => controller.abort(), 800);

  try {
    const resp = await fetch(statusUrl.toString(), {
      headers: { Accept: 'application/json' },
      signal: controller.signal,
    });

    if (!resp.ok) return null;

    const data = await resp.json();
    if (!data || typeof data !== 'object') return null;

    if (typeof data.style_tag !== 'string') return null;
    if (typeof data.preload_html !== 'string') return null;
    if (typeof data.bootstrap_script !== 'string') return null;

    return data;
  } catch {
    return null;
  } finally {
    clearTimeout(t);
  }
}

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);

    // HTML requests: serve SPA entry for client-side routes.
    const wantsHtml = request.method === 'GET' && acceptsHtml(request);

    // Special-case the status page for HTML injection.
    const isStatusPage = url.pathname === '/' || url.pathname === '/index.html';
    if (wantsHtml && isStatusPage) {
      const cacheKey = new Request(url.origin + '/', { method: 'GET' });
      const fallbackCacheKey = new Request(url.origin + '/__uptimer_homepage_fallback__', {
        method: 'GET',
      });
      const cached = await caches.default.match(cacheKey);
      if (cached) return cached;

      const base = await fetchIndexHtml(env, url);
      const html = await base.text();

      const artifact = await fetchPublicHomepageArtifact(env);
      if (!artifact) {
        const fallback = await caches.default.match(fallbackCacheKey);
        if (fallback) {
          return fallback;
        }

        const headers = new Headers(base.headers);
        headers.set('Content-Type', 'text/html; charset=utf-8');
        headers.append('Vary', 'Accept');
        headers.delete('Location');

        return new Response(html, { status: 200, headers });
      }

      const now = Math.floor(Date.now() / 1000);
      const generatedAt = typeof artifact.generated_at === 'number' ? artifact.generated_at : now;
      const age = Math.max(0, now - generatedAt);

      let injected = html.replace(
        '<div id="root"></div>',
        `${artifact.preload_html}<div id="root"></div>`,
      );

      injected = injectStatusMetaTags(injected, artifact, url);

      injected = injected.replace(
        '</head>',
        `  ${artifact.style_tag}\n  ${artifact.bootstrap_script}\n</head>`,
      );

      const headers = new Headers(base.headers);
      headers.set('Content-Type', 'text/html; charset=utf-8');
      headers.set('Cache-Control', computeCacheControl(age));
      headers.append('Vary', 'Accept');
      headers.delete('Location');

      const resp = new Response(injected, { status: 200, headers });

      const fallbackHeaders = new Headers(headers);
      fallbackHeaders.set('Cache-Control', `public, max-age=${FALLBACK_HTML_MAX_AGE_SECONDS}`);
      const fallbackResp = new Response(injected, { status: 200, headers: fallbackHeaders });

      ctx.waitUntil(
        Promise.all([
          caches.default.put(cacheKey, resp.clone()),
          caches.default.put(fallbackCacheKey, fallbackResp),
        ]),
      );
      return resp;
    }

    // Default: serve static assets.
    const assetResp = await env.ASSETS.fetch(request);

    // SPA fallback for client-side routes.
    if (wantsHtml && assetResp.status === 404) {
      const indexResp = await fetchIndexHtml(env, url);
      const html = await indexResp.text();

      const headers = new Headers(indexResp.headers);
      headers.set('Content-Type', 'text/html; charset=utf-8');
      headers.append('Vary', 'Accept');
      headers.delete('Location');

      return new Response(html, { status: 200, headers });
    }

    return assetResp;
  },
};
