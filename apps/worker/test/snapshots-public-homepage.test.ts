import { afterEach, describe, expect, it, vi } from 'vitest';

vi.mock('../src/scheduler/lock', () => ({
  acquireLease: vi.fn(),
}));

import { acquireLease } from '../src/scheduler/lock';
import {
  applyHomepageCacheHeaders,
  buildHomepageRenderArtifact,
  getHomepageSnapshotKey,
  getHomepageSnapshotMaxAgeSeconds,
  getHomepageSnapshotMaxStaleSeconds,
  readHomepageSnapshot,
  readHomepageSnapshotArtifact,
  readStaleHomepageSnapshot,
  readStaleHomepageSnapshotArtifact,
  refreshPublicHomepageSnapshotIfNeeded,
  toHomepageSnapshotPayload,
  writeHomepageSnapshot,
} from '../src/snapshots/public-homepage';
import { createFakeD1Database } from './helpers/fake-d1';

function samplePayload(now = 1_728_000_000) {
  return {
    generated_at: now,
    bootstrap_mode: 'full' as const,
    monitor_count_total: 1,
    site_title: 'Uptimer',
    site_description: '',
    site_locale: 'auto' as const,
    site_timezone: 'UTC',
    uptime_rating_level: 3 as const,
    overall_status: 'up' as const,
    banner: {
      source: 'monitors' as const,
      status: 'operational' as const,
      title: 'All Systems Operational',
      down_ratio: null,
    },
    summary: {
      up: 1,
      down: 0,
      maintenance: 0,
      paused: 0,
      unknown: 0,
    },
    monitors: [
      {
        id: 1,
        name: 'API',
        type: 'http' as const,
        group_name: null,
        status: 'up' as const,
        is_stale: false,
        last_checked_at: now - 30,
        heartbeat_strip: {
          checked_at: [now - 60],
          status_codes: 'u',
          latency_ms: [42],
        },
        uptime_30d: { uptime_pct: 100 },
        uptime_day_strip: {
          day_start_at: [Math.max(0, now - 86_400)],
          downtime_sec: [0],
          unknown_sec: [0],
          uptime_pct_milli: [100_000],
        },
      },
    ],
    active_incidents: [],
    maintenance_windows: {
      active: [],
      upcoming: [],
    },
    resolved_incident_preview: null,
    maintenance_history_preview: null,
  };
}

describe('snapshots/public-homepage', () => {
  afterEach(() => {
    vi.clearAllMocks();
  });

  it('exposes stable snapshot constants', () => {
    expect(getHomepageSnapshotKey()).toBe('homepage');
    expect(getHomepageSnapshotMaxAgeSeconds()).toBe(60);
    expect(getHomepageSnapshotMaxStaleSeconds()).toBe(600);
  });

  it('reads fresh and bounded-stale homepage snapshots without live compute', async () => {
    const payload = samplePayload(190);
    const stored = {
      version: 2,
      data: payload,
      render: buildHomepageRenderArtifact(payload),
    };
    const db = createFakeD1Database([
      {
        match: 'from public_snapshots',
        first: () => ({
          generated_at: payload.generated_at,
          body_json: JSON.stringify(stored),
        }),
      },
    ]);

    await expect(readHomepageSnapshot(db, 200)).resolves.toEqual({
      data: payload,
      age: 10,
    });
    await expect(readHomepageSnapshotArtifact(db, 200)).resolves.toEqual({
      data: stored.render,
      age: 10,
    });
    await expect(readStaleHomepageSnapshot(db, 200)).resolves.toEqual({
      data: payload,
      age: 10,
    });
    await expect(readStaleHomepageSnapshotArtifact(db, 200)).resolves.toEqual({
      data: stored.render,
      age: 10,
    });
  });

  it('reads legacy homepage payloads but refuses to synthesize render artifacts on the read path', async () => {
    const { bootstrap_mode: _ignoredMode, monitor_count_total: _ignoredCount, ...legacyPayload } =
      samplePayload(190);
    const db = createFakeD1Database([
      {
        match: 'from public_snapshots',
        first: () => ({
          generated_at: legacyPayload.generated_at,
          body_json: JSON.stringify(legacyPayload),
        }),
      },
    ]);

    await expect(readHomepageSnapshot(db, 200)).resolves.toEqual({
      data: samplePayload(190),
      age: 10,
    });
    await expect(readHomepageSnapshotArtifact(db, 200)).resolves.toBeNull();
  });

  it('caps bootstrap monitors in render artifacts to keep root misses bounded', () => {
    const payload = {
      ...samplePayload(190),
      monitor_count_total: 30,
      monitors: Array.from({ length: 30 }, (_, index) => ({
        ...samplePayload(190).monitors[0],
        id: index + 1,
        name: `Monitor ${index + 1}`,
      })),
      summary: {
        up: 30,
        down: 0,
        maintenance: 0,
        paused: 0,
        unknown: 0,
      },
      maintenance_history_preview: {
        id: 1,
        title: 'Database patching',
        message: null,
        starts_at: 120,
        ends_at: 180,
        monitor_ids: [30],
      },
    };

    const artifact = buildHomepageRenderArtifact(payload);
    const match = artifact.bootstrap_script.match(
      /__UPTIMER_INITIAL_HOMEPAGE__=(.*);<\/script>$/,
    );
    expect(match).not.toBeNull();

    const bootstrapped = JSON.parse(match?.[1] ?? '{}') as {
      bootstrap_mode: string;
      monitor_count_total: number;
      monitors: Array<{ id: number }>;
    };
    expect(bootstrapped.bootstrap_mode).toBe('partial');
    expect(bootstrapped.monitor_count_total).toBe(30);
    expect(bootstrapped.monitors).toHaveLength(24);
    expect(artifact.preload_html).toContain('Monitor 30');
    expect(artifact.preload_html).not.toContain('#30');
  });

  it('returns null when homepage snapshot is too old or invalid', async () => {
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => undefined);

    const staleDb = createFakeD1Database([
      {
        match: 'from public_snapshots',
        first: () => ({
          generated_at: 0,
          body_json: JSON.stringify(samplePayload(0)),
        }),
      },
    ]);
    await expect(readHomepageSnapshot(staleDb, 200)).resolves.toBeNull();
    await expect(readStaleHomepageSnapshot(staleDb, 800)).resolves.toBeNull();

    const invalidDb = createFakeD1Database([
      {
        match: 'from public_snapshots',
        first: () => ({
          generated_at: 190,
          body_json: '{not-json',
        }),
      },
    ]);
    await expect(readHomepageSnapshot(invalidDb, 200)).resolves.toBeNull();
    warn.mockRestore();
  });

  it('writes normalized homepage snapshots with upsert semantics', async () => {
    let boundArgs = null as unknown[] | null;
    const db = createFakeD1Database([
      {
        match: 'insert into public_snapshots',
        run: (args) => {
          boundArgs = args;
          return { meta: { changes: 1 } };
        },
      },
    ]);

    const payload = samplePayload(280);
    await writeHomepageSnapshot(db, 300, payload);

    const stored = {
      version: 2,
      data: payload,
      render: buildHomepageRenderArtifact(payload),
    };

    expect(boundArgs).toEqual(['homepage', 280, JSON.stringify(stored), 300]);
  });

  it('applies bounded cache headers for homepage payloads', () => {
    const fresh = new Response('ok');
    applyHomepageCacheHeaders(fresh, 10);
    expect(fresh.headers.get('Cache-Control')).toBe(
      'public, max-age=30, stale-while-revalidate=20, stale-if-error=20',
    );

    const stale = new Response('ok');
    applyHomepageCacheHeaders(stale, 120);
    expect(stale.headers.get('Cache-Control')).toBe(
      'public, max-age=0, stale-while-revalidate=0, stale-if-error=0',
    );
  });

  it('validates homepage snapshot payload shape before persistence', () => {
    const payload = samplePayload(123);
    expect(toHomepageSnapshotPayload(payload)).toEqual(payload);
    expect(() => toHomepageSnapshotPayload({ generated_at: 1 })).toThrow();
  });

  it('skips refresh when the homepage snapshot was already generated this minute', async () => {
    const now = 1_728_000_045;
    const db = createFakeD1Database([
      {
        match: 'from public_snapshots',
        first: () => ({
          generated_at: 1_728_000_031,
          body_json: JSON.stringify(samplePayload(1_728_000_031)),
        }),
      },
    ]);

    const compute = vi.fn(async () => samplePayload(now));
    const refreshed = await refreshPublicHomepageSnapshotIfNeeded({ db, now, compute });

    expect(refreshed).toBe(false);
    expect(acquireLease).not.toHaveBeenCalled();
    expect(compute).not.toHaveBeenCalled();
  });

  it('refreshes once when the minute changed and a refresh lease is acquired', async () => {
    vi.mocked(acquireLease).mockResolvedValue(true);

    let readCount = 0;
    let writtenArgs = null as unknown[] | null;
    const now = 1_728_000_120;
    const db = createFakeD1Database([
      {
        match: 'from public_snapshots',
        first: () => {
          readCount += 1;
          if (readCount <= 2) {
            return {
              generated_at: 1_728_000_001,
              body_json: JSON.stringify({
                version: 2,
                data: samplePayload(1_728_000_001),
                render: buildHomepageRenderArtifact(samplePayload(1_728_000_001)),
              }),
            };
          }
          return {
            generated_at: now,
            body_json: JSON.stringify({
              version: 2,
              data: samplePayload(now),
              render: buildHomepageRenderArtifact(samplePayload(now)),
            }),
          };
        },
      },
      {
        match: 'insert into public_snapshots',
        run: (args) => {
          writtenArgs = args;
          return { meta: { changes: 1 } };
        },
      },
    ]);

    const compute = vi.fn(async () => samplePayload(now));
    const refreshed = await refreshPublicHomepageSnapshotIfNeeded({ db, now, compute });
    const stored = {
      version: 2,
      data: samplePayload(now),
      render: buildHomepageRenderArtifact(samplePayload(now)),
    };

    expect(refreshed).toBe(true);
    expect(acquireLease).toHaveBeenCalledWith(db, 'snapshot:homepage:refresh', now, 55);
    expect(compute).toHaveBeenCalledTimes(1);
    expect(writtenArgs).toEqual(['homepage', now, JSON.stringify(stored), now]);
  });
});
