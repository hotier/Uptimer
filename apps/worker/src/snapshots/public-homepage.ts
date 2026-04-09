import { AppError } from '../middleware/errors';
import { acquireLease } from '../scheduler/lock';
import {
  publicHomepageResponseSchema,
  type PublicHomepageResponse,
} from '../schemas/public-homepage';
import { z } from 'zod';

const SNAPSHOT_KEY = 'homepage';
const MAX_AGE_SECONDS = 60;
const MAX_STALE_SECONDS = 10 * 60;
const REFRESH_LOCK_NAME = 'snapshot:homepage:refresh';
const MAX_BOOTSTRAP_MONITORS = 24;

const homepageRenderArtifactSchema = z.object({
  generated_at: z.number().int().nonnegative(),
  style_tag: z.string(),
  preload_html: z.string(),
  bootstrap_script: z.string(),
  meta_title: z.string(),
  meta_description: z.string(),
});

export type PublicHomepageRenderArtifact = z.infer<typeof homepageRenderArtifactSchema>;

type StoredHomepageSnapshot = {
  version: 2;
  data: PublicHomepageResponse;
  render: PublicHomepageRenderArtifact;
};

const storedHomepageSnapshotDataSchema = z.object({
  version: z.literal(2),
  data: publicHomepageResponseSchema,
});

const storedHomepageSnapshotRenderSchema = z.object({
  version: z.literal(2),
  render: homepageRenderArtifactSchema,
});

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null;
}

function safeJsonForInlineScript(value: unknown): string {
  return JSON.stringify(value).replace(/</g, '\\u003c');
}

function normalizeSnapshotText(value: unknown, fallback: string): string {
  if (typeof value !== 'string') return fallback;
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : fallback;
}

function escapeHtml(value: unknown): string {
  return String(value)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

function formatTime(tsSec: number, cache?: Map<number, string>): string {
  if (cache?.has(tsSec)) {
    return cache.get(tsSec) ?? '';
  }

  let formatted = '';
  try {
    formatted = new Date(tsSec * 1000).toLocaleString();
  } catch {
    formatted = '';
  }

  cache?.set(tsSec, formatted);
  return formatted;
}

function monitorGroupLabel(value: string | null | undefined): string {
  const trimmed = value?.trim() ?? '';
  return trimmed.length > 0 ? trimmed : 'Ungrouped';
}

function uptimeFillFromMilli(uptimePctMilli: number | null | undefined): string {
  if (typeof uptimePctMilli !== 'number') return '#cbd5e1';
  if (uptimePctMilli >= 99_950) return '#10b981';
  if (uptimePctMilli >= 99_000) return '#84cc16';
  if (uptimePctMilli >= 95_000) return '#f59e0b';
  return '#ef4444';
}

function heartbeatFillFromCode(code: string | undefined): string {
  switch (code) {
    case 'u':
      return '#10b981';
    case 'd':
      return '#ef4444';
    case 'm':
      return '#3b82f6';
    case 'x':
    default:
      return '#cbd5e1';
  }
}

function heartbeatHeightPct(
  code: string | undefined,
  latencyMs: number | null | undefined,
): number {
  if (code === 'd') return 100;
  if (code === 'm') return 62;
  if (code !== 'u') return 48;
  if (typeof latencyMs !== 'number' || !Number.isFinite(latencyMs)) return 74;
  return 36 + Math.min(64, Math.max(0, latencyMs / 12));
}

function buildUptimeStripSvg(
  strip: PublicHomepageResponse['monitors'][number]['uptime_day_strip'],
): string {
  const count = Math.min(
    strip.day_start_at.length,
    strip.downtime_sec.length,
    strip.unknown_sec.length,
    strip.uptime_pct_milli.length,
  );
  const barWidth = 4;
  const gap = 2;
  const height = 20;
  const width = count <= 0 ? barWidth : count * barWidth + Math.max(0, count - 1) * gap;
  let rects = '';
  for (let index = 0; index < count; index += 1) {
    const x = index * (barWidth + gap);
    const fill = uptimeFillFromMilli(strip.uptime_pct_milli[index]);
    rects += `<rect x="${x}" width="${barWidth}" height="${height}" rx="1" fill="${fill}"/>`;
  }
  return `<svg class="usv" viewBox="0 0 ${width} ${height}" preserveAspectRatio="none" aria-hidden="true">${rects}</svg>`;
}

function buildHeartbeatStripSvg(
  strip: PublicHomepageResponse['monitors'][number]['heartbeat_strip'],
): string {
  const count = Math.min(
    strip.checked_at.length,
    strip.latency_ms.length,
    strip.status_codes.length,
  );
  const barWidth = 4;
  const gap = 2;
  const height = 20;
  const width = count <= 0 ? barWidth : count * barWidth + Math.max(0, count - 1) * gap;
  let rects = '';
  for (let index = 0; index < count; index += 1) {
    const x = index * (barWidth + gap);
    const barHeight =
      (height * heartbeatHeightPct(strip.status_codes[index], strip.latency_ms[index])) / 100;
    const y = height - barHeight;
    rects += `<rect x="${x}" y="${y.toFixed(2)}" width="${barWidth}" height="${barHeight.toFixed(2)}" rx="1" fill="${heartbeatFillFromCode(strip.status_codes[index])}"/>`;
  }
  return `<svg class="usv" viewBox="0 0 ${width} ${height}" preserveAspectRatio="none" aria-hidden="true">${rects}</svg>`;
}

const HOMEPAGE_PRELOAD_STYLE_TAG = `<style id="uptimer-preload-style">
#uptimer-preload{min-height:100vh;background:#f8fafc;color:#0f172a;font:400 14px/1.45 ui-sans-serif,system-ui,sans-serif}
#uptimer-preload *{box-sizing:border-box}
#uptimer-preload .uw{max-width:80rem;margin:0 auto;padding:0 16px}
#uptimer-preload .uh{position:sticky;top:0;z-index:20;background:rgba(255,255,255,.95);backdrop-filter:blur(12px);border-bottom:1px solid rgba(226,232,240,.8)}
#uptimer-preload .uhw{display:flex;align-items:center;justify-content:space-between;gap:16px;padding:16px 0}
#uptimer-preload .ut{min-width:0}
#uptimer-preload .un{font-size:20px;font-weight:700;line-height:1.1;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
#uptimer-preload .ud{margin-top:4px;color:#64748b;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
#uptimer-preload .sb{display:inline-flex;align-items:center;border-radius:999px;padding:4px 10px;font-size:12px;font-weight:600;border:1px solid transparent}
#uptimer-preload .sb-up{background:#ecfdf5;color:#047857;border-color:#a7f3d0}
#uptimer-preload .sb-down{background:#fef2f2;color:#b91c1c;border-color:#fecaca}
#uptimer-preload .sb-maintenance{background:#eff6ff;color:#1d4ed8;border-color:#bfdbfe}
#uptimer-preload .sb-paused{background:#fffbeb;color:#b45309;border-color:#fde68a}
#uptimer-preload .sb-unknown{background:#f8fafc;color:#475569;border-color:#cbd5e1}
#uptimer-preload .um{padding:24px 0 40px}
#uptimer-preload .bn{margin:0 0 24px;border:1px solid #e2e8f0;border-radius:18px;padding:20px;background:#fff;box-shadow:0 10px 30px rgba(15,23,42,.04)}
#uptimer-preload .bt{color:#475569}
#uptimer-preload .bu{margin-top:4px;font-size:12px;color:#94a3b8}
#uptimer-preload .sec{margin-top:24px}
#uptimer-preload .sh{margin:0 0 12px;font-size:16px;font-weight:700}
#uptimer-preload .st{display:grid;gap:12px}
#uptimer-preload .sg{margin-top:20px}
#uptimer-preload .sgh{display:flex;align-items:center;justify-content:space-between;margin-bottom:10px}
#uptimer-preload .sgt{font-size:13px;font-weight:700;color:#475569}
#uptimer-preload .sgc{font-size:12px;color:#94a3b8}
#uptimer-preload .grid{display:grid;gap:12px}
#uptimer-preload .card{border:1px solid rgba(226,232,240,.9);border-radius:16px;padding:14px;background:#fff}
#uptimer-preload .row{display:flex;align-items:flex-start;justify-content:space-between;gap:10px}
#uptimer-preload .lhs{min-width:0;display:flex;align-items:flex-start;gap:10px}
#uptimer-preload .dot{display:block;width:10px;height:10px;border-radius:999px;margin-top:5px}
#uptimer-preload .dot-up{background:#10b981}
#uptimer-preload .dot-down{background:#ef4444}
#uptimer-preload .dot-maintenance{background:#3b82f6}
#uptimer-preload .dot-paused{background:#f59e0b}
#uptimer-preload .dot-unknown{background:#94a3b8}
#uptimer-preload .mn{font-size:15px;font-weight:700;line-height:1.25;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
#uptimer-preload .mt{margin-top:3px;font-size:11px;color:#64748b;text-transform:uppercase;letter-spacing:.08em}
#uptimer-preload .rhs{display:flex;align-items:center;gap:8px;white-space:nowrap}
#uptimer-preload .up{font-size:12px;color:#94a3b8}
#uptimer-preload .lbl{margin:12px 0 6px;font-size:11px;color:#94a3b8}
#uptimer-preload .strip{height:20px;border-radius:8px;background:#e2e8f0;overflow:hidden}
#uptimer-preload .usv{display:block;width:100%;height:100%}
#uptimer-preload .ft{margin-top:12px;font-size:11px;color:#94a3b8}
#uptimer-preload .ih{padding-top:24px;border-top:1px solid #e2e8f0}
@media (min-width:640px){#uptimer-preload .grid{grid-template-columns:repeat(2,minmax(0,1fr))}}
html.dark #uptimer-preload{background:#0f172a;color:#f8fafc}
html.dark #uptimer-preload .uh{background:rgba(15,23,42,.95);border-bottom-color:rgba(51,65,85,.9)}
html.dark #uptimer-preload .ud,#uptimer-preload .sgt{color:#cbd5e1}
html.dark #uptimer-preload .bn,html.dark #uptimer-preload .card{background:#1e293b;border-color:rgba(51,65,85,.95);box-shadow:none}
html.dark #uptimer-preload .bt{color:#cbd5e1}
html.dark #uptimer-preload .bu,#uptimer-preload .sgc,#uptimer-preload .up,#uptimer-preload .lbl,#uptimer-preload .ft{color:#94a3b8}
html.dark #uptimer-preload .mt{color:#94a3b8}
html.dark #uptimer-preload .strip{background:#334155}
html.dark #uptimer-preload .ih{border-top-color:#334155}
</style>`;

function renderIncidentCard(
  incident: PublicHomepageResponse['active_incidents'][number],
  formatTimestamp: (tsSec: number) => string,
): string {
  const impactVariant =
    incident.impact === 'major' || incident.impact === 'critical' ? 'down' : 'paused';

  let html = `<article class="card"><div class="row"><h4 class="mn">${escapeHtml(incident.title)}</h4><span class="sb sb-${impactVariant}">${escapeHtml(incident.impact)}</span></div><div class="ft">${formatTimestamp(incident.started_at)}</div>`;
  if (incident.message) {
    html += `<p class="bt">${escapeHtml(incident.message)}</p>`;
  }
  html += '</article>';
  return html;
}

function renderMaintenanceCard(
  window: NonNullable<PublicHomepageResponse['maintenance_history_preview']>,
  monitorNames: Map<number, string>,
  formatTimestamp: (tsSec: number) => string,
): string {
  let affected = '';
  for (let index = 0; index < window.monitor_ids.length; index += 1) {
    const monitorId = window.monitor_ids[index];
    if (typeof monitorId !== 'number') {
      continue;
    }
    if (index > 0) {
      affected += ', ';
    }
    affected += escapeHtml(monitorNames.get(monitorId) || `#${monitorId}`);
  }

  let html = `<article class="card"><div><h4 class="mn">${escapeHtml(window.title)}</h4><div class="ft">${formatTimestamp(window.starts_at)} - ${formatTimestamp(window.ends_at)}</div></div>`;
  if (affected) {
    html += `<div class="bt">Affected: ${affected}</div>`;
  }
  if (window.message) {
    html += `<p class="bt">${escapeHtml(window.message)}</p>`;
  }
  html += '</article>';
  return html;
}

function renderPreload(
  snapshot: PublicHomepageResponse,
  monitorNameById?: ReadonlyMap<number, string>,
): string {
  const overall = snapshot.overall_status;
  const siteTitle = snapshot.site_title;
  const siteDescription = snapshot.site_description;
  const bannerTitle = snapshot.banner.title;
  const generatedAt = snapshot.generated_at;
  const timeCache = new Map<number, string>();
  const formatTimestamp = (tsSec: number) => escapeHtml(formatTime(tsSec, timeCache));
  const needsMonitorNames =
    snapshot.maintenance_windows.active.length > 0 ||
    snapshot.maintenance_windows.upcoming.length > 0 ||
    snapshot.maintenance_history_preview !== null;
  const monitorNames = new Map<number, string>();
  if (needsMonitorNames) {
    if (monitorNameById) {
      for (const [monitorId, monitorName] of monitorNameById.entries()) {
        monitorNames.set(monitorId, monitorName);
      }
    } else {
      for (const monitor of snapshot.monitors) {
        monitorNames.set(monitor.id, monitor.name);
      }
    }
  }
  const groups = new Map<string, PublicHomepageResponse['monitors']>();
  for (const monitor of snapshot.monitors) {
    const key = monitorGroupLabel(monitor.group_name);
    const existing = groups.get(key) ?? [];
    existing.push(monitor);
    groups.set(key, existing);
  }

  let groupedMonitors = '';
  for (const [groupName, groupMonitors] of groups.entries()) {
    let monitorCards = '';
    for (const monitor of groupMonitors) {
      const uptimePct =
        typeof monitor.uptime_30d?.uptime_pct === 'number'
          ? `${monitor.uptime_30d.uptime_pct.toFixed(3)}%`
          : '-';
      const status = monitor.status;
      const statusLabel = escapeHtml(status);
      const lastCheckedLabel = monitor.last_checked_at
        ? `Last checked: ${formatTimestamp(monitor.last_checked_at)}`
        : 'Never checked';

      monitorCards += `<article class="card"><div class="row"><div class="lhs"><span class="dot dot-${status}"></span><div class="ut"><div class="mn">${escapeHtml(monitor.name)}</div><div class="mt">${escapeHtml(monitor.type)}</div></div></div><div class="rhs"><span class="up">${escapeHtml(uptimePct)}</span><span class="sb sb-${status}">${statusLabel}</span></div></div><div><div class="lbl">Availability (30d)</div><div class="strip">${buildUptimeStripSvg(monitor.uptime_day_strip)}</div></div><div><div class="lbl">Recent checks</div><div class="strip">${buildHeartbeatStripSvg(monitor.heartbeat_strip)}</div></div><div class="ft">${lastCheckedLabel}</div></article>`;
    }

    groupedMonitors += `<section class="sg"><div class="sgh"><h4 class="sgt">${escapeHtml(groupName)}</h4><span class="sgc">${groupMonitors.length}</span></div><div class="grid">${monitorCards}</div></section>`;
  }

  const activeMaintenance = snapshot.maintenance_windows.active;
  const upcomingMaintenance = snapshot.maintenance_windows.upcoming;
  const hiddenMonitorCount = Math.max(0, snapshot.monitor_count_total - snapshot.monitors.length);
  let maintenanceSection = '';
  if (activeMaintenance.length > 0 || upcomingMaintenance.length > 0) {
    let activeCards = '';
    for (const window of activeMaintenance) {
      activeCards += renderMaintenanceCard(window, monitorNames, formatTimestamp);
    }
    let upcomingCards = '';
    for (const window of upcomingMaintenance) {
      upcomingCards += renderMaintenanceCard(window, monitorNames, formatTimestamp);
    }

    maintenanceSection = `<section class="sec"><h3 class="sh">Scheduled Maintenance</h3>${activeCards ? `<div class="st">${activeCards}</div>` : ''}${upcomingCards ? `<div class="st">${upcomingCards}</div>` : ''}</section>`;
  }

  let incidentSection = '';
  if (snapshot.active_incidents.length > 0) {
    let incidentCards = '';
    for (const incident of snapshot.active_incidents) {
      incidentCards += renderIncidentCard(incident, formatTimestamp);
    }
    incidentSection = `<section class="sec"><h3 class="sh">Active Incidents</h3><div class="st">${incidentCards}</div></section>`;
  }

  const incidentHistory = snapshot.resolved_incident_preview
    ? renderIncidentCard(snapshot.resolved_incident_preview, formatTimestamp)
    : '<div class="card">No past incidents</div>';
  const maintenanceHistory = snapshot.maintenance_history_preview
    ? renderMaintenanceCard(snapshot.maintenance_history_preview, monitorNames, formatTimestamp)
    : '<div class="card">No past maintenance</div>';
  const descriptionHtml = siteDescription
    ? `<div class="ud">${escapeHtml(siteDescription)}</div>`
    : '';
  const hiddenMonitorMessage =
    hiddenMonitorCount > 0
      ? `<div class="card ft">${hiddenMonitorCount} more services will appear after the app finishes loading.</div>`
      : '';

  return `<div class="hp"><header class="uh"><div class="uw uhw"><div class="ut"><div class="un">${escapeHtml(siteTitle)}</div>${descriptionHtml}</div><span class="sb sb-${overall}">${escapeHtml(overall)}</span></div></header><main class="uw um"><section class="bn"><div class="bt">${escapeHtml(bannerTitle)}</div><div class="bu">Updated: ${formatTimestamp(generatedAt)}</div></section>${maintenanceSection}${incidentSection}<section class="sec"><h3 class="sh">Services</h3>${groupedMonitors}${hiddenMonitorMessage}</section><section class="sec ih"><div><h3 class="sh">Incident History</h3>${incidentHistory}</div><div><h3 class="sh">Maintenance History</h3>${maintenanceHistory}</div></section></main></div>`;
}

export function buildHomepageRenderArtifact(
  snapshot: PublicHomepageResponse,
): PublicHomepageRenderArtifact {
  const allMonitorNames = new Map<number, string>();
  for (const monitor of snapshot.monitors) {
    allMonitorNames.set(monitor.id, monitor.name);
  }
  const bootstrapSnapshot =
    snapshot.monitors.length > MAX_BOOTSTRAP_MONITORS
      ? {
          ...snapshot,
          bootstrap_mode: 'partial' as const,
          monitors: snapshot.monitors.slice(0, MAX_BOOTSTRAP_MONITORS),
        }
      : {
          ...snapshot,
          bootstrap_mode: 'full' as const,
        };
  const metaTitle = normalizeSnapshotText(snapshot.site_title, 'Uptimer');
  const fallbackDescription = normalizeSnapshotText(
    snapshot.banner.title,
    'Real-time status and incident updates.',
  );
  const metaDescription = normalizeSnapshotText(snapshot.site_description, fallbackDescription)
    .replace(/\s+/g, ' ')
    .trim();

  return {
    generated_at: snapshot.generated_at,
    style_tag: HOMEPAGE_PRELOAD_STYLE_TAG,
    preload_html: `<div id="uptimer-preload">${renderPreload(bootstrapSnapshot, allMonitorNames)}</div>`,
    bootstrap_script: `<script>globalThis.__UPTIMER_INITIAL_HOMEPAGE__=${safeJsonForInlineScript(bootstrapSnapshot)};</script>`,
    meta_title: metaTitle,
    meta_description: metaDescription,
  };
}

function looksLikeHomepagePayload(value: unknown): value is PublicHomepageResponse {
  if (!isRecord(value)) return false;
  return (
    typeof value.generated_at === 'number' &&
    typeof value.site_title === 'string' &&
    Array.isArray(value.monitors) &&
    Array.isArray(value.active_incidents)
  );
}

function readStoredHomepageSnapshotData(value: unknown): PublicHomepageResponse | null {
  if (!isRecord(value)) return null;

  const version = value.version;
  if (version === 2) {
    const parsed = storedHomepageSnapshotDataSchema.safeParse(value);
    return parsed.success ? parsed.data.data : null;
  }

  if (!looksLikeHomepagePayload(value)) {
    return null;
  }

  const parsed = publicHomepageResponseSchema.safeParse({
    ...value,
    bootstrap_mode: 'full',
    monitor_count_total: Array.isArray(value.monitors) ? value.monitors.length : 0,
  });
  if (!parsed.success) {
    return null;
  }

  return parsed.data;
}

function readStoredHomepageSnapshotRender(value: unknown): PublicHomepageRenderArtifact | null {
  if (!isRecord(value)) return null;
  if (value.version !== 2) return null;

  const parsed = storedHomepageSnapshotRenderSchema.safeParse(value);
  return parsed.success ? parsed.data.render : null;
}

function safeJsonParse(text: string): unknown | null {
  const trimmed = text.trim();
  if (!trimmed) return null;
  try {
    return JSON.parse(trimmed) as unknown;
  } catch {
    return null;
  }
}

async function readHomepageSnapshotRow(
  db: D1Database,
): Promise<{ generated_at: number; body_json: string } | null> {
  try {
    return await db
      .prepare(
        `
        SELECT generated_at, body_json
        FROM public_snapshots
        WHERE key = ?1
      `,
      )
      .bind(SNAPSHOT_KEY)
      .first<{ generated_at: number; body_json: string }>();
  } catch (err) {
    console.warn('homepage snapshot: read failed', err);
    return null;
  }
}

function isSameMinute(a: number, b: number): boolean {
  return Math.floor(a / 60) === Math.floor(b / 60);
}

export function getHomepageSnapshotKey() {
  return SNAPSHOT_KEY;
}

export function getHomepageSnapshotMaxAgeSeconds() {
  return MAX_AGE_SECONDS;
}

export function getHomepageSnapshotMaxStaleSeconds() {
  return MAX_STALE_SECONDS;
}

export async function readHomepageSnapshot(
  db: D1Database,
  now: number,
): Promise<{ data: PublicHomepageResponse; age: number } | null> {
  const row = await readHomepageSnapshotRow(db);
  if (!row) return null;

  const age = Math.max(0, now - row.generated_at);
  if (age > MAX_AGE_SECONDS) return null;

  const parsed = safeJsonParse(row.body_json);
  if (parsed === null) return null;

  const data = readStoredHomepageSnapshotData(parsed);
  if (!data) {
    console.warn('homepage snapshot: invalid payload');
    return null;
  }

  return {
    data,
    age,
  };
}

export async function readStaleHomepageSnapshot(
  db: D1Database,
  now: number,
): Promise<{ data: PublicHomepageResponse; age: number } | null> {
  const row = await readHomepageSnapshotRow(db);
  if (!row) return null;

  const age = Math.max(0, now - row.generated_at);
  if (age > MAX_STALE_SECONDS) return null;

  const parsed = safeJsonParse(row.body_json);
  if (parsed === null) return null;

  const data = readStoredHomepageSnapshotData(parsed);
  if (!data) {
    console.warn('homepage snapshot: invalid stale payload');
    return null;
  }

  return {
    data,
    age,
  };
}

export async function readHomepageSnapshotArtifact(
  db: D1Database,
  now: number,
): Promise<{ data: PublicHomepageRenderArtifact; age: number } | null> {
  const row = await readHomepageSnapshotRow(db);
  if (!row) return null;

  const age = Math.max(0, now - row.generated_at);
  if (age > MAX_AGE_SECONDS) return null;

  const parsed = safeJsonParse(row.body_json);
  if (parsed === null) return null;

  const render = readStoredHomepageSnapshotRender(parsed);
  if (!render) {
    console.warn('homepage snapshot: invalid render payload');
    return null;
  }

  return {
    data: render,
    age,
  };
}

export async function readStaleHomepageSnapshotArtifact(
  db: D1Database,
  now: number,
): Promise<{ data: PublicHomepageRenderArtifact; age: number } | null> {
  const row = await readHomepageSnapshotRow(db);
  if (!row) return null;

  const age = Math.max(0, now - row.generated_at);
  if (age > MAX_STALE_SECONDS) return null;

  const parsed = safeJsonParse(row.body_json);
  if (parsed === null) return null;

  const render = readStoredHomepageSnapshotRender(parsed);
  if (!render) {
    console.warn('homepage snapshot: invalid stale render payload');
    return null;
  }

  return {
    data: render,
    age,
  };
}

export async function readHomepageSnapshotGeneratedAt(
  db: D1Database,
): Promise<number | null> {
  const row = await readHomepageSnapshotRow(db);
  return row?.generated_at ?? null;
}

export async function writeHomepageSnapshot(
  db: D1Database,
  now: number,
  payload: PublicHomepageResponse,
): Promise<void> {
  const bodyJson = JSON.stringify({
    version: 2,
    data: payload,
    render: buildHomepageRenderArtifact(payload),
  } satisfies StoredHomepageSnapshot);
  await db
    .prepare(
      `
      INSERT INTO public_snapshots (key, generated_at, body_json, updated_at)
      VALUES (?1, ?2, ?3, ?4)
      ON CONFLICT(key) DO UPDATE SET
        generated_at = excluded.generated_at,
        body_json = excluded.body_json,
        updated_at = excluded.updated_at
    `,
    )
    .bind(SNAPSHOT_KEY, payload.generated_at, bodyJson, now)
    .run();
}

export function applyHomepageCacheHeaders(res: Response, ageSeconds: number): void {
  const remaining = Math.max(0, MAX_AGE_SECONDS - ageSeconds);
  const maxAge = Math.min(30, remaining);
  const stale = Math.max(0, remaining - maxAge);

  res.headers.set(
    'Cache-Control',
    `public, max-age=${maxAge}, stale-while-revalidate=${stale}, stale-if-error=${stale}`,
  );
}

export function toHomepageSnapshotPayload(value: unknown): PublicHomepageResponse {
  const parsed = publicHomepageResponseSchema.safeParse(value);
  if (!parsed.success) {
    throw new AppError(500, 'INTERNAL', 'Failed to generate homepage snapshot');
  }
  return parsed.data;
}

export async function refreshPublicHomepageSnapshot(opts: {
  db: D1Database;
  now: number;
  compute: () => Promise<unknown>;
}): Promise<void> {
  const payload = toHomepageSnapshotPayload(await opts.compute());
  await writeHomepageSnapshot(opts.db, opts.now, payload);
}

export async function refreshPublicHomepageSnapshotIfNeeded(opts: {
  db: D1Database;
  now: number;
  compute: () => Promise<unknown>;
}): Promise<boolean> {
  const generatedAt = await readHomepageSnapshotGeneratedAt(opts.db);
  if (generatedAt !== null && isSameMinute(generatedAt, opts.now)) {
    return false;
  }

  const acquired = await acquireLease(opts.db, REFRESH_LOCK_NAME, opts.now, 55);
  if (!acquired) {
    return false;
  }

  const latestGeneratedAt = await readHomepageSnapshotGeneratedAt(opts.db);
  if (latestGeneratedAt !== null && isSameMinute(latestGeneratedAt, opts.now)) {
    return false;
  }

  await refreshPublicHomepageSnapshot(opts);
  return true;
}
