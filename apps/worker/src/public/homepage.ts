import type { PublicHomepageResponse } from '../schemas/public-homepage';

import {
  buildPublicMonitorCards,
  buildPublicStatusBanner,
  listIncidentMonitorIdsByIncidentId,
  listMaintenanceWindowMonitorIdsByWindowId,
  listVisibleActiveIncidents,
  listVisibleMaintenanceWindows,
  readPublicSiteSettings,
  toIncidentImpact,
  toIncidentStatus,
  type IncidentRow,
  type MaintenanceWindowRow,
} from './data';
import {
  filterStatusPageScopedMonitorIds,
  incidentStatusPageVisibilityPredicate,
  listStatusPageVisibleMonitorIds,
  maintenanceWindowStatusPageVisibilityPredicate,
  shouldIncludeStatusPageScopedItem,
} from './visibility';

const PREVIEW_BATCH_LIMIT = 50;

type IncidentSummary = PublicHomepageResponse['active_incidents'][number];
type MaintenancePreview = NonNullable<PublicHomepageResponse['maintenance_history_preview']>;

function toHeartbeatStatusCode(
  status: Awaited<ReturnType<typeof buildPublicMonitorCards>>['monitors'][number]['heartbeats'][number]['status'],
): string {
  switch (status) {
    case 'up':
      return 'u';
    case 'down':
      return 'd';
    case 'maintenance':
      return 'm';
    case 'unknown':
    default:
      return 'x';
  }
}

function toIncidentSummary(row: IncidentRow): IncidentSummary {
  return {
    id: row.id,
    title: row.title,
    status: toIncidentStatus(row.status),
    impact: toIncidentImpact(row.impact),
    message: row.message,
    started_at: row.started_at,
    resolved_at: row.resolved_at,
  };
}

function toMaintenancePreview(
  row: MaintenanceWindowRow,
  monitorIds: number[],
): MaintenancePreview {
  return {
    id: row.id,
    title: row.title,
    message: row.message,
    starts_at: row.starts_at,
    ends_at: row.ends_at,
    monitor_ids: monitorIds,
  };
}

function toHomepageHeartbeatStrip(
  heartbeats: Awaited<ReturnType<typeof buildPublicMonitorCards>>['monitors'][number]['heartbeats'],
): PublicHomepageResponse['monitors'][number]['heartbeat_strip'] {
  const count = heartbeats.length;
  const checkedAt = new Array<number>(count);
  const latencyMs = new Array<number | null>(count);
  let statusCodes = '';

  for (let index = 0; index < count; index += 1) {
    const heartbeat = heartbeats[index];
    if (!heartbeat) continue;

    checkedAt[index] = heartbeat.checked_at;
    latencyMs[index] = heartbeat.latency_ms;
    statusCodes += toHeartbeatStatusCode(heartbeat.status);
  }

  return {
    checked_at: checkedAt,
    status_codes: statusCodes,
    latency_ms: latencyMs,
  };
}

function toHomepageUptimeDayStrip(
  days: Awaited<ReturnType<typeof buildPublicMonitorCards>>['monitors'][number]['uptime_days'],
): PublicHomepageResponse['monitors'][number]['uptime_day_strip'] {
  const count = days.length;
  const dayStartAt = new Array<number>(count);
  const downtimeSec = new Array<number>(count);
  const unknownSec = new Array<number>(count);
  const uptimePctMilli = new Array<number | null>(count);

  for (let index = 0; index < count; index += 1) {
    const day = days[index];
    if (!day) continue;

    dayStartAt[index] = day.day_start_at;
    downtimeSec[index] = day.downtime_sec;
    unknownSec[index] = day.unknown_sec;
    uptimePctMilli[index] = day.uptime_pct === null ? null : Math.round(day.uptime_pct * 1000);
  }

  return {
    day_start_at: dayStartAt,
    downtime_sec: downtimeSec,
    unknown_sec: unknownSec,
    uptime_pct_milli: uptimePctMilli,
  };
}

function toHomepageMonitorCard(
  monitor: Awaited<ReturnType<typeof buildPublicMonitorCards>>['monitors'][number],
): PublicHomepageResponse['monitors'][number] {
  return {
    id: monitor.id,
    name: monitor.name,
    type: monitor.type,
    group_name: monitor.group_name,
    status: monitor.status,
    is_stale: monitor.is_stale,
    last_checked_at: monitor.last_checked_at,
    heartbeat_strip: toHomepageHeartbeatStrip(monitor.heartbeats),
    uptime_30d: monitor.uptime_30d
      ? {
          uptime_pct: monitor.uptime_30d.uptime_pct,
        }
      : null,
    uptime_day_strip: toHomepageUptimeDayStrip(monitor.uptime_days),
  };
}

async function findLatestVisibleResolvedIncident(
  db: D1Database,
  includeHiddenMonitors: boolean,
): Promise<IncidentRow | null> {
  const incidentVisibilitySql = incidentStatusPageVisibilityPredicate(includeHiddenMonitors);
  let cursor: number | null = null;

  while (true) {
    const queryResult: { results: IncidentRow[] | undefined } = cursor
      ? await db
          .prepare(
            `
            SELECT id, title, status, impact, message, started_at, resolved_at
            FROM incidents
            WHERE status = 'resolved'
              AND ${incidentVisibilitySql}
              AND id < ?2
            ORDER BY id DESC
            LIMIT ?1
          `,
          )
          .bind(PREVIEW_BATCH_LIMIT, cursor)
          .all<IncidentRow>()
      : await db
          .prepare(
            `
            SELECT id, title, status, impact, message, started_at, resolved_at
            FROM incidents
            WHERE status = 'resolved'
              AND ${incidentVisibilitySql}
            ORDER BY id DESC
            LIMIT ?1
          `,
          )
          .bind(PREVIEW_BATCH_LIMIT)
          .all<IncidentRow>();

    const rows: IncidentRow[] = queryResult.results ?? [];
    if (rows.length === 0) return null;

    const monitorIdsByIncidentId = await listIncidentMonitorIdsByIncidentId(
      db,
      rows.map((row) => row.id),
    );
    const visibleMonitorIds = includeHiddenMonitors
      ? new Set<number>()
      : await listStatusPageVisibleMonitorIds(
          db,
          [...monitorIdsByIncidentId.values()].flat(),
        );

    for (const row of rows) {
      const originalMonitorIds = monitorIdsByIncidentId.get(row.id) ?? [];
      const filteredMonitorIds = filterStatusPageScopedMonitorIds(
        originalMonitorIds,
        visibleMonitorIds,
        includeHiddenMonitors,
      );

      if (shouldIncludeStatusPageScopedItem(originalMonitorIds, filteredMonitorIds)) {
        return row;
      }
    }

    if (rows.length < PREVIEW_BATCH_LIMIT) {
      return null;
    }

    cursor = rows[rows.length - 1]?.id ?? null;
  }
}

async function findLatestVisibleHistoricalMaintenanceWindow(
  db: D1Database,
  now: number,
  includeHiddenMonitors: boolean,
): Promise<{ row: MaintenanceWindowRow; monitorIds: number[] } | null> {
  const maintenanceVisibilitySql = maintenanceWindowStatusPageVisibilityPredicate(
    includeHiddenMonitors,
  );
  let cursor: number | null = null;

  while (true) {
    const queryResult: { results: MaintenanceWindowRow[] | undefined } = cursor
      ? await db
          .prepare(
            `
            SELECT id, title, message, starts_at, ends_at, created_at
            FROM maintenance_windows
            WHERE ends_at <= ?1
              AND ${maintenanceVisibilitySql}
              AND id < ?3
            ORDER BY id DESC
            LIMIT ?2
          `,
          )
          .bind(now, PREVIEW_BATCH_LIMIT, cursor)
          .all<MaintenanceWindowRow>()
      : await db
          .prepare(
            `
            SELECT id, title, message, starts_at, ends_at, created_at
            FROM maintenance_windows
            WHERE ends_at <= ?1
              AND ${maintenanceVisibilitySql}
            ORDER BY id DESC
            LIMIT ?2
          `,
          )
          .bind(now, PREVIEW_BATCH_LIMIT)
          .all<MaintenanceWindowRow>();

    const rows: MaintenanceWindowRow[] = queryResult.results ?? [];
    if (rows.length === 0) return null;

    const monitorIdsByWindowId = await listMaintenanceWindowMonitorIdsByWindowId(
      db,
      rows.map((row) => row.id),
    );
    const visibleMonitorIds = includeHiddenMonitors
      ? new Set<number>()
      : await listStatusPageVisibleMonitorIds(
          db,
          [...monitorIdsByWindowId.values()].flat(),
        );

    for (const row of rows) {
      const originalMonitorIds = monitorIdsByWindowId.get(row.id) ?? [];
      const filteredMonitorIds = filterStatusPageScopedMonitorIds(
        originalMonitorIds,
        visibleMonitorIds,
        includeHiddenMonitors,
      );

      if (shouldIncludeStatusPageScopedItem(originalMonitorIds, filteredMonitorIds)) {
        return { row, monitorIds: filteredMonitorIds };
      }
    }

    if (rows.length < PREVIEW_BATCH_LIMIT) {
      return null;
    }

    cursor = rows[rows.length - 1]?.id ?? null;
  }
}

export async function computePublicHomepagePayload(
  db: D1Database,
  now: number,
): Promise<PublicHomepageResponse> {
  const includeHiddenMonitors = false;

  const [
    monitorData,
    activeIncidents,
    maintenanceWindows,
    settings,
    resolvedIncidentPreview,
    maintenanceHistoryPreview,
  ] = await Promise.all([
    buildPublicMonitorCards(db, now, { includeHiddenMonitors }),
    listVisibleActiveIncidents(db, includeHiddenMonitors),
    listVisibleMaintenanceWindows(db, now, includeHiddenMonitors),
    readPublicSiteSettings(db),
    findLatestVisibleResolvedIncident(db, includeHiddenMonitors),
    findLatestVisibleHistoricalMaintenanceWindow(db, now, includeHiddenMonitors),
  ]);

  const monitors = new Array<PublicHomepageResponse['monitors'][number]>(monitorData.monitors.length);
  for (let index = 0; index < monitorData.monitors.length; index += 1) {
    const monitor = monitorData.monitors[index];
    if (!monitor) continue;
    monitors[index] = toHomepageMonitorCard(monitor);
  }

  const activeIncidentSummaries = new Array<IncidentSummary>(activeIncidents.length);
  for (let index = 0; index < activeIncidents.length; index += 1) {
    const incident = activeIncidents[index];
    if (!incident) continue;
    activeIncidentSummaries[index] = toIncidentSummary(incident.row);
  }

  const activeMaintenancePreview = new Array<MaintenancePreview>(maintenanceWindows.active.length);
  for (let index = 0; index < maintenanceWindows.active.length; index += 1) {
    const window = maintenanceWindows.active[index];
    if (!window) continue;
    activeMaintenancePreview[index] = toMaintenancePreview(window.row, window.monitorIds);
  }

  const upcomingMaintenancePreview = new Array<MaintenancePreview>(
    maintenanceWindows.upcoming.length,
  );
  for (let index = 0; index < maintenanceWindows.upcoming.length; index += 1) {
    const window = maintenanceWindows.upcoming[index];
    if (!window) continue;
    upcomingMaintenancePreview[index] = toMaintenancePreview(window.row, window.monitorIds);
  }

  return {
    generated_at: now,
    bootstrap_mode: 'full',
    monitor_count_total: monitorData.monitors.length,
    site_title: settings.site_title,
    site_description: settings.site_description,
    site_locale: settings.site_locale,
    site_timezone: settings.site_timezone,
    uptime_rating_level: monitorData.uptimeRatingLevel,
    overall_status: monitorData.overallStatus,
    banner: buildPublicStatusBanner({
      counts: monitorData.summary,
      monitors: monitorData.monitors,
      activeIncidents,
      activeMaintenanceWindows: maintenanceWindows.active,
    }),
    summary: monitorData.summary,
    monitors,
    active_incidents: activeIncidentSummaries,
    maintenance_windows: {
      active: activeMaintenancePreview,
      upcoming: upcomingMaintenancePreview,
    },
    resolved_incident_preview: resolvedIncidentPreview
      ? toIncidentSummary(resolvedIncidentPreview)
      : null,
    maintenance_history_preview: maintenanceHistoryPreview
      ? toMaintenancePreview(maintenanceHistoryPreview.row, maintenanceHistoryPreview.monitorIds)
      : null,
  };
}
