/**
 * Databend monitoring (design 5.5): the overview, sessions, slow queries, table, storage and index statistics,
 * health, and the kill statement.
 *
 * Every read is a provider statement through the {@link DatabendStatementRunner} the provider passes in, so this file
 * names no request or page. The reads cover the DEFAULT catalog only, a stated limitation: walking every external
 * catalog for one panel would cost a statement per catalog [X35].
 *
 * Each panel degrades to empty when Databend answers that the surface is not there or not granted: unknown database
 * 1003 (`system_history` is enterprise and needs a grant), unknown table 1025, permission denied 1063, licence denied
 * 1112, unknown catalog 1119 and unimplemented 1002 (Trino's rule, [03 7.2]); any other error propagates, so a timeout
 * is never hidden behind an empty panel.
 *
 * Sessions [X08]: Databend creates a session per HTTP request, so `system.processes` lists one row per running
 * statement of the whole warehouse, every user's included, and needs no grant (`user_grant.rs:27-51`). The kill target
 * is that row's `id`, a SESSION id: `KILL QUERY` by the HTTP query id answers 1053, so the query id is never read. A
 * row's state is the word the monitoring panels count, "active", for the command `Query`, and the command lower-cased
 * otherwise (`Aborting`). `KILL QUERY` on a session stops its current statement and needs global SUPER.
 *
 * Studio's own reads are not running work: `system.processes` lists the reading statement too, so both reads of it
 * leave out `connection_id()`, the reading row's own id (measured equal on the fixture), and a panel sends its reads
 * one at a time, so no read of one panel sees a sibling read in flight.
 */
import { QueryError } from "@/lib/db/errors";
import type {
  ActiveSession,
  ActiveSessionDetails,
  DatabaseOverview,
  HealthInfo,
  IndexStats,
  PerformanceMetrics,
  SlowQuery,
  SlowQueryStats,
  StorageStats,
  TableStats,
} from "@/lib/db/types";
import { formatBytes, formatDuration } from "@/lib/db/utils/pool-manager";
import { quoteLiteral } from "@/lib/sql/values";
import {
  DATABEND_SURFACE_ROW_CUT,
  DATABEND_VERSION_SQL,
  type DatabendStatementRunner,
  databendIndexColumns,
  readCompleteRows,
} from "./objects";
import { DatabendError } from "./transport";

const PROVIDER = "databend";

/** What a panel prints for a value Databend does not publish. */
export const DATABEND_UNKNOWN_TEXT = "unknown";

/** What a string field says where Databend measures nothing: a cache ratio, a per-index size. */
export const DATABEND_UNAVAILABLE_TEXT = "N/A";

export const DATABEND_DEFAULT_SESSION_LIMIT = 50;
export const DATABEND_DEFAULT_SLOW_QUERY_LIMIT = 20;

/** The ceiling of every monitoring limit (design 3.12, [11 #2]). */
export const DATABEND_MAX_MONITORING_LIMIT = 500;

/** The rows the health summary embeds. */
const HEALTH_LIMIT = 10;

/** The codes that mean "this surface is not available here" rather than "the read went wrong". */
export const DATABEND_DEGRADE_CODES = Object.freeze([1003, 1025, 1063, 1112, 1119, 1002]);

/** A session id as `system.processes.id` spells it; anything else is refused before a statement is built. */
const DATABEND_KILL_ID_PATTERN = /^[A-Za-z0-9-]{1,64}$/;

/** Every sentence this module shows, so the provider doc can quote them and a test read them back. */
export const DATABEND_MONITORING_SENTENCES = Object.freeze({
  killNeedsId: "Stopping a statement needs its session id, which the Sessions panel lists.",
  killIdRefused: "A Databend session id is 1 to 64 letters, digits and hyphens, as the Sessions panel lists it.",
  killAsked: (pid: string) => `Asked Databend to stop the current statement of session ${pid}.`,
});

/** The tables the statistics read: the default catalog's own, without the two generated databases. */
const BASE_TABLES =
  "FROM default.system.tables WHERE catalog = 'default' AND table_type = 'BASE TABLE' AND database NOT IN ('system', 'information_schema')";

// ============================================================================
// Statements
// ============================================================================

const DATABEND_OVERVIEW_TABLES_SQL = `SELECT count(*) AS table_count, sum(data_compressed_size) AS compressed_bytes, sum(index_size) AS index_bytes ${BASE_TABLES}`;

/** `command` is `Query`, `Aborting` or `Idle` (`table_context.rs:137-151`) [X35]; the reading statement is not counted. */
const DATABEND_ACTIVE_QUERIES_SQL =
  "SELECT count(*) AS active_queries FROM default.system.processes WHERE command = 'Query' AND id <> connection_id()";

const DATABEND_INDEX_COUNT_SQL = "SELECT count(*) AS index_count FROM default.system.indexes";

export function databendSessionsSql(limit: number): string {
  return `SELECT id AS session_id, \`user\` AS user_name, host, database AS database_name, command, extra_info AS query_text, created_time, time AS elapsed_seconds FROM default.system.processes WHERE command <> 'Idle' AND id <> connection_id() ORDER BY created_time LIMIT ${limit}`;
}

/** `log_type` 2 is a finished statement; rows arrive through an ETL batch, so the newest lag. */
export function databendSlowQueriesSql(limit: number): string {
  return `SELECT query_id, query_text, query_duration_ms, result_rows FROM system_history.query_history WHERE log_type = 2 AND event_time >= subtract_hours(now(), 24) ORDER BY query_duration_ms DESC LIMIT ${limit}`;
}

function databendTableStatsSql(database?: string): string {
  const scope = database === undefined ? "" : ` AND database = ${quoteLiteral(database, PROVIDER)}`;
  return `SELECT database AS schema_name, name AS table_name, num_rows, data_compressed_size, index_size ${BASE_TABLES}${scope} ORDER BY data_compressed_size DESC`;
}

const DATABEND_STORAGE_SQL = `SELECT database AS database_name, sum(data_compressed_size) AS compressed_bytes, sum(index_size) AS index_bytes ${BASE_TABLES} GROUP BY database ORDER BY database`;

function databendIndexStatsSql(database?: string): string {
  const scope = database === undefined ? "" : ` WHERE database = ${quoteLiteral(database, PROVIDER)}`;
  return `SELECT database AS schema_name, \`table\` AS table_name, name AS index_name, \`type\` AS index_type, definition FROM default.system.indexes${scope} ORDER BY database, \`table\`, name`;
}

/** The kill of one session's current statement; the id is checked against {@link DATABEND_KILL_ID_PATTERN} first. */
function databendKillSql(pid: string): string {
  if (pid === "") throw new QueryError(DATABEND_MONITORING_SENTENCES.killNeedsId, PROVIDER);
  if (!DATABEND_KILL_ID_PATTERN.test(pid)) throw new QueryError(DATABEND_MONITORING_SENTENCES.killIdRefused, PROVIDER);
  return `KILL QUERY ${quoteLiteral(pid, PROVIDER)}`;
}

// ============================================================================
// Reads
// ============================================================================

function readText(value: unknown): string {
  return typeof value === "string" ? value : "";
}

/**
 * A number Databend reported, or undefined for a NULL; an integer past 2^53 arrives as its text and reads as its
 * nearest number, so not exact.
 */
function readNumber(value: unknown): number | undefined {
  if (typeof value === "number") return value;
  return typeof value === "string" ? Number(value) : undefined;
}

/** A `Timestamp` in the UTC the provider statements pin (design 3.3), or undefined when unreadable. */
function readInstant(value: unknown): Date | undefined {
  const parsed = new Date(`${readText(value).replace(" ", "T")}Z`);
  return Number.isNaN(parsed.getTime()) ? undefined : parsed;
}

/** A requested limit clamped to 1..500, or `fallback` when none or no finite number was given. */
function clampMonitoringLimit(limit: number | undefined, fallback: number): number {
  if (limit === undefined || !Number.isFinite(limit)) return fallback;
  return Math.min(DATABEND_MAX_MONITORING_LIMIT, Math.max(1, Math.trunc(limit)));
}

/** One panel read: whole, or no rows when the surface is not available here. */
async function readPanelRows(
  runner: DatabendStatementRunner,
  sql: string,
  surface: string,
): Promise<Record<string, unknown>[]> {
  try {
    return await readCompleteRows(runner, sql, surface);
  } catch (error) {
    if (error instanceof DatabendError && error.code !== undefined && DATABEND_DEGRADE_CODES.includes(error.code)) {
      return [];
    }
    throw error;
  }
}

async function readPanelRow(runner: DatabendStatementRunner, sql: string): Promise<Record<string, unknown> | null> {
  const rows = await readPanelRows(runner, sql, "overview");
  return rows[0] ?? null;
}

/** Bytes as a formatted size and its number, each omitted when Databend reported no figure. */
function sizeFields<K extends string>(key: K, bytes: number | undefined) {
  return bytes === undefined ? {} : { [key]: formatBytes(bytes), [`${key}Bytes`]: bytes };
}

// ============================================================================
// Panels
// ============================================================================

/** The default catalog's overview; uptime and a connection ceiling are not published, and the size is compressed. */
export async function getOverview(runner: DatabendStatementRunner): Promise<DatabaseOverview> {
  // One at a time, so the active count never includes a sibling read.
  const version = await readPanelRow(runner, DATABEND_VERSION_SQL);
  const tables = await readPanelRow(runner, DATABEND_OVERVIEW_TABLES_SQL);
  const active = await readPanelRow(runner, DATABEND_ACTIVE_QUERIES_SQL);
  const indexes = await readPanelRow(runner, DATABEND_INDEX_COUNT_SQL);
  // An empty catalog sums to NULL, which is a measured zero; a degraded read is no figure at all.
  const sizeBytes =
    tables === null ? undefined : (readNumber(tables.compressed_bytes) ?? 0) + (readNumber(tables.index_bytes) ?? 0);
  const activeQueries = readNumber(active?.active_queries);
  return {
    version: version === null ? DATABEND_UNKNOWN_TEXT : readText(version.server_version),
    uptime: DATABEND_UNKNOWN_TEXT,
    ...(activeQueries === undefined ? {} : { activeConnections: activeQueries }),
    // Zero means "no limit published".
    maxConnections: 0,
    databaseSize: sizeBytes === undefined ? DATABEND_UNAVAILABLE_TEXT : formatBytes(sizeBytes),
    ...(sizeBytes === undefined ? {} : { databaseSizeBytes: sizeBytes }),
    tableCount: readNumber(tables?.table_count) ?? 0,
    indexCount: readNumber(indexes?.index_count) ?? 0,
  };
}

/** Databend publishes none of these figures to SQL, so every field stays absent rather than a fabricated zero. */
export function getPerformanceMetrics(): PerformanceMetrics {
  return {};
}

/** One row per finished execution, so `calls` is 1 and the total is the average. */
export async function getSlowQueries(
  runner: DatabendStatementRunner,
  options: { limit?: number } = {},
): Promise<SlowQueryStats[]> {
  const limit = clampMonitoringLimit(options.limit, DATABEND_DEFAULT_SLOW_QUERY_LIMIT);
  const rows = await readPanelRows(runner, databendSlowQueriesSql(limit), "slow queries");
  return rows.map((row) => {
    const durationMs = readNumber(row.query_duration_ms) ?? 0;
    return {
      queryId: readText(row.query_id),
      query: readText(row.query_text),
      calls: 1,
      totalTime: durationMs,
      avgTime: durationMs,
      rows: readNumber(row.result_rows) ?? 0,
    };
  });
}

/**
 * The state the monitoring panels count: they count `state === "active"`, PostgreSQL's word for a statement in flight,
 * which is what the command `Query` means; another command (`Aborting`) keeps its own word, lower-cased, so no panel
 * counts it as active or idle, as Neo4j's sessions do.
 */
function sessionState(command: string): string {
  return command === "Query" ? "active" : command.toLowerCase();
}

/** Every running statement of the warehouse, keyed by its session id [X08]. */
export async function getActiveSessions(
  runner: DatabendStatementRunner,
  options: { limit?: number } = {},
): Promise<ActiveSessionDetails[]> {
  const limit = clampMonitoringLimit(options.limit, DATABEND_DEFAULT_SESSION_LIMIT);
  const rows = await readPanelRows(runner, databendSessionsSql(limit), "sessions");
  return rows.map((row) => {
    const host = readText(row.host);
    const queryStart = readInstant(row.created_time);
    // `time` is seconds since the session was created, and a session lives for one request, so it is the age.
    const durationMs = (readNumber(row.elapsed_seconds) ?? 0) * 1000;
    const session: ActiveSessionDetails = {
      pid: readText(row.session_id),
      user: readText(row.user_name),
      database: readText(row.database_name),
      state: sessionState(readText(row.command)),
      query: readText(row.query_text),
      duration: formatDuration(durationMs),
      durationMs,
    };
    if (host !== "") session.clientAddr = host;
    if (queryStart !== undefined) session.queryStart = queryStart;
    return session;
  });
}

/** Sends the kill of one session's current statement; a refusal or a failure propagates. */
export async function killSession(runner: DatabendStatementRunner, pid: string): Promise<void> {
  await runner(databendKillSql(pid), DATABEND_SURFACE_ROW_CUT);
}

/** The default catalog's tables, largest first; `schema` narrows to one database. */
export async function getTableStats(
  runner: DatabendStatementRunner,
  options: { schema?: string } = {},
): Promise<TableStats[]> {
  const rows = await readPanelRows(runner, databendTableStatsSql(options.schema), "table statistics");
  return rows.map((row) => {
    const tableBytes = readNumber(row.data_compressed_size);
    const indexBytes = readNumber(row.index_size);
    const totalBytes = (tableBytes ?? 0) + (indexBytes ?? 0);
    const stats: TableStats = {
      schemaName: readText(row.schema_name),
      tableName: readText(row.table_name),
      // Required by the type; a table without statistics (an external one) reports NULL.
      rowCount: readNumber(row.num_rows) ?? 0,
      totalSize: formatBytes(totalBytes),
      totalSizeBytes: totalBytes,
    };
    return Object.assign(stats, sizeFields("tableSize", tableBytes), sizeFields("indexSize", indexBytes));
  });
}

/** One row per database of the default catalog: Databend owns its storage, so the sum is a real footprint. */
export async function getStorageStats(runner: DatabendStatementRunner): Promise<StorageStats[]> {
  const rows = await readPanelRows(runner, DATABEND_STORAGE_SQL, "storage statistics");
  return rows.map((row) => {
    const bytes = (readNumber(row.compressed_bytes) ?? 0) + (readNumber(row.index_bytes) ?? 0);
    return { name: readText(row.database_name), size: formatBytes(bytes), sizeBytes: bytes };
  });
}

/**
 * The search indexes of the default catalog. Sizes exist only per table and type, and no scan counter exists, so
 * the size is "N/A" and `scans` the zero the type requires; no index is a key.
 */
export async function getIndexStats(
  runner: DatabendStatementRunner,
  options: { schema?: string } = {},
): Promise<IndexStats[]> {
  const rows = await readPanelRows(runner, databendIndexStatsSql(options.schema), "index statistics");
  return rows.map((row) => {
    const tableName = readText(row.table_name);
    return {
      schemaName: readText(row.schema_name),
      tableName,
      indexName: readText(row.index_name),
      indexType: readText(row.index_type),
      columns: databendIndexColumns(tableName, readText(row.definition)),
      isUnique: false,
      isPrimary: false,
      indexSize: DATABEND_UNAVAILABLE_TEXT,
      scans: 0,
    };
  });
}

function toSlowQuery(stats: SlowQueryStats): SlowQuery {
  return { query: stats.query, calls: stats.calls, avgTime: formatDuration(stats.avgTime) };
}

function toActiveSession(session: ActiveSessionDetails): ActiveSession {
  return {
    pid: session.pid,
    user: session.user,
    database: session.database,
    state: session.state,
    query: session.query,
    duration: session.duration,
  };
}

/** Size, active queries, slow queries and sessions; Databend publishes no cache ratio. */
export async function getHealth(runner: DatabendStatementRunner): Promise<HealthInfo> {
  // One at a time, so neither the count nor the sessions list Studio's own sibling reads.
  const overview = await getOverview(runner);
  const slow = await getSlowQueries(runner, { limit: HEALTH_LIMIT });
  const sessions = await getActiveSessions(runner, { limit: HEALTH_LIMIT });
  return {
    ...(overview.activeConnections === undefined ? {} : { activeConnections: overview.activeConnections }),
    databaseSize: overview.databaseSize,
    cacheHitRatio: DATABEND_UNAVAILABLE_TEXT,
    slowQueries: slow.map(toSlowQuery),
    activeSessions: sessions.map(toActiveSession),
  };
}
