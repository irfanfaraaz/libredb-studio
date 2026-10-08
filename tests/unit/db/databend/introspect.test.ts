import { describe, expect, test } from "bun:test";
import { QueryError } from "@/lib/db/errors";
import {
  DATABEND_DEFAULT_SESSION_LIMIT,
  DATABEND_DEFAULT_SLOW_QUERY_LIMIT,
  DATABEND_DEGRADE_CODES,
  DATABEND_MAX_MONITORING_LIMIT,
  DATABEND_MONITORING_SENTENCES,
  DATABEND_UNAVAILABLE_TEXT,
  DATABEND_UNKNOWN_TEXT,
  databendSessionsSql,
  databendSlowQueriesSql,
  getActiveSessions,
  getHealth,
  getIndexStats,
  getOverview,
  getPerformanceMetrics,
  getSlowQueries,
  getStorageStats,
  getTableStats,
  killSession,
} from "@/lib/db/providers/sql/databend/introspect";
import {
  DATABEND_OBJECT_SENTENCES,
  DATABEND_SURFACE_ROW_CUT,
  DATABEND_VERSION_SQL,
  type DatabendStatementRunner,
} from "@/lib/db/providers/sql/databend/objects";
import {
  DatabendError,
  type DatabendTruncation,
  type StatementOutcome,
} from "@/lib/db/providers/sql/databend/transport";

/** The en dash and the em dash, built from their code points so this file holds neither. */
const DASHES = new RegExp("[\\u2013\\u2014]");

function outcome(
  columns: readonly (readonly [string, string])[],
  rows: readonly (readonly (string | null)[])[],
  truncated: DatabendTruncation | null = null,
): StatementOutcome {
  return {
    schema: columns.map(([name, type]) => ({ name, type })),
    rows,
    truncated,
    notices: [],
    hasResultSet: true,
    affect: null,
  };
}

/** A runner answering by statement text, recording every statement it was handed. */
function routed(answers: Record<string, StatementOutcome | Error>) {
  const calls: { sql: string; rowCut: number }[] = [];
  const runner: DatabendStatementRunner = async (sql, rowCut) => {
    calls.push({ sql, rowCut });
    const answer = answers[sql];
    if (answer instanceof Error) throw answer;
    return answer;
  };
  return { runner, calls };
}

function failure(code: number): DatabendError {
  return new DatabendError("statement", `failed with ${code}`, { code });
}

const SESSION_SCHEMA = [
  ["session_id", "String"],
  ["user_name", "String"],
  ["host", "Nullable(String)"],
  ["database_name", "String"],
  ["status", "String"],
  ["query_text", "String"],
  ["current_query_id", "String"],
  ["created_time", "Timestamp"],
  ["elapsed_seconds", "UInt64"],
] as const;

const SESSION_ROW = [
  "5c00e52f-34c2-4cab-8ba8-c1f1121157bf",
  "studio_reader",
  "172.20.0.1",
  "libredb_demo",
  "Executing pipeline",
  "SELECT 1",
  "01a11896a18d7ae399fb6947927521b2",
  "2026-10-07 22:58:07.375243",
  "3",
];

const SLOW_SCHEMA = [
  ["query_id", "String"],
  ["query_text", "String"],
  ["query_duration_ms", "Int64"],
  ["result_rows", "UInt64"],
] as const;

/** The design 5.5 statements, written out; the tests below hold each panel to them. */
const BASE_TABLES =
  "FROM default.system.tables WHERE catalog = 'default' AND table_type = 'BASE TABLE' AND database NOT IN ('system', 'information_schema')";
const OVERVIEW_TABLES_SQL = `SELECT count(*) AS table_count, sum(data_compressed_size) AS compressed_bytes, sum(index_size) AS index_bytes ${BASE_TABLES}`;
const ACTIVE_QUERIES_SQL =
  "SELECT count(*) AS active_queries FROM default.system.processes WHERE command = 'Query' AND id <> connection_id()";
const INDEX_COUNT_SQL = "SELECT count(*) AS index_count FROM default.system.indexes";
const STORAGE_SQL = `SELECT database AS database_name, sum(data_compressed_size) AS compressed_bytes, sum(index_size) AS index_bytes ${BASE_TABLES} GROUP BY database ORDER BY database`;
const tableStatsSql = (scope = "") =>
  `SELECT database AS schema_name, name AS table_name, num_rows, data_compressed_size, index_size ${BASE_TABLES}${scope} ORDER BY data_compressed_size DESC`;
const indexStatsSql = (scope = "") =>
  `SELECT database AS schema_name, \`table\` AS table_name, name AS index_name, \`type\` AS index_type, definition FROM default.system.indexes${scope} ORDER BY database, \`table\`, name`;

/** The statements a read sends to a runner that answers every one with no rows. */
async function sentBy(read: (runner: DatabendStatementRunner) => Promise<unknown>): Promise<string[]> {
  const sent: string[] = [];
  await read(async (sql) => {
    sent.push(sql);
    return outcome([], []);
  });
  return sent;
}

const OVERVIEW_ANSWERS = {
  [DATABEND_VERSION_SQL]: outcome([["server_version", "String"]], [["8.0.26-v1.2.951-nightly"]]),
  [OVERVIEW_TABLES_SQL]: outcome(
    [
      ["table_count", "UInt64"],
      ["compressed_bytes", "Nullable(UInt64)"],
      ["index_bytes", "Nullable(UInt64)"],
    ],
    [["2", "14915", "3389"]],
  ),
  [ACTIVE_QUERIES_SQL]: outcome([["active_queries", "UInt64"]], [["1"]]),
  [INDEX_COUNT_SQL]: outcome([["index_count", "UInt64"]], [["4"]]),
};

describe("the design 5.5 statements, exactly, as each panel sends them", () => {
  test("the overview reads the default catalog's sums and counts command = 'Query' [X35], one at a time", async () => {
    expect(OVERVIEW_TABLES_SQL).toBe(
      "SELECT count(*) AS table_count, sum(data_compressed_size) AS compressed_bytes, sum(index_size) AS index_bytes FROM default.system.tables WHERE catalog = 'default' AND table_type = 'BASE TABLE' AND database NOT IN ('system', 'information_schema')",
    );
    const { runner, calls } = routed(OVERVIEW_ANSWERS);
    await getOverview(runner);
    expect(calls.map((call) => call.sql)).toEqual([
      DATABEND_VERSION_SQL,
      OVERVIEW_TABLES_SQL,
      ACTIVE_QUERIES_SQL,
      INDEX_COUNT_SQL,
    ]);
  });

  test("the sessions read covers every non-idle session of the warehouse", () => {
    expect(databendSessionsSql(7)).toBe(
      "SELECT id AS session_id, `user` AS user_name, host, database AS database_name, status, extra_info AS query_text, current_query_id, created_time, time AS elapsed_seconds FROM default.system.processes WHERE command <> 'Idle' AND id <> connection_id() ORDER BY created_time LIMIT 7",
    );
  });

  test("the slow queries read the finished statements of the last 24 hours", () => {
    expect(databendSlowQueriesSql(9)).toBe(
      "SELECT query_id, query_text, query_duration_ms, result_rows FROM system_history.query_history WHERE log_type = 2 AND event_time >= subtract_hours(now(), 24) ORDER BY query_duration_ms DESC LIMIT 9",
    );
  });

  test("table, storage and index stats, with the database filter quoted", async () => {
    expect(await sentBy((runner) => getTableStats(runner))).toEqual([tableStatsSql()]);
    expect(await sentBy((runner) => getTableStats(runner, { schema: "d`b'\\y" }))).toEqual([
      tableStatsSql(" AND database = 'd`b''\\\\y'"),
    ]);
    expect(await sentBy(getStorageStats)).toEqual([STORAGE_SQL]);
    expect(await sentBy((runner) => getIndexStats(runner))).toEqual([indexStatsSql()]);
    expect(await sentBy((runner) => getIndexStats(runner, { schema: "d'" }))).toEqual([
      indexStatsSql(" WHERE database = 'd'''"),
    ]);
  });
});

describe("limits", () => {
  test("clamped to 1..500, the default for none or a non-number", async () => {
    expect(DATABEND_MAX_MONITORING_LIMIT).toBe(500);
    const cases: [number | undefined, number][] = [
      [undefined, DATABEND_DEFAULT_SLOW_QUERY_LIMIT],
      [Number.NaN, DATABEND_DEFAULT_SLOW_QUERY_LIMIT],
      [0, 1],
      [-5, 1],
      [2.7, 2],
      [501, 500],
      [Number.POSITIVE_INFINITY, DATABEND_DEFAULT_SLOW_QUERY_LIMIT],
    ];
    const sent = await Promise.all(cases.map(([limit]) => sentBy((runner) => getSlowQueries(runner, { limit }))));
    expect(sent).toEqual(cases.map(([, clamped]) => [databendSlowQueriesSql(clamped)]));
  });

  test("the panels send the clamped limit", async () => {
    const sessions = routed({ [databendSessionsSql(500)]: outcome(SESSION_SCHEMA, []) });
    await getActiveSessions(sessions.runner, { limit: 10_000 });
    expect(sessions.calls).toEqual([{ sql: databendSessionsSql(500), rowCut: DATABEND_SURFACE_ROW_CUT }]);

    const defaults = routed({
      [databendSessionsSql(DATABEND_DEFAULT_SESSION_LIMIT)]: outcome(SESSION_SCHEMA, []),
      [databendSlowQueriesSql(DATABEND_DEFAULT_SLOW_QUERY_LIMIT)]: outcome(SLOW_SCHEMA, []),
    });
    await getActiveSessions(defaults.runner);
    await getSlowQueries(defaults.runner);
    expect(defaults.calls.map((call) => call.sql)).toEqual([
      databendSessionsSql(DATABEND_DEFAULT_SESSION_LIMIT),
      databendSlowQueriesSql(DATABEND_DEFAULT_SLOW_QUERY_LIMIT),
    ]);
  });
});

describe("sessions and the kill [X08]", () => {
  test("a row maps id to pid, and current_query_id is shown, never the kill target", async () => {
    const { runner } = routed({ [databendSessionsSql(5)]: outcome(SESSION_SCHEMA, [SESSION_ROW]) });
    expect(await getActiveSessions(runner, { limit: 5 })).toEqual([
      {
        pid: "5c00e52f-34c2-4cab-8ba8-c1f1121157bf",
        user: "studio_reader",
        database: "libredb_demo",
        clientAddr: "172.20.0.1",
        state: DATABEND_MONITORING_SENTENCES.sessionState("Executing pipeline", "01a11896a18d7ae399fb6947927521b2"),
        query: "SELECT 1",
        queryStart: new Date("2026-10-07T22:58:07.375Z"),
        duration: "3.00s",
        durationMs: 3000,
      },
    ]);
  });

  test("a session with no host, no query id and an unreadable time keeps only what it has", async () => {
    const row = [...SESSION_ROW];
    row[2] = null as unknown as string;
    row[6] = "";
    row[7] = "not a time";
    const { runner } = routed({ [databendSessionsSql(5)]: outcome(SESSION_SCHEMA, [row]) });
    const [session] = await getActiveSessions(runner, { limit: 5 });
    expect(session.state).toBe("Executing pipeline");
    expect("clientAddr" in session).toBe(false);
    expect("queryStart" in session).toBe(false);
  });

  test("the kill sends KILL QUERY with the session id as a literal", async () => {
    const pid = "5c00e52f-34c2-4cab-8ba8-c1f1121157bf";
    const { runner, calls } = routed({ [`KILL QUERY '${pid}'`]: outcome([], []) });
    await killSession(runner, pid);
    expect(calls).toEqual([{ sql: `KILL QUERY '${pid}'`, rowCut: DATABEND_SURFACE_ROW_CUT }]);
  });

  test("the kill id pattern: 1 to 64 letters, digits and hyphens; anything else sends nothing", async () => {
    const accepted = ["a", "A-9", "x".repeat(64), "01a11896a18d7ae399fb6947927521b2"];
    const sent = await Promise.all(accepted.map((id) => sentBy((runner) => killSession(runner, id))));
    expect(sent).toEqual(accepted.map((id) => [`KILL QUERY '${id}'`]));
    const refused = routed({});
    const refusals = ["x".repeat(65), "a'b", "a b", "a\\b", "a_b", "é"].map((id) =>
      killSession(refused.runner, id).catch((error: unknown) => (error as Error).message),
    );
    expect(await Promise.all(refusals)).toEqual(refusals.map(() => DATABEND_MONITORING_SENTENCES.killIdRefused));
    expect(refused.calls).toEqual([]);
    const empty = routed({});
    await expect(killSession(empty.runner, "")).rejects.toThrow(DATABEND_MONITORING_SENTENCES.killNeedsId);
    expect(empty.calls).toEqual([]);
  });

  test("a refused kill sends nothing, and a failed one propagates", async () => {
    const refused = routed({});
    await expect(killSession(refused.runner, "a'b")).rejects.toBeInstanceOf(QueryError);
    expect(refused.calls).toHaveLength(0);

    const denied = failure(1063);
    const { runner } = routed({ "KILL QUERY 'abc'": denied });
    await expect(killSession(runner, "abc")).rejects.toBe(denied);
  });
});

describe("each panel degrades to empty on the unavailable codes and propagates the rest", () => {
  test("the six codes", () => {
    expect([...DATABEND_DEGRADE_CODES]).toEqual([1003, 1025, 1063, 1112, 1119, 1002]);
  });

  for (const code of [1003, 1025, 1063, 1112, 1119, 1002]) {
    test(`code ${code}`, async () => {
      const runner: DatabendStatementRunner = async () => {
        throw failure(code);
      };
      expect(await getSlowQueries(runner)).toEqual([]);
      expect(await getActiveSessions(runner)).toEqual([]);
      expect(await getTableStats(runner)).toEqual([]);
      expect(await getIndexStats(runner)).toEqual([]);
      expect(await getStorageStats(runner)).toEqual([]);
      expect(await getOverview(runner)).toEqual({
        version: DATABEND_UNKNOWN_TEXT,
        uptime: DATABEND_UNKNOWN_TEXT,
        maxConnections: 0,
        databaseSize: DATABEND_UNAVAILABLE_TEXT,
        tableCount: 0,
        indexCount: 0,
      });
    });
  }

  test("any other error propagates", async () => {
    await Promise.all(
      [failure(1065), new DatabendError("timeout", "late"), new Error("boom")].map(async (error) => {
        const runner: DatabendStatementRunner = async () => {
          throw error;
        };
        await expect(getSlowQueries(runner)).rejects.toBe(error);
        await expect(getOverview(runner)).rejects.toBe(error);
      }),
    );
  });

  test("a cut table list is refused, not shown in part", async () => {
    const cut = { bound: "rows", limit: 100_000 } as const;
    const { runner } = routed({
      [tableStatsSql()]: outcome([["schema_name", "String"]], [["a"]], cut),
    });
    await expect(getTableStats(runner)).rejects.toThrow(DATABEND_OBJECT_SENTENCES.incomplete("table statistics", cut));
  });
});

describe("the panels", () => {
  test("the overview: version, the default catalog's sizes, active queries, and no uptime or ceiling", async () => {
    const { runner } = routed(OVERVIEW_ANSWERS);
    expect(await getOverview(runner)).toEqual({
      version: "8.0.26-v1.2.951-nightly",
      uptime: DATABEND_UNKNOWN_TEXT,
      activeConnections: 1,
      maxConnections: 0,
      databaseSize: "17.88 KB",
      databaseSizeBytes: 18304,
      tableCount: 2,
      indexCount: 4,
    });
  });

  test("an empty catalog sums to NULL, which is zero bytes", async () => {
    const { runner } = routed({
      ...OVERVIEW_ANSWERS,
      [OVERVIEW_TABLES_SQL]: outcome(
        [
          ["table_count", "UInt64"],
          ["compressed_bytes", "Nullable(UInt64)"],
          ["index_bytes", "Nullable(UInt64)"],
        ],
        [["0", null, null]],
      ),
    });
    const overview = await getOverview(runner);
    expect(overview.databaseSizeBytes).toBe(0);
    expect(overview.tableCount).toBe(0);
  });

  test("getPerformanceMetrics is {}: Databend publishes none of its fields", () => {
    expect(getPerformanceMetrics()).toEqual({});
  });

  test("slow queries: one execution per row", async () => {
    const { runner } = routed({
      [databendSlowQueriesSql(3)]: outcome(SLOW_SCHEMA, [["q1", "SELECT 2", "1500", "7"]]),
    });
    expect(await getSlowQueries(runner, { limit: 3 })).toEqual([
      { queryId: "q1", query: "SELECT 2", calls: 1, totalTime: 1500, avgTime: 1500, rows: 7 },
    ]);
  });

  test("table stats: compressed and index bytes, a NULL left out", async () => {
    const { runner } = routed({
      [tableStatsSql(" AND database = 'db'")]: outcome(
        [
          ["schema_name", "String"],
          ["table_name", "String"],
          ["num_rows", "Nullable(UInt64)"],
          ["data_compressed_size", "Nullable(UInt64)"],
          ["index_size", "Nullable(UInt64)"],
        ],
        [
          ["db", "t", "4", "2048", "1024"],
          ["db", "ext", null, null, null],
        ],
      ),
    });
    expect(await getTableStats(runner, { schema: "db" })).toEqual([
      {
        schemaName: "db",
        tableName: "t",
        rowCount: 4,
        tableSize: "2 KB",
        tableSizeBytes: 2048,
        indexSize: "1 KB",
        indexSizeBytes: 1024,
        totalSize: "3 KB",
        totalSizeBytes: 3072,
      },
      { schemaName: "db", tableName: "ext", rowCount: 0, totalSize: "0 B", totalSizeBytes: 0 },
    ]);
  });

  test("storage: one row per database", async () => {
    const { runner } = routed({
      [STORAGE_SQL]: outcome(
        [
          ["database_name", "String"],
          ["compressed_bytes", "Nullable(UInt64)"],
          ["index_bytes", "Nullable(UInt64)"],
        ],
        [
          ["a", "1024", "1024"],
          ["b", "10", null],
        ],
      ),
    });
    expect(await getStorageStats(runner)).toEqual([
      { name: "a", size: "2 KB", sizeBytes: 2048 },
      { name: "b", size: "10 B", sizeBytes: 10 },
    ]);
  });

  test("index stats: the definition's columns, no size or scan counters to read", async () => {
    const { runner } = routed({
      [indexStatsSql()]: outcome(
        [
          ["schema_name", "String"],
          ["table_name", "Nullable(String)"],
          ["index_name", "String"],
          ["index_type", "String"],
          ["definition", "String"],
        ],
        [["db", "t", "idx", "INVERTED", "t(a, b)tokenizer='english'"]],
      ),
    });
    expect(await getIndexStats(runner)).toEqual([
      {
        schemaName: "db",
        tableName: "t",
        indexName: "idx",
        indexType: "INVERTED",
        columns: ["a", "b"],
        isUnique: false,
        isPrimary: false,
        indexSize: DATABEND_UNAVAILABLE_TEXT,
        scans: 0,
      },
    ]);
  });

  test("health sends one read at a time, so no panel read counts a sibling as running work", async () => {
    const answers: Record<string, StatementOutcome> = {
      ...OVERVIEW_ANSWERS,
      [databendSlowQueriesSql(10)]: outcome(SLOW_SCHEMA, []),
      [databendSessionsSql(10)]: outcome(SESSION_SCHEMA, []),
    };
    let running = 0;
    let most = 0;
    const runner: DatabendStatementRunner = async (sql) => {
      running += 1;
      most = Math.max(most, running);
      await new Promise((resolve) => setTimeout(resolve, 1));
      running -= 1;
      return answers[sql];
    };
    await getHealth(runner);
    expect(most).toBe(1);
  });

  test("health: size, active queries, slow queries and sessions, no cache ratio", async () => {
    const { runner } = routed({
      ...OVERVIEW_ANSWERS,
      [databendSlowQueriesSql(10)]: outcome(SLOW_SCHEMA, [["q1", "SELECT 2", "1500", "7"]]),
      [databendSessionsSql(10)]: outcome(SESSION_SCHEMA, [SESSION_ROW]),
    });
    expect(await getHealth(runner)).toEqual({
      activeConnections: 1,
      databaseSize: "17.88 KB",
      cacheHitRatio: DATABEND_UNAVAILABLE_TEXT,
      slowQueries: [{ query: "SELECT 2", calls: 1, avgTime: "1.50s" }],
      activeSessions: [
        {
          pid: "5c00e52f-34c2-4cab-8ba8-c1f1121157bf",
          user: "studio_reader",
          database: "libredb_demo",
          state: DATABEND_MONITORING_SENTENCES.sessionState("Executing pipeline", "01a11896a18d7ae399fb6947927521b2"),
          query: "SELECT 1",
          duration: "3.00s",
        },
      ],
    });
  });
});

describe("the sentences", () => {
  test("each is exported, one sentence, no dash", () => {
    for (const sentence of [
      DATABEND_MONITORING_SENTENCES.killNeedsId,
      DATABEND_MONITORING_SENTENCES.killIdRefused,
      DATABEND_MONITORING_SENTENCES.killAsked("abc"),
    ]) {
      expect(sentence).toMatch(/^[A-Z].*\.$/);
      expect(sentence).not.toMatch(DASHES);
    }
    expect(DATABEND_MONITORING_SENTENCES.sessionState("Running", "q")).toBe("Running (query q)");
  });
});
