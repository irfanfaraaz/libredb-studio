/**
 * The Databend provider (design 2.3, 2.4, 3.10, 3.12, 6.4): the composition of the directory's modules, driven end to
 * end over a fake query server that stands where the shared node transport stands.
 *
 * The fake answers each statement POST for the ids the request carries (`wireIds`), as Databend answers our own
 * statement, so every request goes through the real HTTP transport, the real answer reader and the real decoder.
 * Time is injected (`transportDeps`): a deadline fires only when a test fires it, so no test waits on a real timer;
 * the only real wait is a zero-length yield while concurrent calls settle.
 *
 * Every provider a test builds is disconnected after it, so no permit of the process-wide `databend` limiter is
 * left held for the next test.
 */
import { afterEach, describe, expect, test } from "bun:test";
import { DatabaseConfigError, QueryCancelledError, QueryError, TimeoutError } from "@/lib/db/errors";
import {
  type NodeRequest,
  type NodeResponse,
  type NodeTransport,
  type NodeTransportOptions,
  TransportError,
} from "@/lib/db/http/node-transport";
import { DATABEND_PROVIDER_SENTENCES, DatabendProvider } from "@/lib/db/providers/sql/databend";
import { DATABEND_ANSWER_SENTENCES } from "@/lib/db/providers/sql/databend/answer";
import { DATABEND_ERROR_SENTENCES } from "@/lib/db/providers/sql/databend/errors";
import {
  DATABEND_ACTIVE_QUERIES_SQL,
  DATABEND_DEFAULT_SESSION_LIMIT,
  DATABEND_INDEX_COUNT_SQL,
  DATABEND_MONITORING_SENTENCES,
  DATABEND_OVERVIEW_TABLES_SQL,
  DATABEND_STORAGE_SQL,
  databendIndexStatsSql,
  databendKillSql,
  databendSessionsSql,
  databendSlowQueriesSql,
  databendTableStatsSql,
} from "@/lib/db/providers/sql/databend/introspect";
import { DATABEND_KILL_SPEC, DATABEND_LABELS } from "@/lib/db/providers/sql/databend/labels";
import {
  DATABEND_CATALOG_LIST_SQL,
  DATABEND_OBJECT_SENTENCES,
  DATABEND_VERSION_SQL,
  databendAuthTypeSql,
  databendColumnsSql,
  databendDatabaseListSql,
  databendObjectCountsSql,
  databendObjectListSql,
  databendSourceSql,
} from "@/lib/db/providers/sql/databend/objects";
import {
  globalSettingsChangedWarning,
  ROLE_NOT_CARRIED,
  SETTINGS_NOT_CARRIED,
  TEMP_TABLES_DROPPED,
  TRANSACTION_ENDED,
  TRANSACTION_MAY_STAY_OPEN,
  USE_NOT_CARRIED,
} from "@/lib/db/providers/sql/databend/session";
import { DATABEND_MULTIPLE_STATEMENTS } from "@/lib/db/providers/sql/databend/sql-text";
import { LimiterFullError } from "@/lib/db/utils/bounded-limiter";
import { MAX_UNLIMITED_ROWS } from "@/lib/db/utils/query-limiter";
import {
  answerBody,
  scriptedNodeTransport,
  TEST_USER,
  testConnection,
  transportDeps,
  wireIds,
} from "../../../helpers/databend-node-transport";

// ============================================================================
// A fake query server
// ============================================================================

/** What one answer of the fake carries beside the ids, or a reply of its own, or no answer until the abort. */
type Fields = Record<string, unknown>;
type Reply = Fields | { readonly reply: NodeResponse } | "hang";

interface Seen {
  readonly method: string;
  readonly path: string;
  readonly sql?: string;
  readonly queryId: string;
}

/** The shared transport's failure for an aborted signal, as `tests/helpers/databend-node-transport.ts` words it. */
function abortFailure(signal: AbortSignal): TransportError {
  const reason: unknown = signal.reason;
  return reason instanceof DOMException && reason.name === "TimeoutError"
    ? new TransportError("timeout", "The request did not finish within its time limit")
    : new TransportError("aborted", "The request was cancelled");
}

const EMPTY_OK: NodeResponse = { status: 200, contentType: "application/json", retryAfter: null, text: "{}" };

const column = (name: string, type = "String") => ({ name, type });

/** The answers a connect reads: the probe, the `auth_type` caution, and the default catalog's databases. */
function connectAnswer(sql: string): Reply | undefined {
  if (sql === DATABEND_VERSION_SQL) return { schema: [column("server_version")], data: [["v1.2.951-nightly"]] };
  if (sql === databendAuthTypeSql(TEST_USER)) return { schema: [column("auth_type")], data: [["sha256_password"]] };
  if (sql === databendDatabaseListSql("default")) {
    return { schema: [column("database_name")], data: [["default"], ["libredb_demo"]] };
  }
  return undefined;
}

interface FakeServer {
  readonly factory: (options: NodeTransportOptions) => NodeTransport;
  readonly requests: Seen[];
  /** What happened, in order: every request as `METHOD path`, and every hang an abort ended as `aborted path`. */
  readonly events: string[];
  /** Statement POSTs whose answer has not arrived yet, and the most there ever were at once. */
  inflight: number;
  maxInflight: number;
  built: number;
  sqls(): string[];
}

/**
 * `answer` decides each statement POST by its text (the connect reads answer as above unless it says otherwise);
 * `page` decides each page GET, which hangs by default. A ROLLBACK answers that the transaction ended; a kill, a final
 * and a logout answer 200 unless `close` says otherwise.
 */
function fakeDatabend({
  answer = () => ({}),
  page = () => "hang",
  close = () => EMPTY_OK,
  rollback = { txn_state: "AutoCommit" },
}: {
  readonly answer?: (sql: string) => Reply | Promise<Reply>;
  readonly page?: (path: string, queryId: string) => Reply | Promise<Reply>;
  readonly close?: (path: string) => NodeResponse;
  /** The session fields a ROLLBACK answers with. */
  readonly rollback?: Fields;
} = {}): FakeServer {
  const server: FakeServer = {
    factory: (options) => {
      server.built += 1;
      return {
        async request(request: NodeRequest): Promise<NodeResponse> {
          if (request.signal.aborted) throw abortFailure(request.signal);
          const path = new URL(request.url).pathname;
          const headers = { ...options.headers, ...request.headers };
          const sql = request.body === undefined ? undefined : (JSON.parse(request.body) as { sql?: string }).sql;
          const ids = wireIds(headers);
          server.requests.push({
            method: request.method,
            path,
            ...(sql === undefined ? {} : { sql }),
            queryId: ids.queryId,
          });
          server.events.push(`${request.method} ${path}`);
          const queryId = path.split("/")[3] ?? ids.queryId;
          let decided: Reply | Promise<Reply>;
          if (request.method === "POST" && path === "/v1/query" && sql === "ROLLBACK") {
            decided = { session: { ...(answerBody({}).session as Fields), ...rollback } };
          } else if (request.method === "POST" && path === "/v1/query") {
            decided = answer(sql as string);
          } else if (path.includes("/page/")) {
            decided = page(path, queryId);
          } else {
            return close(path);
          }
          const statement = request.method === "POST";
          if (statement) {
            server.inflight += 1;
            server.maxInflight = Math.max(server.maxInflight, server.inflight);
          }
          let abort = () => {};
          const aborted = new Promise<never>((_resolve, reject) => {
            abort = () => {
              server.events.push(`aborted ${path}`);
              reject(abortFailure(request.signal));
            };
            request.signal.addEventListener("abort", abort, { once: true });
          });
          // A request that is answered never reads the abort's rejection.
          aborted.catch(() => {});
          try {
            const reply = await Promise.race([Promise.resolve(decided), aborted]);
            if (reply === "hang") return await aborted;
            // `Fields` is any record, so the narrowing cannot tell a reply of its own from fields named `reply`.
            if ("reply" in reply) return (reply as { readonly reply: NodeResponse }).reply;
            const body = answerBody({ id: queryId, session_id: ids.sessionId, ...reply });
            // The placeholders `PAGE` and `FINAL` stand for this statement's own links.
            if (body.next_uri === "PAGE") body.next_uri = `/v1/query/${queryId}/page/1`;
            if (body.next_uri === "FINAL") body.next_uri = `/v1/query/${queryId}/final`;
            return { status: 200, contentType: "application/json", retryAfter: null, text: JSON.stringify(body) };
          } finally {
            request.signal.removeEventListener("abort", abort);
            if (statement) server.inflight -= 1;
          }
        },
        close() {},
      };
    },
    requests: [],
    events: [],
    inflight: 0,
    maxInflight: 0,
    built: 0,
    sqls: () => server.requests.flatMap((request) => (request.sql === undefined ? [] : [request.sql])),
  };
  return server;
}

/** A fake whose statements answer `answers` first and the connect reads otherwise. */
function withConnect(answers: (sql: string) => Reply | Promise<Reply> | undefined = () => undefined) {
  return (sql: string): Reply | Promise<Reply> => answers(sql) ?? connectAnswer(sql) ?? {};
}

const opened: DatabendProvider[] = [];

afterEach(async () => {
  await Promise.allSettled(opened.splice(0).map((provider) => provider.disconnect()));
});

/** A provider over `fake` with injected time; `overrides` go onto the loopback test connection. */
function build(fake: FakeServer, overrides: Record<string, unknown> = {}, queryTimeout = 60_000) {
  const time = transportDeps(scriptedNodeTransport());
  const provider = new DatabendProvider(
    testConnection(overrides),
    { queryTimeout },
    { ...time.deps, createNodeTransport: fake.factory },
  );
  opened.push(provider);
  return { provider, time };
}

/** Yields to the event loop until `ready()` holds; a real zero-length wait, never a deadline. */
async function until(ready: () => boolean): Promise<void> {
  for (let turn = 0; turn < 500 && !ready(); turn += 1) {
    // oxlint-disable-next-line no-await-in-loop -- each turn yields once, then reads the condition again.
    await new Promise((resolve) => setTimeout(resolve, 0));
  }
  expect(ready()).toBe(true);
}

/** A promise a test settles later. */
function gate<T = Reply>() {
  let open: (value: T) => void = () => {};
  const promise = new Promise<T>((resolve) => {
    open = resolve;
  });
  return { promise, open };
}

/** The 200 answer of an in-body statement error. */
function failed(code: number, message: string): Fields {
  return { state: "Failed", error: { code, message, detail: null }, schema: [], data: [] };
}

// ============================================================================
// Construction and declarations (design 2.3, 2.4)
// ============================================================================

describe("construction and declarations", () => {
  test("the constructor never dials or throws; the connection's refusals come from connect(), before any socket", async () => {
    const fake = fakeDatabend();
    const { provider } = build(fake, { user: undefined, warehouse: "not a warehouse!" });
    expect(provider.isConnected()).toBe(false);
    expect(provider.getCapabilities().defaultPort).toBe(8000);
    const failure = await provider.connect().catch((error: unknown) => error);
    expect(failure).toBeInstanceOf(DatabaseConfigError);
    expect((failure as Error).message).toContain("User is required");
    expect(fake.built).toBe(0);
    expect(fake.requests).toEqual([]);
  });

  test("getCapabilities() is the literal of design 2.4", () => {
    const { provider } = build(fakeDatabend());
    expect(provider.getCapabilities()).toEqual({
      queryLanguage: "sql",
      supportsExplain: true,
      explainFormat: "databend-text",
      supportsExternalQueryLimiting: true,
      supportsResultPagination: true,
      supportsCreateTable: false,
      supportsInlineRowEdit: false,
      supportsTestDataGeneration: false,
      supportsTransactions: false,
      declaresForeignKeys: false,
      supportsMaintenance: true,
      maintenanceOperations: ["kill"],
      maintenanceOperationSpecs: { kill: DATABEND_KILL_SPEC },
      supportsConnectionString: false,
      defaultPort: 8000,
      identifierQuoting: "backtick-always",
      schemaRefreshPattern: "^\\s*(CREATE|DROP|ALTER|RENAME|UNDROP|TRUNCATE|REPLACE)\\b",
      containerLevels: [
        { id: "catalog", label: "Catalog", labelPlural: "Catalogs" },
        { id: "schema", label: "Database", labelPlural: "Databases" },
      ],
      containerPathShapes: "exact",
      objectKinds: [
        {
          id: "table",
          role: "relation",
          label: "Table",
          labelPlural: "Tables",
          hasSource: true,
          sourceLanguage: "sql",
          hasColumns: true,
        },
        {
          id: "view",
          role: "relation",
          label: "View",
          labelPlural: "Views",
          hasSource: true,
          sourceLanguage: "sql",
          hasColumns: true,
        },
        {
          id: "materialized_view",
          role: "relation",
          label: "Materialized View",
          labelPlural: "Materialized Views",
          hasSource: true,
          sourceLanguage: "sql",
          hasColumns: true,
        },
        {
          id: "dynamic_table",
          role: "relation",
          label: "Dynamic Table",
          labelPlural: "Dynamic Tables",
          hasSource: true,
          sourceLanguage: "sql",
          hasColumns: true,
        },
      ],
    });
    expect(provider.getCapabilities()).not.toHaveProperty("statementTerminator");
    expect(provider.getCapabilities()).not.toHaveProperty("resumesBilledCompute");
  });

  test("the schema refresh pattern matches a leading DDL word only", () => {
    const pattern = new RegExp(build(fakeDatabend()).provider.getCapabilities().schemaRefreshPattern, "i");
    for (const sql of [
      "CREATE TABLE t (a INT)",
      "  undrop table t",
      "RENAME TABLE a TO b",
      "replace into t values (1)",
    ]) {
      expect(pattern.test(sql)).toBe(true);
    }
    for (const sql of ["SELECT 'CREATE'", "INSERT INTO t SELECT * FROM created"]) expect(pattern.test(sql)).toBe(false);
  });

  test("resumesBilledCompute is declared exactly when Warehouse is set, read before any connect", () => {
    expect(build(fakeDatabend(), { warehouse: "" }).provider.getCapabilities().resumesBilledCompute).toBeUndefined();
    expect(build(fakeDatabend(), { warehouse: null }).provider.getCapabilities().resumesBilledCompute).toBeUndefined();
    expect(build(fakeDatabend(), { warehouse: "wh-1" }).provider.getCapabilities().resumesBilledCompute).toBe(true);
  });

  test("getLabels() answers a copy of the Databend labels", () => {
    const labels = build(fakeDatabend()).provider.getLabels();
    expect(labels).toEqual(DATABEND_LABELS);
    expect(labels).not.toBe(DATABEND_LABELS);
  });

  test("every call that needs a connection refuses before connect, sending nothing", async () => {
    const fake = fakeDatabend();
    const { provider } = build(fake);
    await expect(provider.query("SELECT 1")).rejects.toThrow();
    await expect(provider.listContainers()).rejects.toThrow();
    await expect(provider.getPerformanceMetrics()).rejects.toThrow();
    expect(provider.connectWarnings()).toEqual([]);
    expect(await provider.cancelQuery("q-1")).toBe(false);
    await provider.disconnect();
    expect(fake.built).toBe(0);
  });
});

// ============================================================================
// connect() (design 5.4, 6.4) [X07] [X12]
// ============================================================================

describe("connect", () => {
  test("runs the version probe, then the best-effort cautions, each under the surface deadline", async () => {
    const fake = fakeDatabend({ answer: withConnect() });
    const { provider, time } = build(fake, { database: "libredb_demo" });
    await provider.connect();
    expect(provider.isConnected()).toBe(true);
    expect(fake.sqls()).toEqual([
      DATABEND_VERSION_SQL,
      databendDatabaseListSql("default"),
      databendAuthTypeSql(TEST_USER),
    ]);
    expect(time.deadlines.filter((deadline) => deadline.ms === 10_000)).toHaveLength(3);
    expect(provider.connectWarnings()).toEqual([]);
  });

  test("names a database missing from the default catalog, never one of the two it generates", async () => {
    const missing = build(fakeDatabend({ answer: withConnect() }), { database: "nope" }).provider;
    await missing.connect();
    expect(missing.connectWarnings()).toEqual([{ message: DATABEND_PROVIDER_SENTENCES.databaseMissing("nope") }]);

    const generated = build(fakeDatabend({ answer: withConnect() }), { database: "system" }).provider;
    await generated.connect();
    expect(generated.connectWarnings()).toEqual([]);
  });

  test("a database read that fails is no caution and no failed connect", async () => {
    const answers = withConnect((sql) =>
      sql === databendDatabaseListSql("default") ? failed(1063, "denied") : undefined,
    );
    const { provider } = build(fakeDatabend({ answer: answers }), { database: "libredb_demo" });
    await provider.connect();
    expect(provider.connectWarnings()).toEqual([]);
  });

  test("names a no_password user from the auth_type read, and omits it when that read fails [X12]", async () => {
    const noPassword = withConnect((sql) =>
      sql === databendAuthTypeSql(TEST_USER) ? { schema: [column("auth_type")], data: [["no_password"]] } : undefined,
    );
    const named = build(fakeDatabend({ answer: noPassword })).provider;
    await named.connect();
    expect(named.connectWarnings()).toEqual([{ message: DATABEND_OBJECT_SENTENCES.noPassword(TEST_USER) }]);

    const refused = withConnect((sql) =>
      sql === databendAuthTypeSql(TEST_USER) ? failed(1063, "Permission denied") : undefined,
    );
    const quiet = build(fakeDatabend({ answer: refused })).provider;
    await quiet.connect();
    expect(quiet.isConnected()).toBe(true);
    expect(quiet.connectWarnings()).toEqual([]);
  });

  test("an unverified TLS certificate with a password is a caution; without a password it is none", async () => {
    const withPassword = build(fakeDatabend({ answer: withConnect() }), { ssl: { mode: "require" } }).provider;
    await withPassword.connect();
    expect(withPassword.connectWarnings()).toEqual([{ message: DATABEND_PROVIDER_SENTENCES.unverifiedTls }]);

    const without = build(fakeDatabend({ answer: withConnect() }), {
      ssl: { mode: "require" },
      password: undefined,
    }).provider;
    await without.connect();
    expect(without.connectWarnings()).toEqual([]);
  });

  test("a refused probe is the house class, and the provider stays unconnected", async () => {
    const refusal: NodeResponse = {
      status: 401,
      contentType: "application/json",
      retryAfter: null,
      text: JSON.stringify({ error: { code: 5100, message: "wrong password" } }),
    };
    const fake = fakeDatabend({ answer: () => ({ reply: refusal }) });
    const { provider } = build(fake);
    const failure = await provider.connect().catch((error: unknown) => error);
    expect((failure as Error).name).toBe("AuthenticationError");
    expect((failure as Error).message).toContain(DATABEND_ERROR_SENTENCES.signInRefused);
    expect(provider.isConnected()).toBe(false);
    expect(fake.sqls()).toEqual([DATABEND_VERSION_SQL]);
  });

  test("with Warehouse set, a probe outlasting Test Connection's 10 s gives the resuming sentence [X07]", async () => {
    const fake = fakeDatabend({ answer: () => "hang" });
    const { provider, time } = build(fake, { warehouse: "wh-1" }, 10_000);
    const connecting = provider.connect().catch((error: unknown) => error);
    await until(() => fake.requests.length === 1);
    time.fire(10_000);
    const failure = await connecting;
    expect(failure).toBeInstanceOf(TimeoutError);
    expect((failure as Error).message).toBe(DATABEND_ERROR_SENTENCES.resuming("wh-1", "10"));
    expect(provider.isConnected()).toBe(false);
  });

  test("a second connect replaces the first session and closes it", async () => {
    const fake = fakeDatabend({ answer: withConnect() });
    const { provider } = build(fake);
    await provider.connect();
    await provider.connect();
    expect(fake.built).toBe(2);
    expect(provider.isConnected()).toBe(true);
  });
});

// ============================================================================
// query() (design 2.3, 3.7, section 4)
// ============================================================================

describe("query", () => {
  async function connected(answers?: (sql: string) => Reply | Promise<Reply> | undefined, overrides = {}) {
    const fake = fakeDatabend({ answer: withConnect(answers) });
    const built = build(fake, overrides);
    await built.provider.connect();
    return { fake, ...built, sent: () => fake.requests.length };
  }

  test("the statement guard refuses before any socket", async () => {
    const { provider, sent } = await connected();
    const before = sent();
    const failure = await provider.query("SELECT 1; SELECT 2").catch((error: unknown) => error);
    expect(failure).toBeInstanceOf(QueryError);
    expect((failure as Error).message).toBe(DATABEND_MULTIPLE_STATEMENTS);
    expect(sent()).toBe(before);
  });

  test("bound parameters are refused before any socket; an empty list binds nothing", async () => {
    const { provider, sent } = await connected();
    const before = sent();
    await expect(provider.query("SELECT ?", [1])).rejects.toThrow(DATABEND_PROVIDER_SENTENCES.params);
    expect(sent()).toBe(before);
    expect((await provider.query("SELECT 1", [])).rowCount).toBe(0);
  });

  test("decodes rows by the declared types, keeping them in columnTypes", async () => {
    const { provider } = await connected((sql) =>
      sql === "SELECT n, b FROM t"
        ? {
            schema: [column("n", "Nullable(Int32)"), column("b", "Boolean")],
            data: [
              ["1", "1"],
              [null, "0"],
            ],
          }
        : undefined,
    );
    const result = await provider.query("SELECT n, b FROM t");
    expect(result.fields).toEqual(["n", "b"]);
    expect(result.rows).toEqual([
      { n: 1, b: true },
      { n: null, b: false },
    ]);
    expect(result.rowCount).toBe(2);
    expect(result.columnTypes).toEqual({ n: "Nullable(Int32)", b: "Boolean" });
    expect(result).not.toHaveProperty("warnings");
    expect(result).not.toHaveProperty("pagination");
    expect(typeof result.executionTime).toBe("number");
  });

  test("a DML count row is kept and its count is the rowCount, as trino/index.ts reports it", async () => {
    const { provider } = await connected(() => ({
      schema: [column("number of rows inserted", "UInt64")],
      data: [["3"]],
    }));
    const result = await provider.query("INSERT INTO t VALUES (1), (2), (3)");
    expect(result.rows).toEqual([{ "number of rows inserted": 3 }]);
    expect(result.rowCount).toBe(3);
  });

  test("a statement with no result set answers no field, no row and no column type", async () => {
    const { provider } = await connected(() => ({ has_result_set: false }));
    const result = await provider.query("CREATE TABLE t (a INT)");
    expect(result.fields).toEqual([]);
    expect(result.rows).toEqual([]);
    expect(result.rowCount).toBe(0);
    expect(result).not.toHaveProperty("columnTypes");
  });

  const session = (fields: Fields) => ({ session: { ...(answerBody({}).session as Fields), ...fields } });

  test.each([
    ["USE", { affect: { type: "UseDB", name: "other" } }, [USE_NOT_CARRIED]],
    [
      "SET and SET GLOBAL",
      { affect: { type: "ChangeSettings", keys: ["a", "b"], values: ["1", "2"], is_globals: [false, true] } },
      [SETTINGS_NOT_CARRIED, globalSettingsChangedWarning(["b"])],
    ],
    ["SET ROLE", session({ role: "public" }), [ROLE_NOT_CARRIED]],
    ["BEGIN, rolled back", session({ txn_state: "Active" }), [TRANSACTION_ENDED]],
    ["a temporary table", session({ need_keep_alive: true }), [TEMP_TABLES_DROPPED]],
    ["a server warning", { warnings: ["setting x is ignored"] }, ["setting x is ignored"]],
    [
      "another result mode",
      session({ settings: { http_json_result_mode: "json" } }),
      [DATABEND_ANSWER_SENTENCES.resultMode("json")],
    ],
  ])("%s becomes the session.ts or answer.ts sentence as a warning", async (_name, fields, messages) => {
    const { provider } = await connected((sql) => (sql === "SELECT 1" ? fields : undefined));
    const result = await provider.query("SELECT 1");
    expect(result.warnings).toEqual(messages.map((message) => ({ message })));
  });

  test("a ROLLBACK that does not end the transaction, and a logout that fails, are warnings too", async () => {
    const fake = fakeDatabend({
      answer: withConnect((sql) =>
        sql === "SELECT 1" ? session({ txn_state: "Active", need_keep_alive: true }) : undefined,
      ),
      rollback: { txn_state: "Active", need_keep_alive: true },
      close: (path) => (path === "/v1/session/logout" ? { ...EMPTY_OK, status: 404 } : EMPTY_OK),
    });
    const { provider } = build(fake);
    await provider.connect();
    const result = await provider.query("SELECT 1");
    expect(result.warnings).toEqual([
      { message: TRANSACTION_MAY_STAY_OPEN },
      { message: DATABEND_PROVIDER_SENTENCES.closeFailed("logout") },
    ]);
  });

  test("a result cut at the statement budget is marked on pagination and warned about", async () => {
    const width = 5;
    const rows = Array.from({ length: 50_001 }, () => Array.from({ length: width }, () => "1"));
    const { provider } = await connected((sql) =>
      sql === "SELECT * FROM wide"
        ? { schema: Array.from({ length: width }, (_, index) => column(`c${index}`, "Int32")), data: rows }
        : undefined,
    );
    const result = await provider.query("SELECT * FROM wide");
    expect(result.rows).toHaveLength(50_000);
    expect(result.pagination).toEqual({
      limit: MAX_UNLIMITED_ROWS,
      offset: 0,
      hasMore: false,
      totalReturned: 50_000,
      wasLimited: true,
    });
    expect(result.warnings).toEqual([
      { message: DATABEND_PROVIDER_SENTENCES.resultCut({ bound: "cells", limit: 250_000 }) },
    ]);
  });

  test("an in-body statement error is a QueryError carrying the statement", async () => {
    const { provider } = await connected((sql) =>
      sql === "SELECT nope" ? failed(1065, "error: no column nope\n--> SQL:1:8") : undefined,
    );
    const failure = (await provider.query("SELECT nope").catch((error: unknown) => error)) as QueryError;
    expect(failure).toBeInstanceOf(QueryError);
    expect(failure.message).toContain("no column nope");
    expect(failure.query).toBe("SELECT nope");
    expect(failure.position).toBe(8);
  });

  test("cancelQuery answers false for an unknown id and sends nothing", async () => {
    const { provider, sent } = await connected();
    const before = sent();
    expect(await provider.cancelQuery("q-unknown")).toBe(false);
    expect(sent()).toBe(before);
  });

  test("cancelQuery stops a running statement: its kill is sent and the run is cancelled", async () => {
    const { provider, fake } = await connected((sql) =>
      sql === "SELECT sleep(9)" ? { state: "Running", next_uri: "PAGE" } : undefined,
    );
    const running = provider.query("SELECT sleep(9)", undefined, "q-1").catch((error: unknown) => error);
    await until(() => pagesOpened(fake) === 1);
    expect(await provider.cancelQuery("q-1")).toBe(true);
    expect(await running).toBeInstanceOf(QueryCancelledError);
    expect(fake.events.some((event) => /^GET \/v1\/query\/\w+\/kill$/.test(event))).toBe(true);
  });
});

/** The page GETs the fake has received. */
function pagesOpened(fake: FakeServer): number {
  return fake.requests.filter((request) => request.path.includes("/page/")).length;
}

/** The index in `fake.requests` of the first request `matches` accepts, or -1. */
function requestAt(fake: FakeServer, matches: (request: Seen) => boolean): number {
  return fake.requests.findIndex(matches);
}

/** A few turns of the event loop, for a call that must NOT have sent anything yet. */
async function settle(): Promise<void> {
  for (let turn = 0; turn < 20; turn += 1) {
    // oxlint-disable-next-line no-await-in-loop -- the turns run one after another, which is the point.
    await new Promise((resolve) => setTimeout(resolve, 0));
  }
}

// ============================================================================
// The limiter (design 2.3, 3.12) [X04] [X16]
// ============================================================================

describe("the limiter over every statement", () => {
  test("eight concurrent surface calls on two instances never have more than two statements in flight [X04]", async () => {
    const held: Array<() => void> = [];
    const answers = withConnect((sql) => {
      if (sql !== DATABEND_CATALOG_LIST_SQL) return undefined;
      const wait = gate();
      held.push(() => wait.open({ schema: [column("catalog_name")], data: [["default"]] }));
      return wait.promise;
    });
    const fake = fakeDatabend({ answer: answers });
    const first = build(fake).provider;
    const second = build(fake).provider;
    await first.connect();
    await second.connect();
    fake.maxInflight = 0;

    const calls = [first, second].flatMap((provider) => [1, 2, 3, 4].map(() => provider.listContainers()));
    for (let released = 0; released < 8; released += 1) {
      // oxlint-disable-next-line no-await-in-loop -- one statement is released at a time, after the bound is read.
      await until(() => held.length > 0);
      expect(fake.inflight).toBeLessThanOrEqual(2);
      (held.shift() as () => void)();
    }
    const answered = await Promise.all(calls);
    expect(answered.every((containers) => containers[0].name === "default")).toBe(true);
    expect(fake.maxInflight).toBe(2);
  });

  test("a final, a ROLLBACK, a logout and a kill take no permit while both are held and calls wait [X04]", async () => {
    const open = { ...(answerBody({}).session as Fields), txn_state: "Active", need_keep_alive: true };
    const pages = new Map<string, (reply: Reply) => void>();
    const fake = fakeDatabend({
      answer: withConnect((sql) =>
        sql.startsWith("SELECT sleep") ? { state: "Running", next_uri: "PAGE", session: open } : undefined,
      ),
      page: (_path, queryId) => {
        const wait = gate();
        pages.set(queryId, wait.open);
        return wait.promise;
      },
      rollback: { txn_state: "AutoCommit", need_keep_alive: true },
    });
    const { provider } = build(fake);
    await provider.connect();
    const one = provider.query("SELECT sleep(1)", undefined, "q-1").catch((error: unknown) => error);
    const two = provider.query("SELECT sleep(2)", undefined, "q-2").catch((error: unknown) => error);
    await until(() => pages.size === 2);
    const three = provider.query("SELECT sleep(3)", undefined, "q-3").catch((error: unknown) => error);
    await settle();
    expect(fake.sqls()).not.toContain("SELECT sleep(3)");

    // The first statement fails on its page: the server ended it, so its final, ROLLBACK and logout go out before
    // the waiting statement gets the permit it frees.
    const [firstId, secondId] = [...pages.keys()];
    (pages.get(firstId) as (reply: Reply) => void)({
      ...failed(1006, "division by zero"),
      next_uri: "FINAL",
      session: open,
    });
    expect(await one).toBeInstanceOf(QueryError);
    await until(() => fake.sqls().includes("SELECT sleep(3)"));
    const third = requestAt(fake, (request) => request.sql === "SELECT sleep(3)");
    const final = requestAt(fake, (request) => request.path === `/v1/query/${firstId}/final`);
    const rollback = requestAt(fake, (request) => request.sql === "ROLLBACK");
    const logout = requestAt(fake, (request) => request.path === "/v1/session/logout");
    expect(final).toBeGreaterThan(-1);
    expect(rollback).toBeGreaterThan(final);
    expect(logout).toBeGreaterThan(rollback);
    expect(third).toBeGreaterThan(logout);

    // Two and three hold the permits and four waits: the cancel's kill still goes out before four is sent.
    const four = provider.query("SELECT sleep(4)", undefined, "q-4").catch((error: unknown) => error);
    await settle();
    expect(await provider.cancelQuery("q-2")).toBe(true);
    expect(await two).toBeInstanceOf(QueryCancelledError);
    await until(() => fake.sqls().includes("SELECT sleep(4)"));
    const kill = requestAt(fake, (request) => request.path === `/v1/query/${secondId}/kill`);
    expect(kill).toBeGreaterThan(third);
    expect(requestAt(fake, (request) => request.sql === "SELECT sleep(4)")).toBeGreaterThan(kill);
    await provider.disconnect();
    expect(await three).toBeInstanceOf(QueryCancelledError);
    expect(await four).toBeInstanceOf(QueryCancelledError);
  });

  test("a full queue refuses at once and sends nothing [X04]", async () => {
    const fake = fakeDatabend({ answer: withConnect((sql) => (sql === "SELECT 1" ? "hang" : undefined)) });
    const { provider } = build(fake);
    await provider.connect();
    const holding = [provider.query("SELECT 1"), provider.query("SELECT 1")].map((run) =>
      run.catch((error: unknown) => error),
    );
    await until(() => fake.inflight === 2);
    const queued = Array.from({ length: 64 }, () => provider.query("SELECT 2").catch((error: unknown) => error));
    const before = fake.requests.length;
    const refused = await provider.query("SELECT 3").catch((error: unknown) => error);
    expect(refused).toBeInstanceOf(LimiterFullError);
    expect((refused as Error).message).toContain("64 calls waiting for its 2 in-flight slots");
    expect(fake.requests.length).toBe(before);

    await provider.disconnect();
    for (const outcome of await Promise.all(queued)) expect(outcome).toBeInstanceOf(QueryCancelledError);
    await Promise.all(holding);
    expect(fake.sqls()).not.toContain("SELECT 2");
  });

  test("a statement queued behind two permits expires on the injected deadline, unsent [X16]", async () => {
    const fake = fakeDatabend({
      answer: withConnect((sql) => (sql === DATABEND_CATALOG_LIST_SQL ? "hang" : undefined)),
    });
    const { provider, time } = build(fake);
    await provider.connect();
    const holding = [provider.listContainers(), provider.listContainers()].map((call) =>
      call.catch((error: unknown) => error),
    );
    await until(() => fake.inflight === 2);
    const queued = provider.query("SELECT 1").catch((error: unknown) => error);
    await until(() => time.deadlines.some((deadline) => deadline.ms === 60_000));
    time.fire(60_000);
    const failure = await queued;
    expect(failure).toBeInstanceOf(TimeoutError);
    expect((failure as Error).message).toBe(DATABEND_PROVIDER_SENTENCES.slotsBusy("60"));
    expect(fake.sqls()).not.toContain("SELECT 1");
    await provider.disconnect();
    await Promise.all(holding);
  });

  test("with Warehouse set, a surface read queued past its deadline is not the resuming sentence [X07] [X16]", async () => {
    const fake = fakeDatabend({
      answer: withConnect((sql) => (sql === DATABEND_CATALOG_LIST_SQL ? "hang" : undefined)),
    });
    const { provider, time } = build(fake, { warehouse: "wh-1" });
    await provider.connect();
    const holding = [provider.listContainers(), provider.listContainers()].map((call) =>
      call.catch((error: unknown) => error),
    );
    await until(() => fake.inflight === 2);
    const budgets = time.deadlines.length;
    const queued = provider.listContainers().catch((error: unknown) => error);
    await until(() => time.deadlines.length === budgets + 1);
    // The holding reads were sent, so theirs is the resuming sentence; the queued one sent nothing.
    time.fire(10_000);
    const failure = await queued;
    expect(failure).toBeInstanceOf(TimeoutError);
    expect((failure as Error).message).toBe(DATABEND_PROVIDER_SENTENCES.slotsBusy("10"));
    // The two sent reads are killed; the queued third never posts.
    expect(fake.sqls().filter((sql) => sql === DATABEND_CATALOG_LIST_SQL)).toHaveLength(2);
    for (const held of await Promise.all(holding)) {
      expect((held as Error).message).toBe(DATABEND_ERROR_SENTENCES.resuming("wh-1", "10"));
    }
  });
});

// ============================================================================
// Statement budgets on the object surface, end to end [X05]
// ============================================================================

describe("a statement budget cut on the object surface [X05]", () => {
  const COLUMN_SCHEMA = [
    "object_name",
    "column_name",
    "data_type",
    "is_nullable",
    "default_kind",
    "default_expression",
  ];
  const CUT = { bound: "cells" as const, limit: 250_000 };

  test("a cut inside a table drops that table and says so", async () => {
    const container = { catalog: "default", database: "libredb_demo" };
    const fake = fakeDatabend({
      answer: withConnect((sql) => {
        if (sql.startsWith("SELECT name AS object_name")) {
          return { schema: [column("object_name")], data: [["t1"], ["t2"]] };
        }
        if (!sql.startsWith("SELECT `table` AS object_name")) return undefined;
        const rows = [
          ["t1", "a", "Int32", "NO", "", ""],
          ["t1", "b", "String", "YES", "", ""],
          ...Array.from({ length: 41_665 }, (_, index) => ["t2", `c${index}`, "Int32", "NO", "", ""]),
        ];
        return { schema: COLUMN_SCHEMA.map((name) => column(name)), data: rows };
      }),
    });
    const { provider } = build(fake);
    await provider.connect();
    const batch = await provider.describeObjects([container.catalog, container.database], "table");
    expect(batch.details.map((detail) => detail.path)).toEqual([["default", "libredb_demo", "t1"]]);
    expect(batch.details[0].columns.map((columnOf) => columnOf.name)).toEqual(["a", "b"]);
    expect(batch.truncated).toEqual({ limit: 1, reason: DATABEND_OBJECT_SENTENCES.bulkCut(CUT) });
  });

  test("an over-budget describe refuses, naming the bound", async () => {
    const container = { catalog: "default", database: "libredb_demo" };
    const sql = databendColumnsSql(container, "table", "wide");
    const fake = fakeDatabend({
      answer: withConnect((sent) =>
        sent === sql
          ? {
              schema: COLUMN_SCHEMA.slice(1)
                .concat("comment")
                .map((name) => column(name)),
              data: Array.from({ length: 41_667 }, (_, index) => [`c${index}`, "Int32", "NO", "", "", ""]),
            }
          : undefined,
      ),
    });
    const { provider } = build(fake);
    await provider.connect();
    const failure = await provider
      .describeObject(["default", "libredb_demo", "wide"], "table")
      .catch((error: unknown) => error);
    expect(failure).toBeInstanceOf(QueryError);
    expect((failure as Error).message).toBe(DATABEND_OBJECT_SENTENCES.incomplete("column list", CUT));
  });
});

// ============================================================================
// close() [X31]
// ============================================================================

describe("disconnect", () => {
  test("aborts both open long polls, then sends each kill under its own 5 s [X31]", async () => {
    const fake = fakeDatabend({
      answer: withConnect((sql) =>
        sql.startsWith("SELECT sleep") ? { state: "Running", next_uri: "PAGE" } : undefined,
      ),
    });
    const { provider, time } = build(fake);
    await provider.connect();
    const runs = [
      provider.query("SELECT sleep(1)").catch((error: unknown) => error),
      provider.query("SELECT sleep(2)").catch((error: unknown) => error),
    ];
    await until(() => pagesOpened(fake) === 2);
    const closesBefore = time.deadlines.filter((deadline) => deadline.ms === 5_000).length;
    await provider.disconnect();
    expect(provider.isConnected()).toBe(false);
    for (const outcome of await Promise.all(runs)) expect(outcome).toBeInstanceOf(QueryCancelledError);

    const aborts = fake.events.flatMap((event, index) => (event.startsWith("aborted ") ? [index] : []));
    const kills = fake.events.flatMap((event, index) => (/^GET \/v1\/query\/\w+\/kill$/.test(event) ? [index] : []));
    expect(aborts).toHaveLength(2);
    expect(kills).toHaveLength(2);
    expect(Math.max(...aborts)).toBeLessThan(Math.min(...kills));
    const closeBudgets = time.deadlines.filter((deadline) => deadline.ms === 5_000);
    expect(closeBudgets.length - closesBefore).toBe(2);
    expect(closeBudgets.every((deadline) => !deadline.signal.aborted)).toBe(true);
  });
});

// ============================================================================
// Delegation: the object surface and monitoring (design 5.4, 5.5)
// ============================================================================

describe("the object surface and monitoring delegate to objects.ts and introspect.ts", () => {
  const container = { catalog: "default", database: "libredb_demo" };

  async function connected(answers?: (sql: string) => Reply | Promise<Reply> | undefined, overrides = {}) {
    const fake = fakeDatabend({ answer: withConnect(answers) });
    const { provider } = build(fake, overrides);
    await provider.connect();
    const before = fake.sqls().length;
    return { provider, fake, sent: () => fake.sqls().slice(before) };
  }

  test.each([
    ["listContainers()", (p: DatabendProvider) => p.listContainers(), [DATABEND_CATALOG_LIST_SQL]],
    [
      "listContainers([catalog])",
      (p: DatabendProvider) => p.listContainers(["default"]),
      [databendDatabaseListSql("default")],
    ],
    ["listContainers([catalog, database])", (p: DatabendProvider) => p.listContainers(["default", "x"]), []],
    [
      "countObjects",
      (p: DatabendProvider) => p.countObjects(["default", "libredb_demo"]),
      [databendObjectCountsSql(container)],
    ],
    [
      "listObjects",
      (p: DatabendProvider) => p.listObjects(["default", "libredb_demo"], "view"),
      [databendObjectListSql(container, "view")],
    ],
    [
      "readObjectSource",
      (p: DatabendProvider) => p.readObjectSource(["default", "libredb_demo", "t"], "materialized_view"),
      [databendSourceSql(container, "materialized_view", "t")],
    ],
    [
      "getOverview",
      (p: DatabendProvider) => p.getOverview(),
      [DATABEND_VERSION_SQL, DATABEND_OVERVIEW_TABLES_SQL, DATABEND_ACTIVE_QUERIES_SQL, DATABEND_INDEX_COUNT_SQL],
    ],
    ["getSlowQueries", (p: DatabendProvider) => p.getSlowQueries({ limit: 5 }), [databendSlowQueriesSql(5)]],
    [
      "getActiveSessions",
      (p: DatabendProvider) => p.getActiveSessions(),
      [databendSessionsSql(DATABEND_DEFAULT_SESSION_LIMIT)],
    ],
    ["getTableStats", (p: DatabendProvider) => p.getTableStats({ schema: "x" }), [databendTableStatsSql("x")]],
    ["getIndexStats", (p: DatabendProvider) => p.getIndexStats(), [databendIndexStatsSql(undefined)]],
    ["getStorageStats", (p: DatabendProvider) => p.getStorageStats(), [DATABEND_STORAGE_SQL]],
  ])("%s sends exactly its module's statements", async (_name, call, expected) => {
    const { provider, sent } = await connected();
    await call(provider);
    expect(sent()).toEqual(expected);
  });

  test("getHealth reads the overview, then slow queries and sessions", async () => {
    const { provider, sent } = await connected();
    const health = await provider.getHealth();
    expect(health.cacheHitRatio).toBe("N/A");
    expect(sent().slice(0, 4)).toEqual([
      DATABEND_VERSION_SQL,
      DATABEND_OVERVIEW_TABLES_SQL,
      DATABEND_ACTIVE_QUERIES_SQL,
      DATABEND_INDEX_COUNT_SQL,
    ]);
    expect(sent()).toHaveLength(6);
  });

  test("getPerformanceMetrics measures nothing and sends nothing", async () => {
    const { provider, sent } = await connected();
    expect(await provider.getPerformanceMetrics()).toEqual({});
    expect(sent()).toEqual([]);
  });

  test("the session database is marked in the default catalog only", async () => {
    const answers = (sql: string): Reply | undefined =>
      sql === databendDatabaseListSql("other")
        ? { schema: [column("database_name")], data: [["default"], ["libredb_demo"]] }
        : undefined;
    const pinned = await connected(answers, { database: "libredb_demo" });
    const marked = (containers: { name: string; isSessionDefault?: boolean }[]) =>
      containers.filter((container) => container.isSessionDefault).map((container) => container.name);
    expect(marked(await pinned.provider.listContainers(["default"]))).toEqual(["libredb_demo"]);
    expect(marked(await pinned.provider.listContainers(["other"]))).toEqual([]);
    const unpinned = await connected(answers);
    expect(marked(await unpinned.provider.listContainers(["default"]))).toEqual(["default"]);
  });

  test("describeObject reads its columns, and its indexes in the default catalog", async () => {
    const sql = databendColumnsSql(container, "table", "t");
    const { provider, sent } = await connected((sent) =>
      sent === sql
        ? {
            schema: ["column_name", "data_type", "is_nullable", "default_kind", "default_expression", "comment"].map(
              (name) => column(name),
            ),
            data: [["a", "Int32", "NO", "", "", ""]],
          }
        : undefined,
    );
    const detail = await provider.describeObject(["default", "libredb_demo", "t"], "table");
    expect(detail.columns.map((columnOf) => columnOf.name)).toEqual(["a"]);
    expect(sent()[0]).toBe(sql);
    expect(sent()).toHaveLength(2);
  });

  test("a path of the wrong shape or an undeclared kind is refused before any statement", async () => {
    const { provider, sent } = await connected();
    await expect(provider.countObjects(["default"])).rejects.toThrow('received ["default"]');
    await expect(provider.describeObject(["default", "t"], "table")).rejects.toThrow('received ["default","t"]');
    await expect(provider.describeObject(["default", "d", "t"], "index")).rejects.toThrow(
      DATABEND_OBJECT_SENTENCES.unknownKind("index"),
    );
    await expect(provider.readObjectSource(["default", "d", "t"], "index")).rejects.toThrow(QueryError);
    expect(sent()).toEqual([]);
  });

  test("a surface statement's failure is mapped to the house class, naming its statement", async () => {
    const { provider } = await connected((sql) =>
      sql === DATABEND_CATALOG_LIST_SQL ? failed(1006, "catalogs unreadable") : undefined,
    );
    const failure = (await provider.listContainers().catch((error: unknown) => error)) as QueryError;
    expect(failure).toBeInstanceOf(QueryError);
    expect(failure.message).toContain("catalogs unreadable");
    expect(failure.query).toBe(DATABEND_CATALOG_LIST_SQL);
  });

  test("runMaintenance sends the kill of one session, and refuses every other operation unsent", async () => {
    const { provider, sent } = await connected();
    const result = await provider.runMaintenance("kill", "abc-1");
    expect(result.success).toBe(true);
    expect(result.message).toBe(DATABEND_MONITORING_SENTENCES.killAsked("abc-1"));
    expect(sent()).toEqual([databendKillSql("abc-1")]);
    await expect(provider.runMaintenance("kill")).rejects.toThrow(DATABEND_MONITORING_SENTENCES.killNeedsId);
    await expect(provider.runMaintenance("vacuum", "t")).rejects.toThrow(
      DATABEND_PROVIDER_SENTENCES.maintenanceRefused("vacuum"),
    );
    expect(sent()).toHaveLength(1);
  });
});
