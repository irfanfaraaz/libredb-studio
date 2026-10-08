/**
 * The Databend HTTP transport's requests and its first answer (design 3.2 to 3.4; C6, C14, C17; X01, I6, I18), on the
 * scripted node transport with injected time: the closed statement body, the headers of every request, the checks the
 * first answer must pass before any page, the loop's result, and the notices it carries.
 */
import { describe, expect, test } from "bun:test";
import { DatabaseConfigError } from "@/lib/db/errors";
import { DATABEND_ANSWER_SENTENCES } from "@/lib/db/providers/sql/databend/answer";
import {
  DATABEND_MAX_SOCKETS,
  DATABEND_REQUEST_HEADER_NAMES,
} from "@/lib/db/providers/sql/databend/connection-options";
import { DATABEND_ERROR_SENTENCES as S, DATABEND_PROTOCOL_FAULTS as F } from "@/lib/db/providers/sql/databend/errors";
import { DatabendError } from "@/lib/db/providers/sql/databend/transport";
import { serverText } from "@/lib/db/utils/server-text";
import {
  answerBody,
  capturedAnswer,
  idsOf,
  ok,
  pathsOf,
  statement,
  TEST_NODE,
  TEST_PASSWORD,
  TEST_START,
  TEST_USER,
  testOptions,
  transportHarness,
} from "../../../helpers/databend-node-transport";

const FIRST = idsOf(1);
const P = pathsOf(FIRST.queryId);

async function failure(promise: Promise<unknown>): Promise<DatabendError> {
  try {
    await promise;
  } catch (error) {
    expect(error).toBeInstanceOf(DatabendError);
    return error as DatabendError;
  }
  throw new Error("expected the run to fail");
}

describe("the statement body (design 3.3)", () => {
  test("is the closed object, every key exact, the Binary format pinned to hex [X01]", async () => {
    const { script, transport } = transportHarness([{ method: "POST", path: "/v1/query", reply: ok(FIRST) }], {
      options: testOptions({ database: "studio_demo" }),
    });
    await transport.run(statement("SELECT 1", { rowCut: 1000 }));
    script.expectDone();
    expect(JSON.parse(script.requests[0].body as string)).toEqual({
      sql: "SELECT 1",
      session: {
        database: "studio_demo",
        settings: {
          format_null_as_str: "0",
          http_json_result_mode: "display",
          binary_output_format: "hex",
          max_execute_time_in_seconds: "60",
        },
      },
      pagination: { wait_time_secs: 10, max_rows_per_page: 1001, max_rows_in_buffer: 2002 },
    });
  });

  test("leaves the database out when the connection has none", async () => {
    const { script, transport } = transportHarness([{ method: "POST", path: "/v1/query", reply: ok(FIRST) }]);
    await transport.run(statement("SELECT 1"));
    script.expectDone();
    expect(Object.keys(JSON.parse(script.requests[0].body as string).session)).toEqual(["settings"]);
  });

  test("a provider statement also pins the dialect, quoted case and UTC under the surface deadline [C17]", async () => {
    const { script, transport } = transportHarness([{ method: "POST", path: "/v1/query", reply: ok(FIRST) }]);
    await transport.run(statement("SELECT 1", { origin: "provider" }));
    script.expectDone();
    expect(JSON.parse(script.requests[0].body as string).session.settings).toEqual({
      format_null_as_str: "0",
      http_json_result_mode: "display",
      binary_output_format: "hex",
      max_execute_time_in_seconds: "10",
      sql_dialect: "PostgreSQL",
      quoted_ident_case_sensitive: "1",
      timezone: "UTC",
    });
  });

  test("the deadline is whole seconds rounded up, and a page is never 0 nor over 10,000 rows", async () => {
    const { script, transport } = transportHarness(
      [
        { method: "POST", path: "/v1/query", reply: ok(FIRST) },
        { method: "POST", path: "/v1/query", reply: ok(idsOf(2)) },
      ],
      { options: testOptions({}, { queryTimeout: 1500 }) },
    );
    await transport.run(statement("SELECT 1", { rowCut: 0 }));
    await transport.run(statement("SELECT 1", { rowCut: 100_000 }));
    script.expectDone();
    const [small, large] = script.requests.map((request) => JSON.parse(request.body as string));
    expect(small.session.settings.max_execute_time_in_seconds).toBe("2");
    expect(small.pagination).toEqual({ wait_time_secs: 10, max_rows_per_page: 1, max_rows_in_buffer: 2 });
    expect(large.pagination).toEqual({ wait_time_secs: 10, max_rows_per_page: 10_000, max_rows_in_buffer: 20_000 });
  });
});

describe("the headers (design 3.2)", () => {
  test("the node transport is built with the connection's headers, the closed per-request list and three sockets", () => {
    const options = testOptions({ warehouse: "wh_1" });
    const { script } = transportHarness([], { options });
    expect(script.built).toHaveLength(1);
    const [built] = script.built;
    expect(built.origin).toEqual(options.origin);
    expect(built.tls).toBeNull();
    expect(built.maxSockets).toBe(DATABEND_MAX_SOCKETS);
    expect(built.requestHeaderNames).toEqual([...DATABEND_REQUEST_HEADER_NAMES]);
    expect(built.headers).toEqual({
      authorization: `Basic ${Buffer.from(`${TEST_USER}:${TEST_PASSWORD}`).toString("base64")}`,
      accept: "application/json",
      "user-agent": "libredb-studio/1.2.3",
      "x-databend-client-caps": "session_header",
      "x-databend-warehouse": "wh_1",
    });
  });

  test("the POST carries the client session, the query id and the route hint; the GETs add the sticky node", async () => {
    const { script, transport } = transportHarness([
      { method: "POST", path: "/v1/query", reply: ok(FIRST, { state: "Running", next_uri: P.page(0) }) },
      { method: "GET", path: P.page(0), reply: ok(FIRST, { next_uri: P.final }) },
      { method: "GET", path: P.final, reply: ok(FIRST) },
    ]);
    await transport.run(statement("SELECT 1"));
    script.expectDone();
    const [post, page, final] = script.requests;
    const session = Buffer.from(post.headers["x-databend-session"], "base64url").toString("utf8");
    expect(JSON.parse(session)).toEqual({ id: FIRST.sessionId, last_refresh_time: Math.floor(TEST_START / 1000) });
    expect(post.headers["x-databend-query-id"]).toBe(FIRST.queryId);
    expect(post.headers["x-databend-query-id"]).toMatch(/^[0-9a-f]{32}$/);
    expect(post.headers["x-databend-route-hint"]).toBe(`rh:${FIRST.routeHint}:500000`);
    expect(post.headers["x-databend-sticky-node"]).toBeUndefined();
    for (const get of [page, final]) {
      expect(get.headers["x-databend-session"]).toBe(post.headers["x-databend-session"]);
      expect(get.headers["x-databend-route-hint"]).toBe(post.headers["x-databend-route-hint"]);
      expect(get.headers["x-databend-sticky-node"]).toBe(TEST_NODE);
      expect(get.headers["x-databend-query-id"]).toBeUndefined();
      expect(get.body).toBeUndefined();
    }
  });

  test("a CR or LF in User or Warehouse is refused before a transport is built [C6]", () => {
    for (const overrides of [{ user: "reader\r\nx-databend-tenant: t" }, { warehouse: "wh\nx" }]) {
      expect(() => transportHarness([], { options: testOptions(overrides) })).toThrow(DatabaseConfigError);
    }
  });
});

describe("the first answer (design 3.4)", () => {
  test("another session_id fails before any page: one kill, then protocol [C14]", async () => {
    const { script, transport } = transportHarness([
      {
        method: "POST",
        path: "/v1/query",
        reply: ok(FIRST, { session_id: "another", state: "Running", next_uri: P.page(0) }),
      },
      { method: "GET", path: P.kill, reply: { status: 200 } },
    ]);
    const error = await failure(transport.run(statement("SELECT 1")));
    script.expectDone();
    expect(error.category).toBe("protocol");
    expect(error.message).toBe(S.protocol(F.sessionId));
    expect(script.requests[1].headers["x-databend-sticky-node"]).toBeUndefined();
  });

  test("an empty session_id says a proxy may have dropped the session header", async () => {
    const { script, transport } = transportHarness([
      { method: "POST", path: "/v1/query", reply: ok(FIRST, { session_id: "" }) },
      { method: "GET", path: P.kill, reply: { status: 200 } },
    ]);
    const error = await failure(transport.run(statement("SELECT 1")));
    script.expectDone();
    expect(error.message).toBe(S.protocol(F.proxySession));
  });

  test("an answer for another query id is killed and protocol", async () => {
    const { script, transport } = transportHarness([
      { method: "POST", path: "/v1/query", reply: ok({ ...FIRST, queryId: "0".repeat(32) }) },
      { method: "GET", path: P.kill, reply: { status: 200 } },
    ]);
    const error = await failure(transport.run(statement("SELECT 1")));
    script.expectDone();
    expect(error.message).toBe(S.protocol(F.queryId));
  });

  test.each([
    ["a CR LF node_id", "node\r\nx-evil: 1"],
    ["a missing node_id", null],
    ["a node_id over 64 characters", "n".repeat(65)],
  ])("%s stops the loop with a kill that carries no sticky node [C6]", async (_label, nodeId) => {
    const { script, transport } = transportHarness([
      { method: "POST", path: "/v1/query", reply: ok(FIRST, { node_id: nodeId, next_uri: P.page(0) }) },
      { method: "GET", path: P.kill, reply: { status: 200 } },
    ]);
    const error = await failure(transport.run(statement("SELECT 1")));
    script.expectDone();
    expect(error.message).toBe(S.protocol(F.field("node_id")));
    expect(script.requests[1].headers["x-databend-sticky-node"]).toBeUndefined();
  });

  test("a fail-to-start answer (id empty) is Databend's text, and nothing more is sent", async () => {
    const { script, transport } = transportHarness([
      {
        method: "POST",
        path: "/v1/query",
        reply: ok(FIRST, {
          id: "",
          session_id: null,
          node_id: null,
          state: "Failed",
          error: { code: 1001, message: "Failed to upgrade session" },
        }),
      },
    ]);
    const error = await failure(transport.run(statement("SELECT 1")));
    script.expectDone();
    expect(error.category).toBe("statement");
    expect(error.message).toBe(`Failed to upgrade session ${S.nothingRan}`);
  });

  test("an id that is empty with no error is an answer for another statement", async () => {
    const { script, transport } = transportHarness([
      { method: "POST", path: "/v1/query", reply: ok(FIRST, { id: "" }) },
      { method: "GET", path: P.kill, reply: { status: 200 } },
    ]);
    expect((await failure(transport.run(statement("SELECT 1")))).message).toBe(S.protocol(F.queryId));
    script.expectDone();
  });

  test("the session middleware's 400 on the POST is config, and nothing more is sent", async () => {
    const { script, transport } = transportHarness([
      {
        method: "POST",
        path: "/v1/query",
        reply: { status: 400, body: { error: { code: 400, message: "bad session header" } } },
      },
    ]);
    const error = await failure(transport.run(statement("SELECT 1")));
    script.expectDone();
    expect(error.category).toBe("config");
    expect(error.message).toBe(S.middlewareRefused("bad session header"));
  });

  test("a malformed 200 to the POST is protocol and is killed, with no logout", async () => {
    const { script, transport } = transportHarness([
      { method: "POST", path: "/v1/query", reply: { status: 200, body: "<html>" } },
      { method: "GET", path: P.kill, reply: { status: 200 } },
    ]);
    const error = await failure(transport.run(statement("SELECT 1")));
    script.expectDone();
    expect(error.message).toBe(S.protocol(F.notJson));
  });

  test("a refusal of the node transport before any socket is config, and nothing more is sent", async () => {
    const refused = new DatabaseConfigError("Invalid host: refused by the egress policy");
    const { script, transport } = transportHarness([{ method: "POST", path: "/v1/query", reply: { throws: refused } }]);
    const error = await failure(transport.run(statement("SELECT 1")));
    script.expectDone();
    expect(error.category).toBe("config");
    expect(error.message).toBe(refused.message);
    expect(error.cause).toBe(refused);
  });
});

describe("the result", () => {
  test("replays the captured three-page SELECT: every row, the schema, one final GET", async () => {
    const values = {
      "<query-2>": FIRST.queryId,
      "<node-1>": TEST_NODE,
      '"session_id":""': `"session_id":"${FIRST.sessionId}"`,
    };
    const { script, transport } = transportHarness([
      { method: "POST", path: "/v1/query", reply: capturedAnswer("select-pages", 0, values) },
      { method: "GET", path: P.page(1), reply: capturedAnswer("select-pages", 1, values) },
      { method: "GET", path: P.page(2), reply: capturedAnswer("select-pages", 2, values) },
      { method: "GET", path: P.final, reply: capturedAnswer("select-pages", 3, values) },
    ]);
    const outcome = await transport.run(statement("SELECT number, to_string(number) AS text FROM numbers(25)"));
    script.expectDone();
    expect(outcome.schema).toEqual([
      { name: "number", type: "UInt64" },
      { name: "text", type: "String" },
    ]);
    expect(outcome.rows).toHaveLength(25);
    expect(outcome.rows[24]).toEqual(["24", "24"]);
    expect(outcome.truncated).toBeNull();
    expect(outcome.hasResultSet).toBe(true);
    // The capture sent no settings, so its echo has no result mode: the floor warning (I6).
    expect(outcome.notices).toEqual([{ kind: "result-mode", mode: "" }]);
  });

  test("the first non-empty schema is kept: a Starting answer has none", async () => {
    const schema = [{ name: "a", type: "Int32" }];
    const { script, transport } = transportHarness([
      { method: "POST", path: "/v1/query", reply: ok(FIRST, { state: "Starting", next_uri: P.page(0) }) },
      { method: "GET", path: P.page(0), reply: ok(FIRST, { schema, data: [["1"]], next_uri: P.page(1) }) },
      { method: "GET", path: P.page(1), reply: ok(FIRST, { schema: [], data: [], next_uri: null }) },
    ]);
    const outcome = await transport.run(statement("SELECT 1 AS a"));
    script.expectDone();
    expect(outcome.schema).toEqual(schema);
    expect(outcome.rows).toEqual([["1"]]);
  });

  test("DDL answers no result set; an affect is carried", async () => {
    const { script, transport } = transportHarness([
      {
        method: "POST",
        path: "/v1/query",
        reply: ok(FIRST, { has_result_set: false, affect: { type: "UseDB", name: "studio_demo" } }),
      },
    ]);
    const outcome = await transport.run(statement("USE studio_demo"));
    script.expectDone();
    expect(outcome.hasResultSet).toBe(false);
    expect(outcome.affect).toEqual({ type: "UseDB", name: "studio_demo" });
    expect(outcome.notices).toEqual([{ kind: "use-not-carried" }]);
  });

  test("an in-body error closes with one final GET, no kill, and is Databend's statement error", async () => {
    const message = "error: \n  --> SQL:1:15\n  |\n1 | SELECT a FROM ev_temp\n  |               ^^^^^^^ Unknown table";
    const { script, transport } = transportHarness([
      {
        method: "POST",
        path: "/v1/query",
        reply: ok(FIRST, { state: "Failed", error: { code: 1025, message }, data: [], next_uri: P.final }),
      },
      { method: "GET", path: P.final, reply: ok(FIRST) },
    ]);
    const error = await failure(transport.run(statement("SELECT a FROM ev_temp")));
    script.expectDone();
    expect(error.category).toBe("statement");
    expect(error.code).toBe(1025);
    expect(error.position).toBe(15);
  });

  test("an in-body error on an answer with no next link sends nothing more", async () => {
    const { script, transport } = transportHarness([
      { method: "POST", path: "/v1/query", reply: ok(FIRST, { state: "Failed", error: { code: 1065, message: "x" } }) },
    ]);
    expect((await failure(transport.run(statement("SELECT")))).code).toBe(1065);
    script.expectDone();
  });
});

describe("the notices", () => {
  test.each([
    ["display", null],
    ["classic", { kind: "result-mode" as const, mode: "classic" }],
  ])("an echoed result mode %s gives %p (I6)", async (mode, notice) => {
    const session = { txn_state: "AutoCommit", settings: { http_json_result_mode: mode } };
    const { script, transport } = transportHarness([
      { method: "POST", path: "/v1/query", reply: ok(FIRST, { session }) },
    ]);
    const outcome = await transport.run(statement("SELECT 1"));
    script.expectDone();
    expect(outcome.notices).toEqual(notice === null ? [] : [notice]);
    if (notice !== null) expect(DATABEND_ANSWER_SENTENCES.resultMode(mode)).toContain(mode);
  });

  test("warnings become server warnings through serverText, once each across pages (I18)", async () => {
    const withheld = serverText(TEST_PASSWORD, testOptions().secretForms);
    const { script, transport } = transportHarness([
      {
        method: "POST",
        path: "/v1/query",
        reply: ok(FIRST, { warnings: ["setting no_such_setting ignored"], next_uri: P.page(0) }),
      },
      {
        method: "GET",
        path: P.page(0),
        reply: ok(FIRST, { warnings: ["setting no_such_setting ignored", `role ${TEST_PASSWORD} denied`] }),
      },
    ]);
    const outcome = await transport.run(statement("SELECT 1"));
    script.expectDone();
    expect(outcome.notices).toEqual([
      { kind: "server-warning", text: "setting no_such_setting ignored" },
      { kind: "server-warning", text: withheld },
    ]);
  });

  test("SET ROLE is told against the role the first provider statement echoed (design 3.7)", async () => {
    const role = (name: string) => ({
      session: { txn_state: "AutoCommit", role: name, settings: { http_json_result_mode: "display" } },
    });
    const { script, transport } = transportHarness([
      { method: "POST", path: "/v1/query", reply: ok(idsOf(1), role("analyst")) },
      { method: "POST", path: "/v1/query", reply: ok(idsOf(2), role("public")) },
      { method: "POST", path: "/v1/query", reply: ok(idsOf(3), role("analyst")) },
      { method: "POST", path: "/v1/query", reply: ok(idsOf(4), role("public")) },
    ]);
    // A user statement before the probe has nothing to compare with.
    expect((await transport.run(statement("SET ROLE analyst"))).notices).toEqual([]);
    expect((await transport.run(statement("SELECT 1", { origin: "provider" }))).notices).toEqual([]);
    expect((await transport.run(statement("SET ROLE analyst"))).notices).toEqual([{ kind: "role-not-carried" }]);
    expect((await transport.run(statement("SELECT 1"))).notices).toEqual([]);
    script.expectDone();
  });

  test("SET and SET GLOBAL give their warnings from the affect", async () => {
    const affect = { type: "ChangeSettings", keys: ["a", "b"], values: ["1", "2"], is_globals: [false, true] };
    const { script, transport } = transportHarness([
      { method: "POST", path: "/v1/query", reply: ok(FIRST, { affect }) },
    ]);
    expect((await transport.run(statement("SET a = 1"))).notices).toEqual([
      { kind: "settings-not-carried" },
      { kind: "global-settings-changed", keys: ["b"] },
    ]);
    script.expectDone();
  });
});

test("a captured answer's body is the capture's text with its placeholders replaced", () => {
  const reply = capturedAnswer("auth-401", 0, {});
  expect(reply.status).toBe(401);
  expect(JSON.parse(reply.body)).toEqual({
    error: { code: 5100, message: "Authentication failed: incorrect password" },
  });
  expect(answerBody({}).next_uri).toBeNull();
});
